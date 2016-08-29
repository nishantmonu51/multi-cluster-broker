/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import io.airlift.airline.Command;
import io.druid.client.selector.ServerSelectorStrategy;
import io.druid.curator.CuratorConfig;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.guice.http.JettyHttpClientModule;
import io.druid.multi.broker.MultiClusterBroker;
import io.druid.multi.broker.MultiClusterConfig;
import io.druid.multi.broker.MultiClusterQuerySegmentWalker;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.RetryQueryRunnerConfig;
import io.druid.server.QueryResource;
import io.druid.server.initialization.CuratorDiscoveryConfig;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
@Command(
    name = "multiClusterBroker",
    description = "Experimental! Understands multiple clusters and provides a way to query and get a consolidated result across clusters."
)
public class CliMultiClusterBroker extends ServerRunnable
{
  private static final Logger log = new Logger(CliMultiClusterBroker.class);

  public CliMultiClusterBroker()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new JettyHttpClientModule("druid.multi.cluster.broker.http", MultiClusterBroker.class),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/multiClusterBroker");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8085);

            JsonConfigProvider.bind(binder, "druid.multi.broker", MultiClusterConfig.class);
            JsonConfigProvider.bind(binder, "druid.multi.broker.balancer", ServerSelectorStrategy.class);
            JsonConfigProvider.bind(binder, "druid.multi.broker.retryPolicy", RetryQueryRunnerConfig.class);
            binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);
            binder.bind(QuerySegmentWalker.class).to(MultiClusterQuerySegmentWalker.class).in(LazySingleton.class);
            binder.bind(MultiClusterQuerySegmentWalker.class).in(ManageLifecycle.class);

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
            LifecycleModule.register(binder, QueryResource.class);
            Jerseys.addResource(binder, QueryResource.class);

            LifecycleModule.register(binder, Server.class);
            DiscoveryModule.register(binder, Self.class);
          }

          // Provides CuratorFrameworks for individual druid clusters
          @Provides
          @LazySingleton
          @MultiClusterBroker
          public List<CuratorFramework> getCuratorFrameworks(
              MultiClusterConfig config,
              CuratorConfig curatorConfig,
              Lifecycle lifecycle
          )
          {
            List<CuratorFramework> curatorFrameworks = Lists.newArrayList();
            for (final String zkHost : config.getZkHosts()) {
              final CuratorFramework framework =
                  CuratorFrameworkFactory.builder()
                                         .connectString(zkHost)
                                         .sessionTimeoutMs(curatorConfig.getZkSessionTimeoutMs())
                                         .retryPolicy(new BoundedExponentialBackoffRetry(1000, 45000, 30))
                                         .compressionProvider(new PotentiallyGzippedCompressionProvider(curatorConfig.getEnableCompression()))
                                         .build();

              lifecycle.addHandler(
                  new Lifecycle.Handler()
                  {
                    @Override
                    public void start() throws Exception
                    {
                      log.info("Starting Curator [%s]", zkHost);
                      framework.start();
                    }

                    @Override
                    public void stop()
                    {
                      log.info("Stopping Curator [%s]", zkHost);
                      framework.close();
                    }
                  }
              );
              curatorFrameworks.add(framework);

            }
            return curatorFrameworks;
          }

          @Provides
          @LazySingleton
          @MultiClusterBroker
          public List<ServiceDiscovery> getServerDiscoveries(
              @MultiClusterBroker List<CuratorFramework> curatorFrameworks,
              CuratorDiscoveryConfig curatorDiscoveryConfig,
              Lifecycle lifecycle
          )
          {
            List<ServiceDiscovery> serviceDiscoveries = Lists.newArrayList();
            for (CuratorFramework framework : curatorFrameworks) {
              final ServiceDiscovery<Void> serviceDiscovery =
                  ServiceDiscoveryBuilder.builder(Void.class)
                                         .basePath(curatorDiscoveryConfig.getPath())
                                         .client(framework)
                                         .build();

              lifecycle.addHandler(
                  new Lifecycle.Handler()
                  {
                    @Override
                    public void start() throws Exception
                    {
                      serviceDiscovery.start();
                    }

                    @Override
                    public void stop()
                    {
                      try {
                        serviceDiscovery.close();
                      }
                      catch (Exception e) {
                        throw Throwables.propagate(e);
                      }
                    }
                  }
              );
              serviceDiscoveries.add(serviceDiscovery);
            }
            return serviceDiscoveries;
          }

          @Provides
          @LazySingleton
          @MultiClusterBroker
          public List<ServerDiscoverySelector> getBrokerDiscoveryFactories(
              @MultiClusterBroker List<ServiceDiscovery> serviceDiscoveries,
              MultiClusterConfig config
          )
          {
            final String brokerServiceName = config.getBrokerServiceName().replaceAll("/", ":");

            return Lists.newArrayList(Iterables.transform(
                serviceDiscoveries,
                new Function<ServiceDiscovery, ServerDiscoverySelector>()
                {
                  @Override
                  public ServerDiscoverySelector apply(ServiceDiscovery input)
                  {
                    return new ServerDiscoveryFactory(input)
                        .createSelector(brokerServiceName);
                  }
                }
            ));
          }
        }
    );
  }
}
