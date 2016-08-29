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

package io.druid.multi.broker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.http.client.HttpClient;
import io.druid.client.DirectDruidClient;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Smile;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.FluentQueryRunnerBuilder;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class MultiClusterQuerySegmentWalker implements QuerySegmentWalker
{

  private static final Logger log = new Logger(MultiClusterQuerySegmentWalker.class);


  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final ServiceEmitter emitter;
  private final List<ServerDiscoverySelector> brokerSelectorList;
  private final ObjectMapper objectMapper;
  private final ExecutorService executor;

  private final Object lock = new Object();

  private volatile boolean started = false;

  @Inject
  public MultiClusterQuerySegmentWalker(
      QueryToolChestWarehouse warehouse,
      QueryWatcher queryWatcher,
      @Smile ObjectMapper smileMapper,
      @Client HttpClient httpClient,
      ServiceEmitter emitter,
      @MultiClusterBroker List<ServerDiscoverySelector> brokerSelectorList,
      ObjectMapper objectMapper,
      @Processing ExecutorService executor
  )
  {
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.emitter = emitter;
    this.brokerSelectorList = brokerSelectorList;
    this.objectMapper = objectMapper;
    this.executor = executor;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      try {
        for (ServerDiscoverySelector brokerSelector : brokerSelectorList) {
          brokerSelector.start();
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      started = true;
    }
  }


  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      try {
        for (ServerDiscoverySelector brokerSelector : brokerSelectorList) {
          brokerSelector.stop();
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      started = false;
    }
  }


  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      Query<T> query, Iterable<Interval> intervals
  )
  {
    return makeRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      Query<T> query, Iterable<SegmentDescriptor> specs
  )
  {
    return makeRunner(query);
  }


  private <T> QueryRunner<T> makeRunner(Query<T> query)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    PostProcessingOperator<T> postProcessing = objectMapper.convertValue(
        query.<String>getContextValue("postProcessing"),
        new TypeReference<PostProcessingOperator<T>>()
        {
        }
    );
    return new FluentQueryRunnerBuilder<>(toolChest)
        .create(
            new ChainedExecutionQueryRunner<T>(
                executor,
                queryWatcher,
                FunctionalIterable.create(brokerSelectorList)
                                  .transform(new Function<ServerDiscoverySelector, QueryRunner<T>>()
                                  {
                                    @Override
                                    public QueryRunner<T> apply(ServerDiscoverySelector input)
                                    {
                                      Server server = input.pick();
                                      return
                                          new MetricsEmittingQueryRunner<T>(
                                              emitter,
                                              new Function<Query<T>, ServiceMetricEvent.Builder>()
                                              {
                                                @Override
                                                public ServiceMetricEvent.Builder apply(Query<T> input)
                                                {
                                                  return toolChest.makeMetricBuilder(input);
                                                }
                                              },
                                              makeDirectClient(server),
                                              "query/cluster/time",
                                              ImmutableMap.of("brokerHost", server.getHost())
                                          );
                                    }
                                  })
            )
        )
        .mergeResults()
        .applyPostMergeDecoration()
        .emitCPUTimeMetric(emitter)
        .postProcess(postProcessing);
  }


  private DirectDruidClient makeDirectClient(Server host)
  {
    return new DirectDruidClient(
        warehouse,
        queryWatcher,
        smileMapper,
        httpClient,
        String.format("%s:%s", host.getAddress(), host.getPort()),
        emitter
    );
  }


}
