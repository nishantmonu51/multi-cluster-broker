#Experimental! Multi Cluster Broker: 
 
Understands multiple clusters and provides a way to query and get a consolidated result across clusters.

##To run the node - 
```
java <jvm_args> -cp <classpath>:/path/to/multi-cluster-broker-<Version>-selfcontained.jar io.druid.cli.MainExtension server multiClusterBroker
```

##Sample runtime.properties -  
```
druid.service=druid/multiClusterBroker
druid.port=8085

# HTTP server threads
druid.broker.http.numConnections=5
druid.server.http.numThreads=8

# Processing threads and buffers
druid.processing.buffer.sizeBytes=256000000
druid.processing.numThreads=2

druid.multi.broker.brokerServiceName=druid/broker
druid.multi.broker.zkHosts=["zkConnectString-Cluster1","zkConnectString-Cluster2"]
```

##Assumptions : 
* Broker Service Name is same across all the clusters
* Data across the clusters is Disjoint
* Datasource names and schema are consistent across clusters
* Zookeeper paths for announcement and service discovery are consistent across clusters