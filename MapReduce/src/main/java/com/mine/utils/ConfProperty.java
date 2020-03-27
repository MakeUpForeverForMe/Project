package com.mine.utils;

import org.apache.hadoop.conf.Configuration;

public class ConfProperty {
  private static final String QUEUE = "queue.name";
  private static final String CLUSTER = "dfs.cluster";
  private static final String NN1 = "cluster.nn1";
  private static final String NN2 = "cluster.nn2";

  private Configuration configuration = new Configuration();

  public ConfProperty() {
  }

  public ConfProperty(String name) {
    init(name);
  }

  private void init(String name) {
    PropertyGet propertyGet = new PropertyGet(name);
    String cluster = propertyGet.get(CLUSTER);
    configuration.set("mapred.job.queue.name", "root." + propertyGet.get(QUEUE));
    configuration.set("fs.defaultFS", "hdfs://" + cluster);
    configuration.set("dfs.nameservices", cluster);
    configuration.set("dfs.ha.namenodes." + cluster, "nn1,nn2");
    configuration.set("dfs.namenode.rpc-address." + cluster + ".nn1", propertyGet.get(NN1));
    configuration.set("dfs.namenode.rpc-address." + cluster + ".nn2", propertyGet.get(NN2));
    configuration.set("dfs.client.failover.proxy.provider.mycluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
  }

  public Configuration conf() {
    return configuration;
  }
}
