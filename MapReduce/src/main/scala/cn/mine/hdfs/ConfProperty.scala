package cn.mine.hdfs

import org.apache.hadoop.conf.Configuration

import java.util.Properties

/**
  * @author ximing.wei
  */
object ConfProperty {
    def apply(): ConfProperty = new ConfProperty()

    def apply(fileName: String): ConfProperty = new ConfProperty(fileName)
}

class ConfProperty {
    private val QUEUE: String       =   "queue.name"
    private val CLUSTER: String     =   "dfs.cluster"
    private val NN1: String         =   "cluster.nn1"
    private val NN2: String         =   "cluster.nn2"

    private val configuration: Configuration = new Configuration

    def this(fileName: String) {
        this()
        init(fileName)
    }

    private def init(name: String): Unit = {
        val properties = new Properties()
        properties.load(this.getClass.getClassLoader.getResourceAsStream(name))
        val cluster = properties.getProperty(CLUSTER)
        configuration.set("mapred.job.queue.name", "root." + properties.getProperty(QUEUE))
        configuration.set("fs.defaultFS", "hdfs://" + cluster)
        configuration.set("dfs.nameservices", cluster)
        configuration.set("dfs.ha.namenodes." + cluster, "nn1,nn2")
        configuration.set("dfs.namenode.rpc-address." + cluster + ".nn1", properties.getProperty(NN1))
        configuration.set("dfs.namenode.rpc-address." + cluster + ".nn2", properties.getProperty(NN2))
        configuration.set("dfs.client.failover.proxy.provider." + cluster, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    }

    def conf: Configuration = configuration
}
