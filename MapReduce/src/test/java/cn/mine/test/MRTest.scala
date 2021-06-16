package cn.mine.test

import cn.mine.hdfs.ConfProperty
import org.junit.jupiter.api.Test

import java.util.Properties

/**
 * @author ximing.wei
 */
class MRTest {
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("conf.properties"))

    @Test
    def propertyTest(): Unit = {
        properties.setProperty("aa", "jfdls")
        println(properties.getProperty("queue.name") + " ----------- " + properties.getProperty("aa"))
    }

    @Test
    def confPropertyTest(): Unit = {
        val cluster = properties.getProperty("dfs.cluster")
        val confProperty = ConfProperty("conf.properties")

        println(confProperty.conf.get("mapred.job.queue.name") + "\n" +
                confProperty.conf.get("fs.defaultFS") + "\n" +
                confProperty.conf.get("dfs.nameservices") + "\n" +
                confProperty.conf.get("dfs.ha.namenodes." + cluster) + "\n" +
                confProperty.conf.get("dfs.namenode.rpc-address." + cluster + ".nn1") + "\n" +
                confProperty.conf.get("dfs.namenode.rpc-address." + cluster + ".nn1") + "\n" +
                confProperty.conf.get("dfs.client.failover.proxy.provider." + cluster) + "\n")

        println(confProperty.conf)
    }
}
