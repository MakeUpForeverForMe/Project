package com.mine.test

import com.mine.hdfs.ConfProperty
import com.mine.propertyutil.ConfigUtil
import org.junit.Test

/**
  * @author ximing.wei
  */
class MRTest {
    @Test
    def propertyTest(): Unit = {
        val props = ConfigUtil("conf.properties")
        props.setProps("aa", "jfdls")
        println(props.getProps("queue.name") + " ----------- " + props.getProps("aa"))
    }

    @Test
    def confPropertyTest(): Unit = {
        val props = ConfigUtil("conf.properties")
        val cluster = props.getProps("dfs.cluster")
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
