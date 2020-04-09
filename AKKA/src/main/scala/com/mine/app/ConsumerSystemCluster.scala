package com.mine.app

import akka.actor.{ActorRef, ActorSystem, Props}
import com.mine.ImuMap
import com.mine.actor.ConsumerActor
import com.mine.projectUtils.PropertyUtil
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.StdIn

object ConsumerSystemCluster {
    private val propertyUtil: PropertyUtil = new PropertyUtil("consumer.properties")

    private val clusterName: String = propertyUtil.getPropertyValueByKey("cluster.name")
    private val nodeName: String = propertyUtil.getPropertyValueByKey("whoami")

    // node1@127.0.0.1:9991  nhpMap --> (node, (host, port))
    private val nhpMap: Map[String, (String, String)] = propertyUtil.getPropertyValueByKey("cluster.nodes").split(",").map {
        nhp =>
            val nodehp = nhp.split("@")
            val node = nodehp(0)
            val hp = nodehp(1).split(":")
            (node, (hp(0), hp(1)))
    }.toMap

//    private val nhMap: Map[String, String] = nhpMap.filter(_ != nodeName).map(nhp => (nhp._1, nhp._2._1))
    private val nhMap: Map[String, String] = nhpMap.map(nhp => (nhp._1, nhp._2._1))

    private val hpTuple: (String, String) = nhpMap(nodeName)

    private val config: Config = ConfigFactory.parseString(
        s"""
           |akka.actor.provider = akka.remote.RemoteActorRefProvider
           |akka.remote.netty.tcp.hostname = ${hpTuple._1}
           |akka.remote.netty.tcp.port = ${hpTuple._2}
           |akka.log-dead-letters = 0
           |akka.log-dead-letters-during-shutdown = off
           |akka.actor.warn-about-java-serializer-usage = off
              """.stripMargin)

    def main(args: Array[String]): Unit = {
        val actorSystem = ActorSystem(clusterName, config)

        val actorRef: ActorRef = actorSystem.actorOf(Props(new ConsumerActor(clusterName, nhMap)), nodeName)

        var loop: Boolean = true

        do {
            val str = StdIn.readLine(s"作为 $nodeName 请输入你想发送的信息 : ")

            actorRef ! str

            if ("exit" == str) {
                loop = false
                actorSystem.terminate()
            }
        } while (loop)
    }
}
