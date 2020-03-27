package com.mine.app

import akka.actor.{ActorRef, ActorSystem, Props}
import com.mine.MuHashMap
import com.mine.actor.ConsumerActor
import com.mine.util.AnalyzeProperty
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.StdIn

object ConsumerSystemCluster {
  private val property: AnalyzeProperty = AnalyzeProperty("consumer.properties")

  private val clusterName: String = property.getPropertyValue("cluster.name")
  private val nodeName: String = property.getPropertyValue("whoami")

  private val nhpHashMap: MuHashMap = MuHashMap()
  property.getPropertyValue("cluster.nodes").split(",").foreach { nhp =>
    val nodehp = nhp.split("@")
    val hostPort = nodehp(1)
    val node = nodehp(0)
    nhpHashMap += ((node, hostPort))
  }

  private val hpArray: Array[String] = nhpHashMap(nodeName).split(":")
  private val localHost = hpArray(0)
  private val localPort = hpArray(1)

  private val config: Config = ConfigFactory.parseString(
    s"""
       |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
       |akka.remote.netty.tcp.hostname = $localHost
       |akka.remote.netty.tcp.port = $localPort
       |akka.log-dead-letters = 0
       |akka.log-dead-letters-during-shutdown = off
       |akka.actor.warn-about-java-serializer-usage = off
              """.stripMargin)

  def main(args: Array[String]): Unit = {
    val actorSystem = ActorSystem(clusterName, config)

    val actorRef: ActorRef = actorSystem.actorOf(Props(new ConsumerActor(clusterName, nhpHashMap.filter(_._1 != nodeName))), nodeName)

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
