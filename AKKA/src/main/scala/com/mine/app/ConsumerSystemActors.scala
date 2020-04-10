package com.mine.app

import akka.actor.{ActorSystem, Props}
import com.mine.actor.ConsumerActor
import com.mine.ImuMap
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.StdIn

object ConsumerSystemActors {
    private val clusterName = "ConsumerActors"
    private val serverHost: String = "127.0.0.1"
    private val serverPort: String = "9999"
    private val hp: String = serverHost + ":" + serverPort
    private val config: Config = ConfigFactory.parseString(
        s"""
           |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
           |akka.remote.netty.tcp.hostname = $serverHost
           |akka.remote.netty.tcp.port = $serverPort
           |akka.log-dead-letters = 0
           |akka.log-dead-letters-during-shutdown = off
           |akka.actor.warn-about-java-serializer-usage = off
              """.stripMargin)

    def main(args: Array[String]): Unit = {
        // + "_" + UUID.randomUUID().toString
        val actorName1: String = "Actor_1"
        val actorName2: String = "Actor_2"

        val actorSystem = ActorSystem(clusterName, config)

        val actorRef1 = actorSystem.actorOf(Props(new ConsumerActor(clusterName, ImuMap((actorName2, hp)))), actorName1)
        val actorRef2 = actorSystem.actorOf(Props(new ConsumerActor(clusterName, ImuMap((actorName1, hp)))), actorName2)

        actorRef1 ! "start"
        actorRef2 ! "start"

        var loop: Boolean = true
        var str1: String = ""
        var str2: String = ""

        do {
            if (str1 != "exit") {
                str1 = StdIn.readLine(s"作为 $actorName1 请输入你想发送的信息 : ")
                actorRef1 ! str1
            }
            if (str2 != "exit") {
                str2 = StdIn.readLine(s"作为 $actorName2 请输入你想发送的信息 : ")
                actorRef2 ! str2
            }
            if (str1 == "exit" && str2 == "exit") {
                loop = false
                actorSystem.terminate()
            }
        } while (loop)
    }
}
