package com.mine.actor

import akka.actor.{Actor, ActorLogging, ActorSelection}
import com.mine.bean.ConsumerMsg
import com.mine.{ImuMap, MuArrayBuffer}

//class ConsumerActor(clusterName: String, nodeHostMap: Map[String, String]) extends Actor with ActorLogging {
class ConsumerActor extends Actor with ActorLogging {

    log.info(s"${self.path.name} 程序已启动！！！")

    private val actorSelections: MuArrayBuffer[ActorSelection] = MuArrayBuffer()

    private var clusterName: String = _
    private var nodeHostMap: ImuMap = _

    def this(clusterName: String, nodeHostMap: ImuMap) {
        this
        this.clusterName = clusterName
        this.nodeHostMap = nodeHostMap
    }

    override def preStart(): Unit = {
        nodeHostMap.foreach(nodeHost => actorSelections += context.actorSelection(s"akka.tcp://$clusterName@${nodeHost._2}/user/${nodeHost._1}"))

        nodeHostMap.foreach(nodeHost => println(context.actorSelection(s"akka.tcp://$clusterName@${nodeHost._2}/user/${nodeHost._1}")))
    }


    override def receive: Receive = {
        case "exit" =>
            context.stop(self)
            log.info(s"${self.path.name} 的程序退出了...")
        case msg: String =>
            actorSelections.foreach {
                actorSelection =>
                    actorSelection ! ConsumerMsg(msg)
                    println(actorSelection)
                // log.info(s"${self.path.name} 向 ${actorSelection.pathString.split("/")(actorSelection.pathString.split("/").length - 1)} 发送了一条消息 : $msg")
            }
        case ConsumerMsg(msg) => log.info(s"收到 ${sender().path.name} 发送给 ${self.path.name} 的一条信息 : $msg")
    }
}
