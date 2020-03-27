package com.mine.actor

import akka.actor.{Actor, ActorLogging, ActorSelection}
import com.mine.bean.ConsumerMsg
import com.mine.{MuArrayBuffer, MuHashMap}

class ConsumerActor() extends Actor with ActorLogging {

  log.info(s"${self.path.name} 程序已启动！！！")

  private val actorSelections: MuArrayBuffer[ActorSelection] = MuArrayBuffer()

  private var clusterName: String = _
  private var nodeHostPortMap: MuHashMap = _

  def this(clusterName: String, nodeHostPortMap: MuHashMap) {
    this
    this.clusterName = clusterName
    this.nodeHostPortMap = nodeHostPortMap
  }

  override def preStart(): Unit = nodeHostPortMap.foreach(nodeHostPort => actorSelections += context.actorSelection(s"akka.tcp://$clusterName@${nodeHostPort._2}/user/${nodeHostPort._1}"))

  override def receive: Receive = {
    case "exit" =>
      context.stop(self)
      log.info(s"${self.path.name} 的程序退出了...")
    case msg: String => actorSelections.foreach { actorSelection =>
      actorSelection ! ConsumerMsg(msg)
      log.info(s"${self.path.name} 向 ${actorSelection.pathString.split("/")(actorSelection.pathString.split("/").length - 1)} 发送了一条消息 : $msg")
    }
    case ConsumerMsg(msg) => log.info(s"收到 ${sender().path.name} 发送给 ${self.path.name} 的一条信息 : $msg")
  }
}
