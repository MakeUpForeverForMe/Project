package com.mine.zookeeper


import java.text.SimpleDateFormat
import java.util.concurrent.CountDownLatch

import org.apache.zookeeper._
import org.apache.zookeeper.ZooDefs.Ids

import scala.collection.JavaConverters.asScalaBufferConverter

object ZKClient extends Watcher {

    private val countDownLatch = new CountDownLatch(1)

    override def process(watchedEvent: WatchedEvent): Unit = if (Watcher.Event.KeeperState.SyncConnected == watchedEvent.getState) countDownLatch.countDown()

    private final val SESSION_TIME_OUT = 2000
    private val host = "10.83.0.47:2181,10.83.0.123:2181,10.83.0.129:2181"
    private val zkClient = new ZooKeeper(host, SESSION_TIME_OUT, this)

    private val path: String = "/"
    private val test: String = "test"
    private val finalPath: String = path + test

    def main(args: Array[String]): Unit = {
        countDownLatch.await()

        if (existNode(finalPath) == 0) println("存在") else println(createNode(finalPath, "Scala Test"))
        println(getChildrenNum(finalPath))
        getChildren(finalPath).foreach(println)
        println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(getCTime(finalPath)))
        println(getData(finalPath))
        println(setData(finalPath, "Test Scala"))
        println(getData(finalPath))
        println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(getMTime(finalPath)))
        deleteNode(finalPath)
        println("删除成功")
        if (existNode(finalPath) == 0) println(getChildrenNum(finalPath)) else println("节点不存在")

        println(zkClient.getState)
        closeConnection()
        println(zkClient.getState)
    }

    /**
      * 判断是否存在节点
      *
      * @param path 传入路径
      * @return 返回是否存在
      */
    def existNode(path: String): Int = if (null != zkClient.exists(path, false)) 0 else 1

    /**
      * 创建节点
      *
      * @param path 传入路径
      * @param data 存储数据
      * @return 返回创建的加点
      */
    def createNode(path: String, data: String): String =
        this.zkClient.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

    /**
      * 获取某个路径下子节点的数量
      *
      * @param path 传入路径
      * @return 获取当前路径下的子节点数
      */
    def getChildrenNum(path: String): Int = zkClient.getChildren(path, false).size()

    /**
      * 获取路径下所有子节点
      * 返回 Java 类型的 List，转为 Scala 类型
      *
      * @param path 传入路径
      * @return 返回子目录List
      */
    def getChildren(path: String): List[String] = zkClient.getChildren(path, false).asScala.toList

    /**
      * 获取创建时间
      *
      * @param path 传入路径
      * @return 返回创建时间/ms
      */
    def getCTime(path: String): Long = zkClient.exists(path, false).getCtime

    /**
      * 获取更新时间
      *
      * @param path 传入路径
      * @return 返回更新时间/ms
      */
    def getMTime(path: String): Long = zkClient.exists(path, false).getMtime

    /**
      * 设置节点信息
      *
      * @param path 传入路径
      * @param data 存储数据
      * @return 返回存储数据后的版本
      */
    def setData(path: String, data: String): Int = zkClient.setData(path, data.getBytes(), -1).getVersion

    /**
      * 获取节点上面的数据
      * 将字节行数据转为人类可读取的数据
      *
      * @param path 传入路径
      * @return 返回数据
      */
    def getData(path: String): String = new String(zkClient.getData(path, false, null))

    /**
      * 删除节点
      *
      * @param path 传入路径
      */
    def deleteNode(path: String): Unit = zkClient.delete(path, -1)

    /**
      * 关闭连接
      */
    def closeConnection(): Unit = zkClient.close()
}
