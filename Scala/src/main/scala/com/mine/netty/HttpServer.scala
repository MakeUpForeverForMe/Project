package com.mine.netty

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel

/**
 * @author 魏喜明
 */
object HttpServer {
  def main(args: Array[String]): Unit = {
    // 接收连接,但是不处理
    val parentGroup = new NioEventLoopGroup()
    // 真正处理连接的group
    val childGroup = new NioEventLoopGroup()
    try {
      // 加载Initializer
      val serverBootstrap = new ServerBootstrap()
      serverBootstrap.group(parentGroup, childGroup)
          .channel(classOf[NioServerSocketChannel])
          // 这里的 childHandler 是服务于 childGroup 的
          // 如果直接使用 handler 方法添加处理器，则是服务于 parentGroup 的
          .childHandler(new HttpServerInitializer())
      // 绑定监听端口
      val channelFuture = serverBootstrap.bind(8899).sync()
      channelFuture.channel().closeFuture().sync()
    } finally {
      parentGroup.shutdownGracefully()
      childGroup.shutdownGracefully()
    }
  }
}
