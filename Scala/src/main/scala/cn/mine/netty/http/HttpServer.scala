package cn.mine.netty.http

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel

/**
 * @author 魏喜明
 */
object HttpServer {
  def main(args: Array[String]): Unit = {
    // 接收组：接收连接，但是不处理
    val acceptGroup = new NioEventLoopGroup()
    // 工作组：真正处理连接的 group
    val workerGroup = new NioEventLoopGroup()
    try {
      // 加载 Initializer
      val serverBootstrap = new ServerBootstrap()
      serverBootstrap.group(acceptGroup, workerGroup)
          .channel(classOf[NioServerSocketChannel])
          // 这里的 childHandler 是服务于 workerGroup 的
          // 如果直接使用 handler 方法添加处理器，则是服务于 acceptGroup 的
          .childHandler(new HttpServerInitializer())
      // 绑定监听端口
      val channelFuture = serverBootstrap.bind(8899).sync()
      channelFuture.channel().closeFuture().sync()
    } finally {
      acceptGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
    }
  }
}
