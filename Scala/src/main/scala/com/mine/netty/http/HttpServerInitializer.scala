package com.mine.netty.http

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.HttpServerCodec

/**
 * @author 魏喜明
 */
class HttpServerInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(socketChannel: SocketChannel): Unit = {
    val pipeline = socketChannel.pipeline()
    // 处理 http 服务的关键 handler
    pipeline.addLast("httpServerCodec", new HttpServerCodec())
    // 自定义的 handler
    pipeline.addLast("httpServerHandler", new HttpServerHandler)
  }
}
