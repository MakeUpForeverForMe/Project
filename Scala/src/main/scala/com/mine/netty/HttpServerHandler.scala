package com.mine.netty

import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{DefaultFullHttpResponse, HttpHeaderNames, HttpObject, HttpRequest, HttpResponseStatus, HttpVersion}
import io.netty.util.CharsetUtil

/**
 * @author 魏喜明
 */
class HttpServerHandler extends SimpleChannelInboundHandler[HttpObject] {
  override def channelRead0(channelHandlerContext: ChannelHandlerContext, httpObject: HttpObject): Unit = {
    httpObject match {
      case httpRequest: HttpRequest =>
        println("请求方法名称:" + httpRequest.method().name())
        println("请求来自:" + channelHandlerContext.channel().remoteAddress())
        val byteBuf = Unpooled.copiedBuffer("Hello World!", CharsetUtil.UTF_8)
        val response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf)
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain")
        response.headers().set(HttpHeaderNames.CONTENT_LANGUAGE, byteBuf.readableBytes())
        channelHandlerContext.writeAndFlush(response)
        channelHandlerContext.channel().close()
      case _ =>
        println("参数错误！")
    }
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    println("Handler Add")
    super.handlerAdded(ctx)
  }

  override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
    println("Channel Registered")
    super.channelRegistered(ctx)
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("Channel Active")
    super.channelActive(ctx)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    println("Channel Inactive")
    super.channelInactive(ctx)
  }

  override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
    println("Channel Unregistered")
    super.channelUnregistered(ctx)
  }
}
