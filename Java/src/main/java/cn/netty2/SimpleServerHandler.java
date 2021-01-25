package cn.netty2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author 魏喜明
 */
public class SimpleServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext channelHandlerContext, Object msg) {
        System.out.println("SimpleServerHandler.channelRead，服务端读入数据");
        ByteBuf result = (ByteBuf) msg;
        byte[] bytes = new byte[result.readableBytes()];
        // msg中存储的是 ByteBuf 类型的数据，把数据读取到byte[]中
        result.readBytes(bytes);
        String resultStr = new String(bytes);
        // 接收并打印客户端的信息
        System.out.println("Client said（客户端说）：" + resultStr);
        // 释放资源，这行很关键
        result.release();

        // 向客户端发送消息
        String response = "服务端返回：hello client!";
        // 在当前场景下，发送的数据必须转换成ByteBuf数组
        ByteBuf encoded = channelHandlerContext.alloc().buffer(4 * response.length());
        encoded.writeBytes(response.getBytes());
        channelHandlerContext.write(encoded);
        channelHandlerContext.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) {
        // 当出现异常就关闭连接
        throwable.printStackTrace();
        channelHandlerContext.close();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext channelHandlerContext) {
        channelHandlerContext.flush();
    }
}
