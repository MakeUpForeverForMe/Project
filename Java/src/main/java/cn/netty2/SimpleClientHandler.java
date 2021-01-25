package cn.netty2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;

/**
 * @author 魏喜明
 */
public class SimpleClientHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext channelHandlerContext, Object msg) {
        System.out.println("SimpleClientHandler.channelRead，客户端读入数据");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        result.readBytes(result1);
        System.out.println("Server said（服务端说）：" + new String(result1));
        result.release();
        AttributeKey<String> key = AttributeKey.valueOf("ServerData");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable cause) {
        // 当出现异常就关闭连接
        cause.printStackTrace();
        channelHandlerContext.close();
    }


    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext channelHandlerContext) {
        String msg = "客户端发送：hello Server!";
        ByteBuf encoded = channelHandlerContext.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        channelHandlerContext.write(encoded);
        channelHandlerContext.flush();
    }
}
