package cn.netty2;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @author 魏喜明
 */
public class SimpleClient {

    public void connect(String host, int port) throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            socketChannel.pipeline().addLast(new SimpleClientHandler());
                        }
                    });

            // Start the client.
            ChannelFuture channelFuture = bootstrap.connect(host, port).sync();
            System.out.println("客户端已启动！");
            // Wait until the connection is closed.
            channelFuture.channel().closeFuture().sync();
        } finally {
            System.out.println("客户端已关闭！");
            workerGroup.shutdownGracefully();
        }
    }
}
