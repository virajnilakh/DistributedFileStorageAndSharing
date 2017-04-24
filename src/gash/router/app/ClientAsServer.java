package gash.router.app;

import java.util.HashMap;

import gash.router.client.CommInit;
import global.Constants;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class ClientAsServer implements Runnable{
	protected static HashMap<Integer, ServerBootstrap> bootstrap = new HashMap<Integer, ServerBootstrap>();

	@Override
	public void run() {
		// TODO Auto-generated method stub
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();

		try {
			ServerBootstrap b = new ServerBootstrap();
			bootstrap.put(Constants.clientPort, b);

			b.group(bossGroup, workerGroup);
			b.channel(NioServerSocketChannel.class);
			b.option(ChannelOption.SO_BACKLOG, 100);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			// b.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR);

			boolean compressComm = false;
			b.childHandler(new CommInit(false));

			// Start the server.
			System.out.println("Starting command server listening on port = " + Constants.clientPort);
			ChannelFuture f = b.bind(Constants.clientPort).syncUninterruptibly();

			System.out.println(f.channel().localAddress() + " -> open: " + f.channel().isOpen() + ", write: "
					+ f.channel().isWritable() + ", act: " + f.channel().isActive());

			// block until the server socket is closed.
			f.channel().closeFuture().sync();

		} catch (Exception ex) {
			// on bind().sync()
			System.out.println("Error on client side ");
			ex.printStackTrace();
		} finally {
			// Shut down all event loops to terminate all threads.
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}

}
