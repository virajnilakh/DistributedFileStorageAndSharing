package gash.router.client;

import java.util.concurrent.Callable;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

/*
 * Author: Ashutosh Singh
 * */
public class WriteChannel implements Callable<Long> {

	static Channel channel = null;
	CommandMessage message;

	public WriteChannel(CommandMessage msg, Channel channel) {
		message = msg;
		this.channel = channel;
	}
	
	@Override
	public Long call() {

		try {
			CommConnection.getInstance().dequeueWrite(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		channel.writeAndFlush(message);
		return (long) 1;
	}
}