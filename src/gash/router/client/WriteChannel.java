package gash.router.client;

import java.util.concurrent.Callable;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class WriteChannel implements Callable<Long> {

	static Channel channel = null;

	// long id=0;
	// ArrayList<CommandMessage> messages= new ArrayList<CommandMessage>();
	CommandMessage message;

	public WriteChannel(CommandMessage msg, Channel channel) {
		message = msg;
		this.channel = channel;
	}

	/* Needs refactoring */
	@Override
	public Long call() {

		try {
			CommConnection.getInstance().dequeueWrite(message);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		channel.writeAndFlush(message);
		return (long) 1;
	}
}