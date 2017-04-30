package replication;

import java.util.concurrent.Callable;

import gash.router.client.CommConnection;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class Replication implements Callable<Long> {

	static Channel channel = null;
	WorkMessage message;

	public Replication(WorkMessage msg, Channel channel) {
		message = msg;
		this.channel = channel;
	}

	@Override
	public Long call() throws Exception {
		try {
			//Get Connection to new node
			//CommConnection.getInstance().dequeueWrite(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		channel.writeAndFlush(message);
		return (long) 1;
	}

}
