package gash.router.server;

import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class OutboundCommandMessageQueueHandler implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			CommandAndChannel cch=QueueHandler.dequeueOutboundCommandAndChannel();
			CommandMessage msg=cch.getMsg();
			Channel channel=cch.getChannel();
			if(msg!=null){
				channel.writeAndFlush(msg);
			}
		}
	}

}
