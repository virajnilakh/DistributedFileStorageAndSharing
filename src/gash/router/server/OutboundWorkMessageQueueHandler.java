package gash.router.server;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class OutboundWorkMessageQueueHandler implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			WorkAndChannel wch=QueueHandler.dequeueOutboundWorkAndChannel();
			WorkMessage msg=wch.getMsg();
			Channel channel=wch.getChannel();
			if(msg!=null){
				channel.writeAndFlush(msg);
			}
		}
	}

}
