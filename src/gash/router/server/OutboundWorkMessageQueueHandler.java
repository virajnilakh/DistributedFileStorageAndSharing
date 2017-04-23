package gash.router.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.common.Common.Request;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class OutboundWorkMessageQueueHandler implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			WorkAndChannel wch=QueueHandler.dequeueOutboundWorkAndChannel();
			WorkMessage msg=wch.getMsg();
			if(msg.getReq().getRequestType() == Request.RequestType.WRITEFILE){
				System.out.println(" ");
			}
			ChannelFuture cf =null;
			Channel channel=wch.getChannel();
			if(msg!=null){
				cf=channel.write(msg);
			}
			channel.flush();
			if (cf.isDone() && !cf.isSuccess()) {
				System.out.println("failed in outbound work message handler");

			}
		}
	}

}
