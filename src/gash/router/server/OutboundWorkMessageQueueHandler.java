package gash.router.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.work.Work.WorkMessage;

public class OutboundWorkMessageQueueHandler implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			WorkAndChannel wch=QueueHandler.dequeueOutboundWorkAndChannel();
			WorkMessage msg=wch.getMsg();
			/*if(msg.getReq().getRequestType() == RequestType.WRITEFILE){
				System.out.println(" ");
			}*/
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
