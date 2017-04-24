package gash.router.server;

import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class OutboundCommandMessageQueueHandler implements Runnable {
	ServerState state=null;
	public OutboundCommandMessageQueueHandler(ServerState s){
		 state=s;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
		
			CommandAndChannel cch=QueueHandler.dequeueOutboundCommandAndChannel();
			CommandMessage msg=cch.getMsg();
			Channel channel=cch.getChannel();
			if(msg!=null){
				if(!state.isLeader()){
					System.out.println("==========Sending stolen read request directly to the client============================");
				}
				channel.writeAndFlush(msg);
			}
		}
	}

}
