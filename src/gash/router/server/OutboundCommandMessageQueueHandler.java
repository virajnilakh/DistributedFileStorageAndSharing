package gash.router.server;

import routing.Pipe.CommandMessage;

public class OutboundCommandMessageQueueHandler implements Runnable{
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			CommandMessage msg=QueueHandler.dequeueOutboundCommandMessage();
			if(msg!=null){
				ServerState.getClientChannel().writeAndFlush(msg);
			}
		}
	}

}
