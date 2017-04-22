package gash.router.server;

import java.util.concurrent.LinkedBlockingDeque;

import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class QueueHandler implements Runnable {
	private ServerState state;
	private static LinkedBlockingDeque<CommandMessage> cmdMsgInboundQueue = new LinkedBlockingDeque<CommandMessage>();
	private static LinkedBlockingDeque<CommandMessage> cmdMsgOutboundQueue = new LinkedBlockingDeque<CommandMessage>();
	private static LinkedBlockingDeque<WorkMessage> workMsgInboundQueue = new LinkedBlockingDeque<WorkMessage>();
	public static LinkedBlockingDeque<WorkMessage> getWorkMsgInboundQueue() {
		return workMsgInboundQueue;
	}

	public static void setWorkMsgInboundQueue(LinkedBlockingDeque<WorkMessage> workMsgInboundQueue) {
		QueueHandler.workMsgInboundQueue = workMsgInboundQueue;
	}

	public static LinkedBlockingDeque<WorkMessage> getWorkMsgOutboundQueue() {
		return workMsgOutboundQueue;
	}

	public static void setWorkMsgOutboundQueue(LinkedBlockingDeque<WorkMessage> workMsgOutboundQueue) {
		QueueHandler.workMsgOutboundQueue = workMsgOutboundQueue;
	}
	private static LinkedBlockingDeque<WorkMessage> workMsgOutboundQueue = new LinkedBlockingDeque<WorkMessage>();
	
	QueueHandler(ServerState state){
		this.state = state;
	}
	boolean forever = true;
	
	@Override
	public void run() {
		
		// TODO Auto-generated method stub
		while(forever){
			
		}
			
	}

	public static LinkedBlockingDeque<CommandMessage> getCmdMsgInboundQueue() {
		return cmdMsgInboundQueue;
	}

	public static void setCmdMsgInboundQueue(LinkedBlockingDeque<CommandMessage> cmdMsgInboundQueue) {
		QueueHandler.cmdMsgInboundQueue = cmdMsgInboundQueue;
	}

	public static LinkedBlockingDeque<CommandMessage> getCmdMsgOutboundQueue() {
		return cmdMsgOutboundQueue;
	}

	public static void setCmdMsgOutboundQueue(LinkedBlockingDeque<CommandMessage> cmdMsgOutboundQueue) {
		QueueHandler.cmdMsgOutboundQueue = cmdMsgOutboundQueue;
	}

	public static void enqueueInboundCommandMessage(CommandMessage msg) {
		// TODO Auto-generated method stub
		cmdMsgInboundQueue.add(msg);
	}

	public static CommandMessage dequeueInboundCommandMessage() {
		// TODO Auto-generated method stub
		CommandMessage msg=null;
		try {
			msg= cmdMsgInboundQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return msg;
	}
	public static void enqueueOutboundCommandMessage(CommandMessage msg) {
		// TODO Auto-generated method stub
		cmdMsgOutboundQueue.add(msg);
	}

	public static CommandMessage dequeueOutboundCommandMessage() {
		// TODO Auto-generated method stub
		CommandMessage msg=null;
		try {
			msg= cmdMsgInboundQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return msg;
	}

}
