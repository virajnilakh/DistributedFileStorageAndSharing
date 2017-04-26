package gash.router.server;

import java.util.concurrent.LinkedBlockingDeque;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;


public class QueueHandler implements Runnable {
	private ServerState state;
	private static LinkedBlockingDeque<CommandAndChannel> cmdMsgInboundQueue = new LinkedBlockingDeque<CommandAndChannel>();
	private static LinkedBlockingDeque<CommandAndChannel> cmdMsgOutboundQueue = new LinkedBlockingDeque<CommandAndChannel>();
	private static LinkedBlockingDeque<WorkAndChannel> workMsgInboundQueue = new LinkedBlockingDeque<WorkAndChannel>();

	public static LinkedBlockingDeque<WorkAndChannel> getWorkMsgInboundQueue() {
		return workMsgInboundQueue;
	}

	public static void setWorkMsgInboundQueue(LinkedBlockingDeque<WorkAndChannel> workMsgInboundQueue) {
		QueueHandler.workMsgInboundQueue = workMsgInboundQueue;
	}

	public static LinkedBlockingDeque<WorkAndChannel> getWorkMsgOutboundQueue() {
		return workMsgOutboundQueue;
	}

	public static void setWorkMsgOutboundQueue(LinkedBlockingDeque<WorkAndChannel> wm) {
		workMsgOutboundQueue = wm;
	}
	private static LinkedBlockingDeque<WorkAndChannel> workMsgOutboundQueue = new LinkedBlockingDeque<WorkAndChannel>();
	
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

	public static LinkedBlockingDeque<CommandAndChannel> getCmdMsgInboundQueue() {
		return cmdMsgInboundQueue;
	}

	public static void setCmdMsgInboundQueue(LinkedBlockingDeque<CommandAndChannel> cmdMsgInboundQueue) {
		QueueHandler.cmdMsgInboundQueue = cmdMsgInboundQueue;
	}

	public static LinkedBlockingDeque<CommandAndChannel> getCmdMsgOutboundQueue() {
		return cmdMsgOutboundQueue;
	}

	public static void setCmdMsgOutboundQueue(LinkedBlockingDeque<CommandAndChannel> cmdMsgOutboundQueue) {
		QueueHandler.cmdMsgOutboundQueue = cmdMsgOutboundQueue;
	}

	public static  void  enqueueInboundCommandAndChannel(CommandMessage msg,Channel channel) {
		CommandAndChannel cch=new CommandAndChannel(msg,channel);
		// TODO Auto-generated method stub
		cmdMsgInboundQueue.add(cch);
	}

	public static CommandAndChannel dequeueInboundCommandAndChannel() {
		// TODO Auto-generated method stub
		CommandAndChannel msg=null;
		try {
			msg= cmdMsgInboundQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return msg;
	}
	public static void enqueueInboundWorkAndChannel(WorkMessage msg,Channel channel){
		WorkAndChannel wch=new WorkAndChannel(msg,channel);
		workMsgInboundQueue.add(wch);
	}
	public static void enqueueOutboundWorkAndChannel(WorkMessage msg,Channel channel){
		WorkAndChannel wch=new WorkAndChannel(msg,channel);
		workMsgOutboundQueue.add(wch);
	}
	public static WorkAndChannel dequeueInboundWorkAndChannel(){
		WorkAndChannel msg=null;
		try {
			msg= workMsgInboundQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return msg;
		
	}
	public static WorkAndChannel dequeueOutboundWorkAndChannel(){
		WorkAndChannel msg=null;
		try {
			msg= workMsgOutboundQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return msg;
		
	}
	public static void enqueueOutboundCommandAndChannel(CommandMessage msg,Channel channel) {
		CommandAndChannel cch=new CommandAndChannel(msg,channel);
		// TODO Auto-generated method stub
		cmdMsgOutboundQueue.add(cch);
	}

	public static CommandAndChannel dequeueOutboundCommandAndChannel() {
		// TODO Auto-generated method stub
		CommandAndChannel msg=null;
		try {
			msg= cmdMsgInboundQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return msg;
	}

	

}
