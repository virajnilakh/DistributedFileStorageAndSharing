package gash.router.server;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class WorkAndChannel {
	private WorkMessage msg=null;
	private Channel channel=null;
	public WorkAndChannel(WorkMessage msg,Channel channel){
		this.msg=msg;
		this.channel=channel;
	}
	public WorkMessage getMsg() {
		return msg;
	}
	public void setMsg(WorkMessage msg) {
		this.msg = msg;
	}
	public Channel getChannel() {
		return channel;
	}
	public void setChannel(Channel channel) {
		this.channel = channel;
	}

}
