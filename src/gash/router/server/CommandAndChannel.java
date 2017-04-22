package gash.router.server;

import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class CommandAndChannel {
	private CommandMessage msg=null;
	private Channel channel=null;
	public CommandAndChannel(CommandMessage msg,Channel channel){
		this.msg=msg;
		this.channel=channel;
	}
	public CommandMessage getMsg() {
		return msg;
	}
	public void setMsg(CommandMessage msg) {
		this.msg = msg;
	}
	public Channel getChannel() {
		return channel;
	}
	public void setChannel(Channel channel) {
		this.channel = channel;
	}
	
}
