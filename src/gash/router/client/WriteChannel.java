package gash.router.client;


import java.util.concurrent.Callable;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class WriteChannel implements Callable<Long>{

	//long id=0;
	//ArrayList<CommandMessage> messages= new ArrayList<CommandMessage>();
	CommandMessage message;
	Channel channel;
	public WriteChannel(CommandMessage msg,Channel ch){
		message=msg;
		channel=ch;
	}
	
	/*Needs refactoring*/
	@Override
	public Long call(){
		channel.writeAndFlush(message);		
		return (long)1;
	}
}