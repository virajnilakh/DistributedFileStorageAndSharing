package gash.router.server.state;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public interface Handelable {
	public void handleMessage(Channel channel,WorkMessage wm);
}
