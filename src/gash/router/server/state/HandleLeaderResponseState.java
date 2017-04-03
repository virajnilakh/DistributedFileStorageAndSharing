package gash.router.server.state;

import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class HandleLeaderResponseState implements Handelable {
	ServerState state;
	public HandleLeaderResponseState(ServerState s){
		state=s;
	}
	@Override
	public synchronized void handleMessage(Channel channel, WorkMessage wm) {
		// TODO Auto-generated method stub
		state.setLeaderId(wm.getLeader().getLeaderId());
		state.setLeaderAddress(wm.getLeader().getLeaderHost());
	}

}
