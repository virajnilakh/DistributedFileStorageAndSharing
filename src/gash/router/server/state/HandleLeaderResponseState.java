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
		System.out.println("New Leader elected is "+wm.getLeaderStatus().getLeaderId()+"and "+wm.getLeaderStatus().getLeaderHost());
		state.setLeaderId(wm.getLeaderStatus().getLeaderId());
		state.setLeaderAddress(wm.getLeaderStatus().getLeaderHost());
		state.becomeFollower();
	}

}
