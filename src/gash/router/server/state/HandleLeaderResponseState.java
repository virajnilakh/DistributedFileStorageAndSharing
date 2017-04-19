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
		state.getElecHandler().getTimer().cancel();
		state.getElecHandler().setTimer();
		try{
			state.getLocalhostJedis().select(0);
			state.getLocalhostJedis().set("1", wm.getLeaderStatus().getLeaderHost()+":4568");
			System.out.println("---Redis updated---");
			
		}catch(Exception e){
			System.out.println("---Problem with redis at HandleLeaderResponse---");
		}
		state.setLeaderId(wm.getLeaderStatus().getLeaderId());
		state.setLeaderAddress(wm.getLeaderStatus().getLeaderHost());
		state.becomeFollower();
	}

}
