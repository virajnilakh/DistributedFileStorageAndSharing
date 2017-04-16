package gash.router.server.state;

import gash.router.election.ElectionHandler;
import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class HandleVoteReceivedState implements Handelable{
	ServerState state;
	public HandleVoteReceivedState(ServerState s){
		state=s;
	}
	@Override
	public synchronized void handleMessage(Channel channel, WorkMessage wm) {
		// TODO Auto-generated method stub
		if(state.isCandidate()){
    		System.out.println("Received Vote!!!");
    		if(wm.getElectionMessage().getInfo().getIsVoteGranted()){
    			state.getElecHandler().incrementVoteCount();
    		}
    		boolean leader=state.getElecHandler().checkIfLeader(wm);
    		if(leader){
    			state.becomeLeader();
    			state.setLeaderId(state.getConf().getNodeId());
        		System.out.println("Node:"+state.getConf().getNodeId()+" is the Leader!!");
				WorkMessage response = state.getElecHandler().buildLeaderResponse(state.getConf().getNodeId(), state.getCurrentTerm());
				state.getEmon().broadcast(response);
				if(state.getJedisHandler1().ping().equals("PONG")){
					state.getJedisHandler1().set("1", state.getIpAddress()+":4568");
					System.out.println("---Updated redis server!---");
				}
				
				
    		}
    	}
	}

}
