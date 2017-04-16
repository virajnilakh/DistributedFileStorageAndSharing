package gash.router.server.state;

import gash.router.server.ServerState;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.election.Election.ElectionMessage;
import pipe.work.Work.WorkMessage;

public class HandleVoteRequestState implements Handelable{
	ServerState state;
	public HandleVoteRequestState(ServerState s){
		state=s;
	}
	@Override
	public synchronized void handleMessage(Channel channel, WorkMessage wm) {
		// TODO Auto-generated method stub
		ElectionMessage electionMessage = wm.getElectionMessage();
		
		switch(state.getState()){
    	case Follower:
    		if(state.getElecHandler().getVote2TermMap(state.getCurrentTerm())){
				

            }else{
            	vote(channel,electionMessage);

            }
    		break;
    	case Candidate:
    		if(state.getTimeout()<wm.getHeader().getTime()){
    			
    		}else{
    			state.becomeFollower();
    			vote(channel,electionMessage);
    		}
    		break;
    	case Leader:
    		if(state.getCurrentTerm()<electionMessage.getTerm()){
    			state.becomeFollower();
    			vote(channel,electionMessage);
    			
    		}else{
    			WorkMessage hb=state.getElecHandler().buildLeaderResponse(state.getConf().getNodeId(), state.getCurrentTerm());
    			channel.writeAndFlush(hb);
    		}
    		break;
    	default:
    		break;
	}

}
public void vote(Channel channel,ElectionMessage electionMessage){
	WorkMessage vote = state.getElecHandler().buildVote(electionMessage.getInfo().getCandidateID(),true,state.getCurrentTerm());
	state.getElecHandler().setVote2TermMap(electionMessage.getTerm(),true);
    state.getElecHandler().setHasVoted(true);
    System.out.println("Voted for "+electionMessage.getInfo().getCandidateID());
    ChannelFuture cf = channel.writeAndFlush(vote);
    cf.awaitUninterruptibly();
}
}
