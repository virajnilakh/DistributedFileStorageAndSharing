package gash.router.election;

import java.util.HashMap;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import gash.router.server.ServerState;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.common.Common.Header;
import pipe.election.Election.ElectionMessage;
import pipe.election.Election.ElectionMessage.ElectionMessageType;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.election.Election.VotingInfo;
import pipe.work.Work.WorkMessage;

public class ElectionHandler{
    private static ServerState state;
    private static Timer timer;
    private static boolean hasVoted=false;
    private HashMap<Integer,Boolean> vote2TermMap=new HashMap<Integer,Boolean>();
    public static boolean getHasVoted(){
        return hasVoted;
    }
    public void setHasVoted(boolean b){
        hasVoted=b;
    }
    public ElectionHandler(ServerState s){
        state=s;
        state.setCurrentTerm(0);
        timer=new Timer();
    }
    public synchronized void handleMessage(Channel channel,WorkMessage wm){
        ElectionMessage electionMessage = wm.getElectionMessage();
        switch (electionMessage.getType()) {
            case ASKFORVOTE:
            if(electionMessage.getTerm()>state.getCurrentTerm()){
                state.setCurrentTerm(electionMessage.getTerm());
                vote2TermMap.put(electionMessage.getTerm(),false);
            }
            switch(state.getState()){
            	case Follower:
            		if(vote2TermMap.get(state.getCurrentTerm())){
    					

                    }else{
                        WorkMessage vote = buildVote(wm.getElectionMessage().getInfo().getCandidateID(),true,state.getCurrentTerm());
                        vote2TermMap.put(electionMessage.getTerm(),true);
                        hasVoted=true;
                        System.out.println("Voted for "+wm.getElectionMessage().getInfo().getCandidateID());
                        ChannelFuture cf = channel.writeAndFlush(vote);
                        cf.awaitUninterruptibly();
    					vote2TermMap.put(state.getCurrentTerm(), true);

                    }
            		break;
            	case Candidate:
            		if(state.getTimeout()<wm.getHeader().getTime()){
            			
            		}else{
            			state.becomeFollower();
            			WorkMessage vote = buildVote(wm.getElectionMessage().getInfo().getCandidateID(),true,state.getCurrentTerm());
                        vote2TermMap.put(electionMessage.getTerm(),true);
                        hasVoted=true;
                        System.out.println("Voted for "+wm.getElectionMessage().getInfo().getCandidateID());
                        ChannelFuture cf = channel.writeAndFlush(vote);
                        cf.awaitUninterruptibly();
    					vote2TermMap.put(state.getCurrentTerm(), true);
            		}
            		break;
            	case Leader:
            		break;
            }
            
            case LEADERRESPONSE:
            	
            		System.out.println("New Leader elected is"+wm.getLeader().getLeaderId());
            		state.setLeaderId(wm.getLeader().getLeaderId());
            		state.setLeaderAddress(wm.getLeader().getLeaderHost());
            	
            case VOTE:
            	if(state.isCandidate()){
            		System.out.println("Received Vote!!!");
            		boolean leader=checkIfLeader(wm);
            		if(leader){
            			state.becomeLeader();
                		System.out.println("NOde:"+state.getConf().getNodeId()+" is the Leader!!");
						WorkMessage response = ElectionHandler.buildLeaderResponse(state.getConf().getNodeId(), state.getCurrentTerm());
						state.getEmon().broadcast(response);
						
            		}
            	}
        }
    }
    private static WorkMessage buildLeaderResponse(int nodeId, int currentTerm) {
		// TODO Auto-generated method stub
		return null;
	}
	private boolean checkIfLeader(WorkMessage wm) {
		// TODO Auto-generated method stub
		return false;
	}
	public static WorkMessage buildVote(int candidate,boolean isGranted,int term){
    	WorkMessage.Builder workMessage = WorkMessage.newBuilder();
		Header.Builder header = Header.newBuilder();
		ElectionMessage.Builder electionMessage = ElectionMessage.newBuilder();
		VotingInfo.Builder infoMsgBuilder = VotingInfo.newBuilder();
		LeaderStatus.Builder status=LeaderStatus.newBuilder();

		status.setState(LeaderState.LEADERUNKNOWN);

		header.setElection(true);
		header.setNodeId(state.getConf().getNodeId());
		header.setTime(System.currentTimeMillis());

		infoMsgBuilder.setCandidateID(candidate);
		infoMsgBuilder.setIsVoteGranted(isGranted);

		electionMessage.setType(ElectionMessageType.VOTE);
		electionMessage.setTerm(term);
        electionMessage.setInfo(infoMsgBuilder);

		workMessage.setLeader(status);
		workMessage.setElectionMessage(electionMessage);
		workMessage.setSecret(789456);
		workMessage.setHeader(header);
		return workMessage.build();
    }
    public static WorkMessage createAskForVoteMessage(long timeout,int term){
        WorkMessage.Builder workMessage = WorkMessage.newBuilder();
		Header.Builder header = Header.newBuilder();
		ElectionMessage.Builder electionMessage = ElectionMessage.newBuilder();
		VotingInfo.Builder infoMsgBuilder = VotingInfo.newBuilder();
		LeaderStatus.Builder status=LeaderStatus.newBuilder();

		status.setState(LeaderState.LEADERUNKNOWN);

		header.setElection(true);
		header.setNodeId(state.getConf().getNodeId());
		header.setTime(timeout);

		infoMsgBuilder.setCandidateID(state.getConf().getNodeId());

		electionMessage.setType(ElectionMessageType.ASKFORVOTE);
		electionMessage.setTerm(term);
        electionMessage.setInfo(infoMsgBuilder);

		workMessage.setLeader(status);
		workMessage.setElectionMessage(electionMessage);
		workMessage.setSecret(789456);
		workMessage.setHeader(header);
		return workMessage.build();

    }
    public void initElection(){
        int randomTimeout=(1000+(new Random()).nextInt(3500))*state.getConf().getNodeId();
        timer.schedule(new ElectionTimer(),(long)randomTimeout,(long)randomTimeout);
    }
    private static class ElectionTimer extends TimerTask{
        
        @Override
        public void run(){
            if(ElectionHandler.getHasVoted()){
                timer.cancel();
            }
            if(state.isFollower() && !ElectionHandler.getHasVoted()){
                try{
                    state.becomeCandidate();
                    state.setTimeout(System.currentTimeMillis());
                    WorkMessage electionMessage = ElectionHandler.createAskForVoteMessage(state.getTimeout(), state.getCurrentTerm());
                    state.getEmon().broadcast(electionMessage);
                }catch(Exception e){
                    System.out.println("Error occured in timer");
                }

            }
        }
    }

}
