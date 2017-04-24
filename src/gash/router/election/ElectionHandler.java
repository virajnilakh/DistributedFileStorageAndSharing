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

public class ElectionHandler {
	private static ServerState state;
	private static Timer timer;
	private static boolean hasVoted = false;
	private HashMap<Integer, Boolean> vote2TermMap = new HashMap<Integer, Boolean>();

	public boolean getVote2TermMap(int key) {
		return vote2TermMap.get(key);
	}

	public void setVote2TermMap(int key, boolean value) {
		this.vote2TermMap.put(key, value);
	}

	private static int voteCount = 1;

	public static synchronized void incrementVoteCount() {
		voteCount++;
	}

	public static boolean getHasVoted() {
		return hasVoted;
	}

	public void setHasVoted(boolean b) {
		hasVoted = b;
	}

	public ElectionHandler(ServerState s) {
		state = s;
		state.setCurrentTerm(0);
		timer = new Timer();
		vote2TermMap.put(state.getCurrentTerm(), false);
	}

	public static WorkMessage buildLeaderResponse(int nodeId, int currentTerm) {
		// TODO Auto-generated method stub
		WorkMessage.Builder workMessage = WorkMessage.newBuilder();
		Header.Builder header = Header.newBuilder();
		ElectionMessage.Builder electionMessage = ElectionMessage.newBuilder();
		VotingInfo.Builder infoMsgBuilder = VotingInfo.newBuilder();
		LeaderStatus.Builder status = LeaderStatus.newBuilder();

		status.setState(LeaderState.LEADERALIVE);
		status.setLeaderId(nodeId);
		status.setLeaderHost(state.getIpAddress());
		header.setElection(true);
		
		header.setNodeId(state.getConf().getNodeId());
		header.setTime(System.currentTimeMillis());

		// infoMsgBuilder.setCandidateID(candidate);
		// infoMsgBuilder.setIsVoteGranted(isGranted);

		electionMessage.setType(ElectionMessageType.LEADERRESPONSE);
		electionMessage.setTerm(currentTerm);
		electionMessage.setInfo(infoMsgBuilder);

		workMessage.setLeaderStatus(status);
		workMessage.setElectionMessage(electionMessage);
		workMessage.setSecret(789456);
		workMessage.setHeader(header);
		return workMessage.build();
	}

	public boolean checkIfLeader(WorkMessage wm) {
		// TODO Auto-generated method stub
		if (voteCount >= state.getEmon().getOutboundEdges().size() / 2 + 1) {
			return true;
		} else {
			return false;
		}
	}

	public static WorkMessage buildVote(int candidate, boolean isGranted, int term) {
		WorkMessage.Builder workMessage = WorkMessage.newBuilder();
		Header.Builder header = Header.newBuilder();
		ElectionMessage.Builder electionMessage = ElectionMessage.newBuilder();
		VotingInfo.Builder infoMsgBuilder = VotingInfo.newBuilder();
		LeaderStatus.Builder status = LeaderStatus.newBuilder();

		status.setState(LeaderState.LEADERUNKNOWN);

		header.setElection(true);
		header.setNodeId(state.getConf().getNodeId());
		header.setTime(System.currentTimeMillis());

		infoMsgBuilder.setCandidateID(candidate);
		infoMsgBuilder.setIsVoteGranted(isGranted);

		electionMessage.setType(ElectionMessageType.VOTE);
		electionMessage.setTerm(term);
		electionMessage.setInfo(infoMsgBuilder);

		workMessage.setLeaderStatus(status);
		workMessage.setElectionMessage(electionMessage);
		workMessage.setSecret(789456);
		workMessage.setHeader(header);
		return workMessage.build();
	}

	public static WorkMessage createAskForVoteMessage(long timeout, int term) {
		WorkMessage.Builder workMessage = WorkMessage.newBuilder();
		Header.Builder header = Header.newBuilder();
		ElectionMessage.Builder electionMessage = ElectionMessage.newBuilder();
		VotingInfo.Builder infoMsgBuilder = VotingInfo.newBuilder();
		LeaderStatus.Builder status = LeaderStatus.newBuilder();

		status.setState(LeaderState.LEADERUNKNOWN);

		header.setElection(true);
		header.setNodeId(state.getConf().getNodeId());
		header.setTime(timeout);

		infoMsgBuilder.setCandidateID(state.getConf().getNodeId());

		electionMessage.setType(ElectionMessageType.ASKFORVOTE);
		electionMessage.setTerm(term);
		electionMessage.setInfo(infoMsgBuilder);

		workMessage.setLeaderStatus(status);
		workMessage.setElectionMessage(electionMessage);
		workMessage.setSecret(789456);
		workMessage.setHeader(header);
		return workMessage.build();

	}

	public synchronized void initElection() {
		int randomTimeout = (2000 + (new Random()).nextInt(3500)) * state.getConf().getNodeId();
		timer.schedule(new ElectionTimer(), (long) randomTimeout, (long) randomTimeout);
	}

	public static Timer getTimer() {
		return timer;
	}

	public static void setTimer() {
		timer = null;
		timer = new Timer();
		int randomTimeout = (2000 + (new Random()).nextInt(3500)) * (state.getConf().getNodeId() + 2);
		timer.schedule(new ElectionTimer(), (long) randomTimeout, (long) randomTimeout);
	}

	private static class ElectionTimer extends TimerTask {

		@Override
		public void run() {

			/*
			 * if(getHasVoted()){ timer.cancel(); }
			 */

			if (state.isCandidate()) {
				state.becomeFollower();
				state.setCurrentTerm(state.getCurrentTerm() - 1);

			}
			
			if (state.isFollower()) {
				try {
					System.out.println("---Timer timed out---");

					state.becomeCandidate();
					state.setTimeout(System.currentTimeMillis());
					System.out.println("Asking for vote:");
					state.setCurrentTerm(state.getCurrentTerm() + 1);
					WorkMessage electionMessage = ElectionHandler.createAskForVoteMessage(state.getTimeout(),
							state.getCurrentTerm());
					state.getEmon().broadcast(electionMessage);
				} catch (Exception e) {
					System.out.println("Error occured in timer");
					e.printStackTrace();
				}

			}
		}
	}

}
