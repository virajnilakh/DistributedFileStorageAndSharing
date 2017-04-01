public class ElectionHandler{
    private ServerState state;
    private Timer timer;
    private boolean hasVoted=false;
    public boolean getHasVoted(){
        return hasVoted;
    }
    public void setHasVoted(boolean b){
        hasVoted=b;
    }
    public ElectionHandler(ServerState s){
        state=s;
        timer=new Timer();
    }
    public WorkMessage createAskForVoteMessage(long timeout,int term){
        WorkMessage.Builder workMessage = WorkMessage.newBuilder();
		Header.Builder header = Header.newBuilder();
		ElectionMessage.Builder electionMessage = ElectionMessage.newBuilder();
		VotingInfo.Builder infoMsgBuilder = VotingInfo.newBuilder();
		LeaderStatus.Builder status=LeaderStatus.newBuilder();

		status.setState(LeaderState.LEADERUNKNOWN);

		header.setElection(true);
		header.setNodeId(state.getConf().getNodeId());
		header.setTime(electionTimeoutTime);

		infoMsgBuilder.setCandidateID(state.getConf().getNodeId());

		electionMessage.setType(ElectionMessageType.ASKFORVOTE);
		electionMessage.setTerm(currentTerm);
        electionMessage.setInfo(infoMsgBuilder);

		workMessage.setLeader(status);
		workMessage.setElectionMessage(electionMessage);
		workMessage.setSecret(789456);
		workMessage.setHeader(header);
		return workMessage.build();

    }
    public void initElection(){
        int randomTimeout=(1000+(new Random).nextInt(3500))*state.getConf().getNodeId();
        timer.schedule(new ElectionHandler(state,this),(long)randomTimeout,(long)randomTimeout);
    }
}
