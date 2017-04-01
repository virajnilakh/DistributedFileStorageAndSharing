public class ElectionHandler{
    private ServerState state;
    private Timer timer;
    private boolean hasVoted=false;
    private HashMap<Integer,Boolean> vote2TermMap=new HashMap<Integer,Boolean>();
    public boolean getHasVoted(){
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
            if(state.getState()==Follower){
                if(vote2TermMap.get(sate.getCurrentTerm())){


                }else{
                    WorkMessage vote = ElectionHandler.buildVote(wm.getElectionMessage().getInfo().getCandidateID(),true,state.getCurrentTerm());
                    vote2TermMap.put(electionMessage.getTerm(),true);
                    hasVoted=true;
                    System.out.println("Voted for "+wm.getElectionMessage().getInfo().getCandidateID());
                    ChannelFuture cf= channel.writeAndFlush(vote);
                    cf.awaitUninterruptibly();
                }
            }else if(state.getState()==Candidate){

            }else{

            }
            case LEADERRESPONSE:
            case VOTE:
        }
    }
    public WorkMessage buildVote(int candidate,boolean isGranted,int term){

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
