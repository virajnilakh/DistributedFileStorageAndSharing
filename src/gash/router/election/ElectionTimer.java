private static class ElectionTimer extends TimerTask{
    private ServerState state;
    private ElectionHandler electionHandler;
    public ElectionTimer(ServerState s,ElectionHandler e){
        state=s;
        electionHandler=e;
    }
    @Ovveride
    public void run(){
        if(e.getHasVoted()){
            timer.cancel();
        }
        if(state.isFollower() && !e.getHasVoted()){
            try{
                state.becomeCandidate();
                state.setTimeout(System.currentTimeMillis());
                WorkMessage electionMessage = e.createAskForVoteMessage(state.getTimeout(), state.getCurrentTerm());
                state.getEmon().broadcast(electionMessage);
            }catch(Exception e){
                System.out.println("Error occured in timer");
            }

        }
    }
}
