private static class ElectionTimer extends TimerTask{
    private ServerState s;
    private ElectionHandler e;

    public ElectionTimer(ServerState s,ElectionHandler e){
        state=s;
        electionHandler=e;
    }
    @Ovveride
    public void run(){}
}
