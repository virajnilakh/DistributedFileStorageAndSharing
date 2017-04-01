public class ElectionHandler{
    private ServerState state;
    private Timer timer;
    private boolean hasVoted=false;

    public ElectionHandler(ServerState s){
        state=s;
        timer=new Timer();
    }
    public void initElection(){
        int randomTimeout=(1000+(new Random).nextInt(3500))*state.getConf().getNodeId();
        timer.schedule(new ElectionHandler(state,this),(long)randomTimeout,(long)randomTimeout);
    }
}
