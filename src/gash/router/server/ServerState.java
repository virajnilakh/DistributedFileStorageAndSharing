package gash.router.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;

import discovery.LocalAddress;
import gash.router.container.RoutingConf;
import gash.router.election.ElectionHandler;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.state.Handelable;
import gash.router.server.state.HandleLeaderResponseState;
import gash.router.server.state.HandleVoteReceivedState;
import gash.router.server.state.HandleVoteRequestState;
import gash.router.server.tasks.TaskList;
import io.netty.channel.Channel;
import pipe.election.Election.ElectionMessage;
import pipe.work.Work.WorkMessage;
import redis.clients.jedis.Jedis; 

public class ServerState {
	 
	public enum State {
		Follower, Leader, Candidate;
	}
	private Handelable requestHandler;
	private ElectionHandler elecHandler=new ElectionHandler(this);
	public ElectionHandler getElecHandler() {
		return elecHandler;
	}
	public void setElecHandler(ElectionHandler elecHandler) {
		this.elecHandler = elecHandler;
	}
	private Jedis jedisHandler1=null;
	private Jedis jedisHandler2=null;
	private RoutingConf conf;
	private static EdgeMonitor emon;
	private TaskList tasks;
	private boolean hasLeader=false;
	private int leaderId=0;
	private State state=State.Follower;
	private int currentTerm=0;
	private long timeout=0;
	private String leaderAddress="";
	Handelable reqVote;
	Handelable resLeader;
	Handelable voteReceived;
	
	private String ipAddress="";
	
	public ServerState() throws UnknownHostException{
		//ipAddress=LocalAddress.getLocalHostLANAddress().getHostAddress();
		ipAddress=InetAddress.getLocalHost().getHostAddress();
		System.out.println(LocalAddress.getLocalHostLANAddress().getHostAddress());
		reqVote=new HandleVoteRequestState(this);
		resLeader=new HandleLeaderResponseState(this);
		voteReceived=new HandleVoteReceivedState(this);
		jedisHandler1=new Jedis("localhost",6379);
		//jedisHandler2=new Jedis("localhost",6379);
		/*try{
			ipAddress=InetAddress.getLocalHost().getHostAddress();
		}catch(Exception e){
			e.printStackTrace();
		}*/
	}
	public String getIpAddress() {
		return ipAddress;
	}
	public Jedis getJedisHandler1() {
		return jedisHandler1;
	}
	
	
	public long getTimeout() {
		return timeout;
	}
	public void becomeLeader(){
		this.state=State.Leader;
	}
	public void becomeFollower(){
		this.state=State.Follower;
	}
	public void becomeCandidate(){
		this.state=State.Candidate;
	}
	public boolean isFollower(){
		if(this.state==State.Follower)
			return true;
		else
			return false;
	}
	public boolean isLeader(){
		if(this.state==State.Leader)
			return true;
		else
			return false;
	}
	public boolean isCandidate(){
		if(this.state==State.Candidate)
			return true;
		else
			return false;
	}
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public int getCurrentTerm() {
		return currentTerm;
	}

	public void setCurrentTerm(int currentTerm) {
		this.currentTerm = currentTerm;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public int getLeaderId() {
		return leaderId;
	}
	public synchronized void handleMessage(Channel channel, WorkMessage wm) {
		// TODO Auto-generated method stub
		ElectionMessage electionMessage = wm.getElectionMessage();
        switch (electionMessage.getType()) {
            case ASKFORVOTE:
				System.out.println("Got a vote request:");

            	if(electionMessage.getTerm()>getCurrentTerm()){
                    setCurrentTerm(electionMessage.getTerm());
                    elecHandler.setVote2TermMap(electionMessage.getTerm(),false);
                }
            	requestHandler=reqVote;
            	requestHandler.handleMessage(channel, wm);
            	break;
            case LEADERRESPONSE:
				System.out.println("Got response from leader:");

            	requestHandler=resLeader;
            	requestHandler.handleMessage(channel, wm);
            	break;
            case VOTE:
				System.out.println("Got a vote:");

            	requestHandler=voteReceived;
            	requestHandler.handleMessage(channel, wm);
            	break;
        }
	}
	public String getLeaderAddress() {
		return leaderAddress;
	}
	public void setLeaderAddress(String leaderAddress) {
		this.leaderAddress = leaderAddress;
	}
	public boolean isHasLeader() {
		return hasLeader;
	}

	public void setHasLeader(boolean hasLeader) {
		this.hasLeader = hasLeader;
	}

	public void setLeaderId(int leaderId) {
		this.leaderId = leaderId;

	}

	public RoutingConf getConf() {
		return conf;
	}

	public void setConf(RoutingConf conf) {
		this.conf = conf;
	}

	public static EdgeMonitor getEmon() {
		return emon;
	}

	public static void setEmon(EdgeMonitor emon) {
		emon = emon;
	}

	public TaskList getTasks() {
		return tasks;
	}

	public void setTasks(TaskList tasks) {
		this.tasks = tasks;
	}

}
