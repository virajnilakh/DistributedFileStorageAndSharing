package gash.router.server;

import java.util.concurrent.ConcurrentHashMap;

import gash.router.container.RoutingConf;
import gash.router.election.ElectionHandler;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.tasks.TaskList;

public class ServerState {
	public enum State {
		Follower, Leader, Candidate;
	}
	private ElectionHandler elecHandler=new ElectionHandler(this);
	public ElectionHandler getElecHandler() {
		return elecHandler;
	}
	public void setElecHandler(ElectionHandler elecHandler) {
		this.elecHandler = elecHandler;
	}

	private RoutingConf conf;
	private EdgeMonitor emon;
	private TaskList tasks;
	private boolean hasLeader=false;
	private int leaderId=0;
	private State state=State.Follower;
	private int currentTerm=0;
	private long timeout=0;
	private String leaderAddress="";
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

	public EdgeMonitor getEmon() {
		return emon;
	}

	public void setEmon(EdgeMonitor emon) {
		this.emon = emon;
	}

	public TaskList getTasks() {
		return tasks;
	}

	public void setTasks(TaskList tasks) {
		this.tasks = tasks;
	}

}
