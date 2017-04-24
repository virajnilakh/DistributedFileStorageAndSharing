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
import global.Constants;
import io.netty.channel.Channel;
import pipe.election.Election.ElectionMessage;
import pipe.work.Work.WorkMessage;
import redis.clients.jedis.Jedis;

public class ServerState {

	public enum State {
		Follower, Leader, Candidate;
	}

	public static boolean roundTrip = false;
	private static boolean stealReq = false;
	private static int stealNode = 0;

	public static boolean isStealReq() {
		return stealReq;
	}

	public static void setStealReq(boolean s) {
		stealReq = s;
	}

	public static int getStealNode() {
		return stealNode;
	}

	public static void setStealNode(int stealNode) {
		ServerState.stealNode = stealNode;
	}

	public static boolean isRoundTrip() {
		return roundTrip;
	}

	public static void setRoundTrip(boolean roundTrip) {
		ServerState.roundTrip = roundTrip;
	}

	public static Channel next = null;
	private static Jedis nextJedis = null;

	public static Channel getNext() {
		return next;
	}

	public static void setNext(Channel next) {
		ServerState.next = next;
	}

	public static Jedis getNextJedis() {
		return nextJedis;
	}

	public static void setNextJedis(Jedis nextJedis) {
		ServerState.nextJedis = nextJedis;
	}

	private Handelable requestHandler;
	private ElectionHandler elecHandler = new ElectionHandler(this);

	public ElectionHandler getElecHandler() {
		return elecHandler;
	}

	public void setElecHandler(ElectionHandler elecHandler) {
		this.elecHandler = elecHandler;
	}

	private Jedis localhostJedis = new Jedis("localhost", 6379);

	public Jedis getLocalhostJedis() {
		return localhostJedis;
	}

	private Jedis jedisHandler1 = null;
	private Jedis jedisHandler2 = null;
	private Jedis jedisHandler3 = null;
	// private int nodeId=0;
	private static RoutingConf conf;
	private static EdgeMonitor emon;
	private TaskList tasks;
	private boolean hasLeader = false;
	private int leaderId = 0;
	private State state = State.Follower;
	private int currentTerm = 0;
	private long timeout = 0;
	private String leaderAddress = "";
	Handelable reqVote;
	Handelable resLeader;
	Handelable voteReceived;

	private String ipAddress = "";

	public ServerState() throws UnknownHostException {
		// ipAddress=LocalAddress.getLocalHostLANAddress().getHostAddress();
		ipAddress = LocalAddress.getLocalHostLANAddress().getHostAddress();
		// ipAddress = "10.250.175.205";

		System.out.println(LocalAddress.getLocalHostLANAddress().getHostAddress());
		reqVote = new HandleVoteRequestState(this);
		resLeader = new HandleLeaderResponseState(this);
		voteReceived = new HandleVoteReceivedState(this);
		nextJedis = new Jedis(Constants.next, Constants.redisPort);
		jedisHandler1 = new Jedis(Constants.jedis1, Constants.redisPort);
		jedisHandler2 = new Jedis(Constants.jedis2, Constants.redisPort);
		jedisHandler3 = new Jedis(Constants.jedis3, Constants.redisPort);

		// setRedis();
		/*
		 * try{ ipAddress=InetAddress.getLocalHost().getHostAddress();
		 * }catch(Exception e){ e.printStackTrace(); }
		 */
	}

	public void startAllThreads() {
		new Thread(new InboundCommandMessageQueueHandler()).start();
		new Thread(new InboundWorkMessageQueueHandler(this)).start();
		new Thread(new OutboundCommandMessageQueueHandler(this)).start();
		new Thread(new OutboundWorkMessageQueueHandler()).start();
		new Thread(new WorkStealHandler(this)).start();

	}

	public void setRedis() {
		// TODO Auto-generated method stub
		try {
			if (getJedisHandler1().ping().equals("PONG")) {
				getJedisHandler1().select(1);
				// getJedisHandler1().flushDB();

				// nodeId=getJedisHandler1().dbSize().intValue()+1;
				getJedisHandler1().set(this.getConf().getNodeId() + "", this.ipAddress + ":4567");
			}
		} catch (Exception e) {
			System.out.println("Connection to redis failed at 169.254.214.175:4567");
		}
		try {
			if (getJedisHandler2().ping().equals("PONG")) {

				getJedisHandler2().select(1);
				// getJedisHandler2().flushDB();
				getJedisHandler2().set(this.getConf().getNodeId() + "", this.ipAddress + ":4567");
			}
		} catch (Exception e) {
			System.out.println("Connection to redis failed at 169.254.56.202:4567");
		}
		try {
			if (getJedisHandler3().ping().equals("PONG")) {
				getJedisHandler3().select(1);
				// getJedisHandler3().flushDB();
				getJedisHandler3().set(this.getConf().getNodeId() + "", this.ipAddress + ":4567");
			}
		} catch (Exception e) {
			System.out.println("Connection to redis failed at 169.254.80.87:4567");
		}

	}

	public void delRedis(int id) {
		// TODO Auto-generated method stub
		try {
			if (getJedisHandler1().ping().equals("PONG")) {
				getJedisHandler1().select(1);
				getJedisHandler1().del(id + "");
			}
		} catch (Exception e) {
			System.out.println("Connection to redis failed at 169.254.214.175:4567");
		}
		try {
			if (getJedisHandler2().ping().equals("PONG")) {
				getJedisHandler2().select(1);
				getJedisHandler2().del(id + "");
			}
		} catch (Exception e) {
			System.out.println("Connection to redis failed at 169.254.56.202:4567");
		}
		try {
			if (getJedisHandler3().ping().equals("PONG")) {
				getJedisHandler3().select(1);
				getJedisHandler3().del(id + "");
			}
		} catch (Exception e) {
			System.out.println("Connection to redis failed at 169.254.80.87:4567");
		}

	}

	/*
	 * public int getNodeId() { return nodeId; } public void setNodeId(int
	 * nodeId) { this.nodeId = nodeId; }
	 */
	public Jedis getAnyJedis() {
		try {
			if (getJedisHandler1().ping().equals("PONG")) {
				getJedisHandler1().select(1);
				return getJedisHandler1();
			}
		} catch (Exception e) {
			System.out.println("Connection to redis failed at 169.254.214.175:4567");
		}
		try {
			if (getJedisHandler2().ping().equals("PONG")) {
				getJedisHandler2().select(1);
				return getJedisHandler2();
			}
		} catch (Exception e) {
			System.out.println("Connection to redis failed at 169.254.56.202:4567");
		}
		try {
			if (getJedisHandler3().ping().equals("PONG")) {
				getJedisHandler3().select(1);
				return getJedisHandler3();
			}
		} catch (Exception e) {
			System.out.println("Connection to redis failed at 169.254.80.87:4567");
		}
		return null;
	}

	public Jedis getJedisHandler2() {
		return jedisHandler2;
	}

	public Jedis getJedisHandler3() {
		return jedisHandler3;
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

	public void becomeLeader() {
		this.state = State.Leader;
	}

	public void becomeFollower() {
		this.state = State.Follower;
	}

	public void becomeCandidate() {
		this.state = State.Candidate;
	}

	public boolean isFollower() {
		if (this.state == State.Follower)
			return true;
		else
			return false;
	}

	public boolean isLeader() {
		if (this.state == State.Leader)
			return true;
		else
			return false;
	}

	public boolean isCandidate() {
		if (this.state == State.Candidate)
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

			if (electionMessage.getTerm() > getCurrentTerm()) {
				setCurrentTerm(electionMessage.getTerm());
				elecHandler.setVote2TermMap(electionMessage.getTerm(), false);
			}
			requestHandler = reqVote;
			requestHandler.handleMessage(channel, wm);
			break;
		case LEADERRESPONSE:
			System.out.println("Got response from leader:");

			requestHandler = resLeader;
			requestHandler.handleMessage(channel, wm);
			break;
		case VOTE:
			System.out.println("Got a vote:");

			requestHandler = voteReceived;
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

	public static RoutingConf getConf() {
		return conf;
	}

	public void setConf(RoutingConf conf) {
		this.conf = conf;
	}

	public static EdgeMonitor getEmon() {
		return emon;
	}

	public static void setEmon(EdgeMonitor em) {
		emon = em;
	}

	public TaskList getTasks() {
		return tasks;
	}

	public void setTasks(TaskList tasks) {
		this.tasks = tasks;
	}

}
