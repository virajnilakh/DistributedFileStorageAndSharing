package gash.router.server;

import gash.router.server.edges.EdgeMonitor;
import pipe.common.Common.Header;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

public class WorkStealHandler implements Runnable{
	private ServerState state=null;
	public WorkStealHandler(ServerState s){
		state=s;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			try {
				if(!state.isLeader()){
					WorkMessage msg=createStealRequest();
					EdgeMonitor.sendToNode(msg,state.getLeaderId());
				}
				
				Thread.sleep(7000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private WorkMessage createStealRequest() {
		// TODO Auto-generated method stub
		WorkState.Builder sb = WorkState.newBuilder();
		sb.setEnqueued(-1);
		sb.setProcessed(-1);

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setSteal(true);
		hb.setTime(System.currentTimeMillis());
		LeaderStatus.Builder status = LeaderStatus.newBuilder();
		status.setLeaderId(state.getConf().getNodeId());
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(0);
		//wb.setBeat(bb);
		if (state.getLeaderId() != 0) {
			status.setState(LeaderState.LEADERKNOWN);
		} else {
			status.setState(LeaderState.LEADERUNKNOWN);
		}
		wb.setLeaderStatus(status);

		return wb.build();
		
	}

}
