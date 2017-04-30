package replication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import gash.router.client.CommConnection;
import gash.router.client.MessageCreator;
import gash.router.client.WriteChannel;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class DataReplicationManager {

	public void replicateToNewNode() {
		Channel channel = null;
		replicate(channel);
	}

	// Hit Database and Replicate all files
	public void replicate(Channel channel) {

		/*
		 * Iterate over all chunks from db and enqueue
		 * 
		 * 
		 * 
		 * ExecutorService service =
		 * Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors
		 * ()); List<Replication> futuresList = new ArrayList<Replication>();
		 * 
		 * for (int i = 0; i < chunksFile.size(); i++) { WorkMessage msg =
		 * WorkMessageCreator.createWriteRequest(chunksFile.get(i), hash, name,
		 * numChunks, i + 1, filesize);
		 * 
		 * Replication repThread = new Replication(msg, channel); //Get Instance
		 * and enqueue work write
		 * CommConnection.getInstance().enqueueWrite(myCallable);
		 * 
		 * }
		 * 
		 * try { List<Future<Long>> futures =
		 * service.invokeAll(CommConnection.getInstance().outboundWriteQueue); }
		 * catch (NullPointerException e) { // TODO Auto-generated catch block
		 * // e.printStackTrace();
		 * 
		 * } System.out.println("Completed tasks"); service.shutdown();
		 */
	}
}
