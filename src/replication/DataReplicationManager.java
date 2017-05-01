package replication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.protobuf.ByteString;

import database.Chunk;
import database.DBHandler;
import gash.router.client.CommConnection;
import gash.router.client.MessageCreator;
import gash.router.client.WriteChannel;
import gash.router.server.QueueHandler;
import gash.router.server.WorkMessageCreator;
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

		DBHandler mysql_db = new DBHandler();
		ArrayList<Chunk> chunks = new ArrayList<Chunk>();
		chunks = mysql_db.getChunks();
		int numChunks = chunks.size();
		for (int i = 0; i < numChunks; i++) {
			Chunk chunk = chunks.get(i);
			String fileName = chunk.getFileName(chunk.getChunkFileId(), mysql_db);
			WorkMessage msg = WorkMessageCreator.createWriteRequest(ByteString.copyFrom(chunk.getChunkData()),
					chunk.getChunkFileId(), fileName, numChunks, chunk.getChunkId(), chunk.getChunkSize());

			QueueHandler.enqueueOutboundWorkAndChannel(msg, channel);
		}

	}
}
