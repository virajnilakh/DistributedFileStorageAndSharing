package gash.router.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import database.Chunk;
import database.DBHandler;
import gash.router.client.MessageCreator;
import gash.router.client.WriteChannel;
import gash.router.container.RoutingConf;
import global.Constants;
import global.Utility;
import io.netty.channel.Channel;
import pipe.common.Common.Failure;
import pipe.common.Common.Request;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.Task;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import routing.Pipe.CommandMessage;

public class InboundWorkMessageQueueHandler implements Runnable {
	protected static Logger logger = LoggerFactory.getLogger("work");
	protected ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();
	protected ArrayList<WorkMessage> lstMsg = new ArrayList<WorkMessage>();
	private static Jedis jedisHandler1 = new Jedis("localhost", 6379);
	List<WriteChannel> futuresList = new ArrayList<WriteChannel>();
	ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	protected ServerState state;
	protected boolean debug = false;
	protected RoutingConf conf;

	public InboundWorkMessageQueueHandler(ServerState s) {
		state = s;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			WorkAndChannel wch = QueueHandler.dequeueInboundWorkAndChannel();
			WorkMessage msg = wch.getMsg();
			Channel channel = wch.getChannel();
			if (msg == null) {
				// TODO add logging
				System.out.println("ERROR: Unexpected content  - " + msg);
				return;
			}

			if (msg.getHeader().hasElection()) {
				handleElectionMsg(msg, channel);
			} else if (msg.getReq().getRequestType() == Request.RequestType.READFILE) {
				if (msg.getReq().getRrb().getFilename().equals("*")) {
					readFileNamesCmd(msg, channel);
				} else {
					readFileCmd(msg, channel);
				}
			}

			else if (msg.getReq().getRequestType() == Request.RequestType.WRITEFILE) {

				try {
					writeFileCmd(msg, channel);
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (debug)
				PrintUtil.printWork(msg);

			System.out.flush();
		}

	}

	/**
	 * @param msg
	 * @param channel
	 */
	private void handleElectionMsg(WorkMessage msg, Channel channel) {
		// TODO How can you implement this without if-else statements?
		try {
			if (msg.getHeader().hasElection()) {
				System.out.println("Processing the message:");
				state.handleMessage(channel, msg);
			} else if (msg.getLeaderStatus().getState() == LeaderState.LEADERALIVE) {
				System.out.println(
						"Heartbeat from leader " + msg.getLeaderStatus().getLeaderId() + "...Resetting the timmer:");
				state.getElecHandler().getTimer().cancel();
				state.getElecHandler().setTimer();
				try {
					state.getLocalhostJedis().select(0);
					state.getLocalhostJedis().set("2", msg.getLeaderStatus().getLeaderHost() + ":4568");
					System.out.println("---Redis updated---");

				} catch (Exception e) {
					System.out.println("---Problem with redis at voteReceived in WorkHandler---");
				}
				state.setLeaderId(msg.getLeaderStatus().getLeaderId());
				state.setLeaderAddress(msg.getLeaderStatus().getLeaderHost());
				state.becomeFollower();
				state.setTimeout(0);
			} else if (msg.hasBeat()) {
				Heartbeat hb = msg.getBeat();
				logger.info("heartbeat from " + msg.getHeader().getNodeId());
				Timer t = state.getEmon().getTimer(msg.getHeader().getNodeId());
				if (t != null) {
					t.cancel();
					t = null;
				}
				state.getEmon().setTimer(msg.getHeader().getNodeId());
			} else if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
				boolean p = msg.getPing();
				WorkMessage.Builder rb = WorkMessage.newBuilder();
				rb.setPing(true);
				// QueueHandler.enqueueInboundWorkAndChannel(rb.build(),
				// channel);
				channel.write(rb.build());
			} else if (msg.hasErr()) {
				Failure err = msg.getErr();
				logger.error("failure from " + msg.getHeader().getNodeId());
				// PrintUtil.printFailure(err);
			} else if (msg.hasTask()) {
				Task t = msg.getTask();
			} else if (msg.hasState()) {
				WorkState s = msg.getState();
			}
		} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}
	}

	private void readFileCmd(WorkMessage msg, Channel channel) {
		// TODO Auto-generated method stub

		// Read a specific file
		String fileName = msg.getReq().getRrb().getFilename();
		if (fileName == "") {
			System.out.println("Filename is empty");
			return;
		}
		// long filesize = msg.getReqMsg().getRrb().getFil
		long filesize = 0; // TODO: update this
		String fileId = Utility.getHashFileName(fileName);
		// File file = new File(Constants.dataDir + fileName);

		ArrayList<ByteString> chunksFile = new ArrayList<ByteString>();
		ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		List<WriteChannel> futuresList = new ArrayList<WriteChannel>();
		double sizeChunks = Constants.sizeOfChunk;
		byte[] buffer = new byte[(int) sizeChunks];

		long start = System.currentTimeMillis();
		// System.out.print(start);
		// System.out.println("Start send");

		// GET from Mysql DB
		DBHandler mysql_db = new DBHandler();
		ArrayList<Chunk> chunks = new ArrayList<Chunk>();
		chunks = mysql_db.getChunks(fileId);
		futuresList = new ArrayList<WriteChannel>();
		int numChunks = chunks.size();
		// System.out.println("After db");
		// System.out.println("No. of chunks: " + chunks.size());
		for (int i = 0; i < numChunks; i++) {
			CommandMessage commMsg = null;
			try {
				Chunk chunk = chunks.get(i);
				// System.out.println("i" + i);
				// System.out.println("ChunkSize after db:" +
				// ByteString.copyFrom(chunk.getChunkData()).size());
				// System.out.println("ChunkID after db:" + chunk.getChunkId());
				commMsg = MessageCreator.createWriteRequest(ByteString.copyFrom(chunk.getChunkData()), fileId, fileName,
						numChunks, chunk.getChunkId(), filesize);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			QueueHandler.enqueueOutboundCommandAndChannel(commMsg, channel);
			// WriteChannel myCallable = new WriteChannel(commMsg, channel);
			// futuresList.add(myCallable);
		}
		mysql_db.closeConn();
		try {
			List<Future<Long>> futures = service.invokeAll(futuresList);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Completed tasks");
		service.shutdown();

	}

	/**
	 * @param msg
	 * @param channel
	 */
	private void readFileNamesCmd(WorkMessage msg, Channel channel) {
		System.out.println("Send all files message received");
		ScanParams params = new ScanParams();
		params.match("*");
		// Use "0" to do a full iteration of the collection.
		ScanResult<String> scanResult = jedisHandler1.scan("0", params);
		List<String> keys = scanResult.getResult();
		// System.out.println("Key count " + keys.size());
		Iterator<String> it = keys.iterator();
		ArrayList<String> fileNames = new ArrayList<String>();
		while (it.hasNext()) {
			String s = it.next();
			if (s.length() > 6) {
				Map<String, String> map = jedisHandler1.hgetAll(s);
				String temp = map.get("name");
				fileNames.add(temp);
				// System.out.println(temp);
			}
		}
		// TODO:
		// WorkMessage msg2 =
		// WorkMessageCreator.createAllFilesResponse(fileNames);
		// String msg2 = createAllFilesResponse(fileNames);
		// WorkMessage msg2=new WorkMessage();
		// channel.writeAndFlush(msg2);
		System.out.println("Responses sent");

	}

	/**
	 * @param msg
	 * @param channel
	 * @throws UnsupportedEncodingException
	 * @throws Exception
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws InterruptedException
	 */
	private void writeFileCmd(WorkMessage msg, Channel channel)
			throws UnsupportedEncodingException, Exception, IOException, FileNotFoundException, InterruptedException {

		String file_id = msg.getReq().getRwb().getFileId();
		String file_name = msg.getReq().getRwb().getFilename();
		String file_ext = msg.getReq().getRwb().getFileExt();
		long file_size = msg.getReq().getRwb().getFileSize();
		int chunk_id = msg.getReq().getRwb().getChunk().getChunkId();
		int num_of_chunks = msg.getReq().getRwb().getNumOfChunks();
		byte[] chunk_data = msg.getReq().getRwb().getChunk().getChunkData().toByteArray();
		int chunk_size = msg.getReq().getRwb().getChunk().getChunkSize();

		// Pushing chunks to Mysql DB
		DBHandler mysql_db = new DBHandler();
		mysql_db.addChunk(file_id, chunk_id, chunk_data, chunk_size, num_of_chunks);
		mysql_db.closeConn();

		System.out.println("Message received :" + msg.getReq().getRwb().getChunk().getChunkId());
		// PrintUtil.printCommand(msg);

		lstMsg.add(msg);
		// ServerState.getEmon().broadcast(wm);
		// System.out.println("Message broadcasted");

		// System.out.println("List size is: ");
		// System.out.println(lstMsg.size());
		String storeStr = new String(msg.getReq().getRwb().getChunk().getChunkData().toByteArray(), "ASCII");
		// System.out.println("No. of chunks" +
		// String.valueOf(msg.getReq().getRwb().getNumOfChunks()));
		// storeRedisData(msg);

		CommandMessage commMsg = WorkMessageCreator.createAckWriteRequest(file_id, msg.getReq().getRwb().getFilename(),
				msg.getReq().getRwb().getChunk().getChunkId());

		WriteChannel myCallable = new WriteChannel(commMsg, channel);

		futuresList.add(myCallable);

		if (lstMsg.size() == msg.getReq().getRwb().getNumOfChunks()) {

			System.out.println("Checking if chunks exist");
			try {
				if (!jedisHandler1.exists(file_id)) {
					// return null;
					System.out.println("Does not exists");
				}
				System.out.println("Printing map");

				Map<String, String> map1 = jedisHandler1.hgetAll(file_id);

				System.out.println(new PrettyPrintingMap<String, String>(map1));

			} catch (JedisConnectionException exception) {
				// Do stuff
			} finally {
				// Clean up
			}

			System.out.println("All chunks received");
			// Sorting
			Collections.sort(lstMsg, new Comparator<WorkMessage>() {
				@Override
				public int compare(WorkMessage msg1, WorkMessage msg2) {
					return Integer.compare(msg1.getReq().getRwb().getChunk().getChunkId(),
							msg2.getReq().getRwb().getChunk().getChunkId());
				}
			});

			System.out.println("All chunks sorted");
			for (WorkMessage message : lstMsg) {
				chunkedFile.add(message.getReq().getRwb().getChunk().getChunkData());
			}
			System.out.println("Chunked file created");

			File directory = new File(Constants.dataDir);
			if (!directory.exists()) {
				directory.mkdir();
				// If you require it to make the entire directory path
				// including parents,
				// use directory.mkdirs(); here instead.
			}

			File file = new File(Constants.dataDir + msg.getReq().getRwb().getFilename());
			file.createNewFile();
			System.out.println("File created in Gossamer dir");
			FileOutputStream outputStream = new FileOutputStream(file);
			ByteString bs = ByteString.copyFrom(chunkedFile);
			outputStream.write(bs.toByteArray());
			outputStream.flush();
			outputStream.close();
			System.out.println("File created");
			long end = System.currentTimeMillis();
			System.out.println("End time");
			System.out.println(end);

			// Pushing file to Mysql DB
			DBHandler mysql_db2 = new DBHandler();
			mysql_db2.addFile(file_id, file_name, file_ext, num_of_chunks, file_size);
			mysql_db2.closeConn();

			// Send acks
			// List<Future<Long>> futures = service.invokeAll(futuresList);
			// service.shutdown();

			// Cleanup
			chunkedFile = new ArrayList<ByteString>();
			lstMsg = new ArrayList<WorkMessage>();
			futuresList = new ArrayList<WriteChannel>();

		}
	}

}
