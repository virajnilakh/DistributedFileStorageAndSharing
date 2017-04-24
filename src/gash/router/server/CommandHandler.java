/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import database.Chunk;
import database.DBHandler;
import database.DBManager;
import gash.router.client.MessageCreator;
import gash.router.client.WriteChannel;
import gash.router.container.RoutingConf;
import global.Constants;
import global.Utility;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.common.Common.Request;
import pipe.common.Common.TaskType;
import pipe.work.Work.WorkMessage;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import routing.Pipe.CommandMessage;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class CommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected RoutingConf conf;
	protected ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();
	protected ArrayList<CommandMessage> lstMsg = new ArrayList<CommandMessage>();
	private static Jedis jedisHandler1 = new Jedis("localhost", 6379);
	List<WriteChannel> futuresList = new ArrayList<WriteChannel>();
	ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	public CommandHandler(RoutingConf conf) {
		if (conf != null) {
			this.conf = conf;

		}
	}

	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 * 
	 * @param msg
	 * @throws Exception
	 */
	public void handleMessage(CommandMessage msg, Channel channel) throws Exception {

		System.out.println("Message recieved");

		if (msg == null) {
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		if (msg.getReq().getRequestType() == TaskType.REQUESTREADFILE) {
			QueueHandler.enqueueInboundCommandAndChannel(msg,channel);
		}else if (msg.getPing()) {
			// System.out.println("Received ping from cluster 1");
			if (!ServerState.isRoundTrip() && msg.getHeader().getDestination() == Constants.clusterId) {
				ServerState.setRoundTrip(true);
				CommandMessage ping = createCommandPing(msg.getHeader().getDestination());
				ServerState.getNext().writeAndFlush(ping);
			} else if (ServerState.isRoundTrip() && msg.getHeader().getDestination() == Constants.clusterId) {
				ServerState.setRoundTrip(false);
				System.out.println("***Got the ping back by going through the ring across all clusters***");
				channel.writeAndFlush(msg);
			} else {
				CommandMessage ping = createCommandPing(msg.getHeader().getDestination());
				ServerState.getNext().writeAndFlush(ping);
			}

		}

		if (msg.getReq().getRequestType() == TaskType.REQUESTREADFILE) {
			if (msg.getReq().getRrb().getFilename().equals("*")) {
				readFileNamesCmd(msg, channel);
			} else {
				readFileCmd(msg, channel);
			}
		}

		if (msg.getReq().getRequestType() == TaskType.REQUESTWRITEFILE) {

			writeFileCmd(msg, channel);
		}

		pingCmd(msg, channel);

		System.out.flush();

	}

	private void readFileCmd(CommandMessage msg, Channel channel) {
		// TODO Auto-generated method stub

		// Read a specific file
		String fileName = msg.getReq().getRrb().getFilename();
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
		System.out.print(start);
		System.out.println("Start send");

		// GET from Mysql DB
		DBHandler mysql_db = new DBHandler();
		ArrayList<Chunk> chunks = new ArrayList<Chunk>();
		chunks = mysql_db.getChunks(fileId);
		futuresList = new ArrayList<WriteChannel>();
		int numChunks = chunks.size();
		System.out.println("After db");
		System.out.println("No. of chunks: " + chunks.size());
		for (int i = 0; i < numChunks; i++) {
			CommandMessage commMsg = null;
			try {
				Chunk chunk = chunks.get(i);
				System.out.println("i" + i);
				System.out.println("ChunkSize after db:" + ByteString.copyFrom(chunk.getChunkData()).size());
				System.out.println("ChunkID after db:" + chunk.getChunkId());
				commMsg = MessageCreator.createWriteRequest(ByteString.copyFrom(chunk.getChunkData()), fileId, fileName,
						numChunks, chunk.getChunkId(), filesize);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			WriteChannel myCallable = new WriteChannel(commMsg, channel);
			futuresList.add(myCallable);
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

	private CommandMessage createCommandPing(int clusterId) {
		// TODO Auto-generated method stub
		CommandMessage.Builder command = CommandMessage.newBuilder();
		Boolean ping = true;
		command.setPing(ping);

		Header.Builder header = Header.newBuilder();
		header.setNodeId(2);
		header.setTime(System.currentTimeMillis());
		header.setDestination(clusterId);
		command.setHeader(header);

		return command.build();
	}

	/**
	 * @param msg
	 * @param channel
	 */
	private void readFileNamesCmd(CommandMessage msg, Channel channel) {
		System.out.println("Send all files message received");
		ScanParams params = new ScanParams();
		params.match("*");
		// Use "0" to do a full iteration of the collection.
		ScanResult<String> scanResult = jedisHandler1.scan("0", params);
		List<String> keys = scanResult.getResult();
		System.out.println("Key count " + keys.size());
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
		CommandMessage msg2 = WorkMessageCreator.createAllFilesResponse(fileNames);
		// String msg2 = createAllFilesResponse(fileNames);
		channel.writeAndFlush(msg2);
		System.out.println("Responses sent");

	}

	/**
	 * @param msg
	 * @param channel
	 */
	private void pingCmd(CommandMessage msg, Channel channel) {
		try {
			if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
			} else if (msg.hasMessage()) {
				logger.info(msg.getMessage());
			} else {
			}

		} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(conf.getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}
	}

	private long getFileSize(CommandMessage msg) {
		try {
			return msg.getReq().getRwb().getFileSize();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
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
	private void writeFileCmd(CommandMessage msg, Channel channel)
			throws UnsupportedEncodingException, Exception, IOException, FileNotFoundException, InterruptedException {

		String file_id = msg.getReq().getRwb().getFileId();
		String file_name = msg.getReq().getRwb().getFilename();
		String file_ext = msg.getReq().getRwb().getFileExt();
		long file_size = getFileSize(msg);

		int chunk_id = msg.getReq().getRwb().getChunk().getChunkId();
		int num_of_chunks = msg.getReq().getRwb().getNumOfChunks();
		byte[] chunk_data = msg.getReq().getRwb().getChunk().getChunkData().toByteArray();
		int chunk_size = msg.getReq().getRwb().getChunk().getChunkSize();

		// Pushing chunks to Mysql DB
		DBHandler mysql_db = new DBHandler();
		mysql_db.addChunk(file_id, chunk_id, chunk_data, chunk_size, num_of_chunks);
		mysql_db.closeConn();

		WorkMessage wm = WorkMessageCreator.SendWriteWorkMessage(msg);
		System.out.println("File replicated");

		System.out.println("Message received :" + msg.getReq().getRwb().getChunk().getChunkId());
		PrintUtil.printCommand(msg);

		lstMsg.add(msg);
		ServerState.getEmon().broadcast(wm);
		System.out.println("Message broadcasted");

		System.out.println("List size is: ");
		System.out.println(lstMsg.size());
		String storeStr = new String(msg.getReq().getRwb().getChunk().getChunkData().toByteArray(), "ASCII");
		System.out.println("No. of chunks" + String.valueOf(msg.getReq().getRwb().getNumOfChunks()));
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
			Collections.sort(lstMsg, new Comparator<CommandMessage>() {
				@Override
				public int compare(CommandMessage msg1, CommandMessage msg2) {
					return Integer.compare(msg1.getReq().getRwb().getChunk().getChunkId(),
							msg2.getReq().getRwb().getChunk().getChunkId());
				}
			});

			System.out.println("All chunks sorted");
			for (CommandMessage message : lstMsg) {
				chunkedFile.add(message.getReq().getRwb().getChunk().getChunkData());
			}
			System.out.println("Chunked file created");

			File directory = new File(Constants.dataDir);
			if (!directory.exists()) {
				directory.mkdir();
				File logFile = new File(Constants.logFile);
				logFile.createNewFile(); // Create log File

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
			lstMsg = new ArrayList<CommandMessage>();
			futuresList = new ArrayList<WriteChannel>();

		}
	}

	/**
	 * @param msg
	 */
	private void storeRedisData(CommandMessage msg) {
		jedisHandler1.hset(msg.getReq().getRwb().getFileId(), "name", msg.getReq().getRwb().getFilename());
		// jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(),msg.getReqMsg().getRwb().getFileId());
		// jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(),msg.getReqMsg().getRwb().getFileExt());
		// Uncomment to store chunk data
		// jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(),String.valueOf(msg.getReqMsg().getRwb().getChunk().getChunkId()),storeStr);
		// jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(),msg.getReqMsg().getRwb().getChunk().getChunkData());
		jedisHandler1.hset(msg.getReq().getRwb().getFileId(), "numChunks",
				String.valueOf(msg.getReq().getRwb().getNumOfChunks()));

		Map<String, String> map = jedisHandler1.hgetAll(msg.getReq().getRwb().getFileId());
		String temp = map.get("chunks");

		if (temp != null) {
			jedisHandler1.hset(msg.getReq().getRwb().getFileId(), "chunks",
					temp + "," + String.valueOf(msg.getReq().getRwb().getChunk().getChunkId()));
		} else {
			jedisHandler1.hset(msg.getReq().getRwb().getFileId(), "chunks",
					String.valueOf(msg.getReq().getRwb().getChunk().getChunkId()));
		}
	}

	/**
	 * 
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}