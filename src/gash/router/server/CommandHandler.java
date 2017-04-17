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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.client.MessageClient;
import gash.router.client.WriteChannel;
import gash.router.container.RoutingConf;
import global.Constants;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Chunk;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.common.Common.ReadResponse;
import pipe.common.Common.Request;
import pipe.common.Common.Response;
import pipe.common.Common.WriteBody;
import pipe.common.Common.WriteResponse;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.work.Work.WorkMessage;
import redis.clients.jedis.Jedis;
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

		if (msg == null) {
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		if (msg.getReqMsg().getRequestType() == Request.RequestType.READFILE) {

			if (msg.getReqMsg().getRrb().getFilename().equals("*")) {
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
				// CommandMessage msg2 = createAllFilesResponse(fileNames);
				String msg2 = createAllFilesResponse(fileNames);
				channel.writeAndFlush(msg2);
				System.out.println("Responses sent");
			}
		}

		if (msg.getReqMsg().getRequestType() == Request.RequestType.WRITEFILE) {

			WorkMessage wm = SendWriteWorkMessage(msg);
			System.out.println("File replicated");

			System.out.println("Message received :" + msg.getReqMsg().getRwb().getChunk().getChunkId());
			PrintUtil.printCommand(msg);

			lstMsg.add(msg);
			ServerState.getEmon().broadcast(wm);
			System.out.println("Message broadcasted");

			System.out.println("List size is: ");
			System.out.println(lstMsg.size());
			String storeStr = new String(msg.getReqMsg().getRwb().getChunk().getChunkData().toByteArray(), "ASCII");
			System.out.println("No. of chunks" + String.valueOf(msg.getReqMsg().getRwb().getNumOfChunks()));
			jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(), "name", msg.getReqMsg().getRwb().getFilename());
			// jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(),msg.getReqMsg().getRwb().getFileId());
			// jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(),msg.getReqMsg().getRwb().getFileExt());
			// Uncomment to store chunk data
			// jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(),String.valueOf(msg.getReqMsg().getRwb().getChunk().getChunkId()),storeStr);
			// jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(),msg.getReqMsg().getRwb().getChunk().getChunkData());
			jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(), "numChunks",
					String.valueOf(msg.getReqMsg().getRwb().getNumOfChunks()));

			Map<String, String> map = jedisHandler1.hgetAll(msg.getReqMsg().getRwb().getFileId());
			String temp = map.get("chunks");

			jedisHandler1.hset(msg.getReqMsg().getRwb().getFileId(), "chunks",
					temp + "," + String.valueOf(msg.getReqMsg().getRwb().getChunk().getChunkId()));

			CommandMessage commMsg = createAckWriteRequest(msg.getReqMsg().getRwb().getFileId(),
					msg.getReqMsg().getRwb().getFilename(), msg.getReqMsg().getRwb().getChunk().getChunkId());

			WriteChannel myCallable = new WriteChannel(commMsg, channel);

			futuresList.add(myCallable);

			if (lstMsg.size() == msg.getReqMsg().getRwb().getNumOfChunks()) {

				System.out.println("Checking if chunks exist");
				try {
					if (!jedisHandler1.exists(msg.getReqMsg().getRwb().getFileId())) {
						// return null;
						System.out.println("Does not exists");
					}
					System.out.println("Printing map");

					Map<String, String> map1 = jedisHandler1.hgetAll(msg.getReqMsg().getRwb().getFileId());

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
						return Integer.compare(msg1.getReqMsg().getRwb().getChunk().getChunkId(),
								msg2.getReqMsg().getRwb().getChunk().getChunkId());
					}
				});

				System.out.println("All chunks sorted");
				for (CommandMessage message : lstMsg) {
					chunkedFile.add(message.getReqMsg().getRwb().getChunk().getChunkData());
				}
				System.out.println("Chunked file created");

				File directory = new File(Constants.dataDir);
				if (!directory.exists()) {
					directory.mkdir();
					// If you require it to make the entire directory path
					// including parents,
					// use directory.mkdirs(); here instead.
				}

				File file = new File(Constants.dataDir + msg.getReqMsg().getRwb().getFilename());
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

				// Send acks
				List<Future<Long>> futures = service.invokeAll(futuresList);
				service.shutdown();

				// Cleanup
				chunkedFile = new ArrayList<ByteString>();
				lstMsg = new ArrayList<CommandMessage>();
				futuresList = new ArrayList<WriteChannel>();

			}
		}

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

		System.out.flush();
		
	}
	private static void sendReadCommand(File file,Channel channel) {
		// TODO Auto-generated method stub

		ArrayList<ByteString> chunksFile = new ArrayList<ByteString>();
		ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		List<WriteChannel> futuresList = new ArrayList<WriteChannel>();
		int sizeChunks = Constants.sizeOfChunk;
		int numChunks = 0;
		byte[] buffer = new byte[sizeChunks];

		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			String name = file.getName();
			String hash = getHashFileName(name);
			int tmp = 0;
			while ((tmp = bis.read(buffer)) > 0) {
				try {
					ByteString bs = ByteString.copyFrom(buffer, 0, tmp);
					chunksFile.add(bs);
					numChunks++;
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

			for (int i = 0; i < chunksFile.size(); i++) {
				CommandMessage commMsg = MessageClient.sendWriteRequest(chunksFile.get(i), hash, name, numChunks,
						i + 1);
				WriteChannel myCallable = new WriteChannel(commMsg, channel);
				futuresList.add(myCallable);
			}

			System.out.println("No. of chunks: " + futuresList.size());

			long start = System.currentTimeMillis();
			System.out.print(start);
			System.out.println("Start send");

			List<Future<Long>> futures = service.invokeAll(futuresList);
			System.out.println("Completed tasks");
			service.shutdown();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	private WorkMessage SendWriteWorkMessage(CommandMessage msg) {
		// TODO Auto-generated method stub

		Header.Builder header = Header.newBuilder();
		// ToDO: set correct nodeId
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		Chunk.Builder chunk = Chunk.newBuilder();
		chunk.setChunkId(msg.getReqMsg().getRwb().getChunk().getChunkId());
		chunk.setChunkData(msg.getReqMsg().getRwb().getChunk().getChunkData());

		WriteBody.Builder body = WriteBody.newBuilder();
		body.setFilename(msg.getReqMsg().getRwb().getFilename());
		// File Id is the MD5 hash in string format of the file name
		body.setFileId(msg.getReqMsg().getRwb().getFileId());
		body.setNumOfChunks(msg.getReqMsg().getRwb().getNumOfChunks());
		body.setChunk(chunk);

		Request.Builder req = Request.newBuilder();
		req.setRequestType(Request.RequestType.WRITEFILE);
		req.setRwb(body);

		LeaderStatus.Builder status = LeaderStatus.newBuilder();
		status.setState(LeaderState.LEADERALIVE);

		WorkMessage.Builder comm = WorkMessage.newBuilder();
		comm.setHeader(header);
		comm.setSecret(0);
		comm.setLeaderStatus(status);
		comm.setReq(req);
		return comm.build();

	}

	private String createAllFilesResponse(ArrayList<String> fileNames) {
		// TODO Auto-generated method stub
		Header.Builder header = Header.newBuilder();
		// ToDO: Set actual Node Id and hash as well
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);
		ReadResponse.Builder body = ReadResponse.newBuilder();
		// body.setChunkId(chunkId, chunkId);
		System.out.println("File names size:" + fileNames.size());
		String fileName = "";
		for (int i = 0; i < fileNames.size() - 1; i++) {
			System.out.println(fileNames.get(i));
			fileName += "," + fileNames.get(i);
		}
		// body.setFilename(0, fileName);
		Response.Builder res = Response.newBuilder();
		// res.setResponseType(Response.ResponseType.WRITEFILE);
		// req.setRwb(body);
		// res.setFilename(fileName);
		res.setReadResponse(body);
		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setResMsg(res);
		// return comm.build();
		return fileName;
	}

	public static CommandMessage createAckWriteRequest(String hash, String fileName, int chunkId) throws Exception {

		Header.Builder header = Header.newBuilder();
		// ToDO: Set actual Node Id and hash as well

		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		WriteResponse.Builder body = WriteResponse.newBuilder();
		// body.setChunkId(chunkId, chunkId);

		Response.Builder res = Response.newBuilder();

		// res.setResponseType(Response.ResponseType.WRITEFILE);
		// req.setRwb(body);
		res.setFilename(fileName);
		res.setWriteResponse(body);

		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setResMsg(res);
		return comm.build();
	}
	private static String getHashFileName(String name) {
		// TODO Auto-generated method stub

		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		digest.reset();

		digest.update(name.getBytes());
		byte[] bs = digest.digest();

		BigInteger bigInt = new BigInteger(1, bs);
		String hashText = bigInt.toString(16);

		// Zero pad until 32 chars
		while (hashText.length() < 32) {
			hashText = "0" + hashText;
		}

		return hashText;
	}

	/**
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