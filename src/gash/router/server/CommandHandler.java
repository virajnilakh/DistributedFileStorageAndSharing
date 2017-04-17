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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.client.WriteChannel;
import gash.router.container.RoutingConf;
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

		}

		if (msg.getReqMsg().getRequestType() == Request.RequestType.WRITEFILE) {

			System.out.println("Message received :" + msg.getReqMsg().getRwb().getChunk().getChunkId());
			PrintUtil.printCommand(msg);
			lstMsg.add(msg);
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
				File file = new File("C:\\Gossamer\\" + msg.getReqMsg().getRwb().getFilename());
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