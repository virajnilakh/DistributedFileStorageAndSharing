/*
 * copyright 2016, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.client;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.server.PrettyPrintingMap;
import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import global.Constants;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import routing.Pipe.CommandMessage;

/**
 * A client-side netty pipeline send/receive.
 * 
 * Note a management client is (and should only be) a trusted, private client.
 * This is not intended for public use.
 * 
 * @author gash
 * 
 */
public class CommHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("connect");
	protected ConcurrentMap<String, CommListener> listeners = new ConcurrentHashMap<String, CommListener>();
	//private volatile Channel channel;

	public CommHandler() {
	}

	/**
	 * Notification registration. Classes/Applications receiving information
	 * will register their interest in receiving content.
	 * 
	 * Note: Notification is serial, FIFO like. If multiple listeners are
	 * present, the data (message) is passed to the listener as a mutable
	 * object.
	 * 
	 * @param listener
	 */
	public void addListener(CommListener listener) {
		if (listener == null)
			return;

		listeners.putIfAbsent(listener.getListenerID(), listener);
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

		Logger logger = LoggerFactory.getLogger("work");
		ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();
		ArrayList<CommandMessage> lstMsg = new ArrayList<CommandMessage>();
		Jedis jedisHandler1 = new Jedis("localhost", 6379);
		List<WriteChannel> futuresList = new ArrayList<WriteChannel>();
		System.out.println("Message received :" + msg.getReqMsg().getRwb().getChunk().getChunkId());
		PrintUtil.printCommand(msg);

		lstMsg.add(msg);
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
			
			// Cleanup
			chunkedFile = new ArrayList<ByteString>();
			lstMsg = new ArrayList<CommandMessage>();
			futuresList = new ArrayList<WriteChannel>();

		}
		System.out.println("--> got incoming message");
		for (String id : listeners.keySet()) {
			CommListener cl = listeners.get(id);
			
			// TODO this may need to be delegated to a thread pool to allow
			// async processing of replies
			cl.onMessage(msg);
		}
	}
	
	protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
		System.out.println("--> got incoming message");
		for (String id : listeners.keySet()) {
			CommListener cl = listeners.get(id);
			
			// TODO this may need to be delegated to a thread pool to allow
			// async processing of replies
			cl.onMessage(msg);
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
		System.out.println("--> user event: " + evt.toString());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from channel.", cause);
		ctx.close();
	}

}
