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
	ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();
	ArrayList<CommandMessage> lstMsg = new ArrayList<CommandMessage>();
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
		
		lstMsg.add(msg);

		if (lstMsg.size() == msg.getReqMsg().getRwb().getNumOfChunks()) {
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

			File directory = new File(Constants.clientDir);
			if (!directory.exists()) {
				directory.mkdir();
				// If you require it to make the entire directory path
				// including parents,
				// use directory.mkdirs(); here instead.
			}

			File file = new File(Constants.clientDir + msg.getReqMsg().getRwb().getFilename());
			file.createNewFile();
			System.out.println("File created in ClientStuff dir");
			FileOutputStream outputStream = new FileOutputStream(file);
			ByteString bs = ByteString.copyFrom(chunkedFile);
			outputStream.write(bs.toByteArray());
			outputStream.flush();
			outputStream.close();

            				// Cleanup
			chunkedFile = new ArrayList<ByteString>();
			lstMsg = new ArrayList<CommandMessage>();
			//futuresList = new ArrayList<WriteChannel>();


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
