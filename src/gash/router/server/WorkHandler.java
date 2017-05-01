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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import database.DBHandler;
import gash.router.client.WriteChannel;
import gash.router.container.RoutingConf;
import global.Constants;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Failure;
import pipe.common.Common.Request;
import pipe.common.Common.TaskType;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.Task;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 *
 * TODO replace println with logging!
 *
 * @author gash
 *
 */
public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {
	protected static Logger logger = LoggerFactory.getLogger("work");
	protected ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();
	protected ArrayList<WorkMessage> lstMsg = new ArrayList<WorkMessage>();
	private static Jedis jedisHandler1 = new Jedis("localhost", 6379);
	List<WriteChannel> futuresList = new ArrayList<WriteChannel>();
	ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	protected ServerState state;
	protected boolean debug = false;
	protected RoutingConf conf;

	public WorkHandler(ServerState state) {
		if (state != null) {
			this.state = state;
		}
	}

	public WorkHandler(RoutingConf conf) {
		if (conf != null) {
			this.conf = conf;
		}
	}

	private long getFileSize(WorkMessage msg) {
		try {
			return msg.getReq().getRwb().getFileSize();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * override this method to provide processing behavior. T
	 *
	 * @param msg
	 * @throws IOException
	 */
	public void handleMessage(WorkMessage msg, Channel channel) throws IOException {
		try{
			if (msg == null) {
				// TODO add logging
				System.out.println("ERROR: Unexpected content  - " + msg);
				return;
			}
			if (!state.isStealReq() && !msg.hasBeat() && msg.getHeader().getSteal()) {
				state.setStealNode(msg.getHeader().getNodeId());
				state.setStealReq(true);
			}
			if (!msg.getHeader().getSteal() && !msg.hasBeat() && msg.getReq().getRequestType() == TaskType.REQUESTREADFILE) {
				QueueHandler.enqueueInboundWorkAndChannel(msg, channel);
			} else if (!msg.getHeader().getSteal() && !msg.hasBeat() && msg.getReq().getRequestType() == TaskType.REQUESTWRITEFILE) {

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

				System.out.println("Message received :" + msg.getReq().getRwb().getChunk().getChunkId());

				lstMsg.add(msg);

				System.out.println("List size is: ");
				System.out.println(lstMsg.size());
				String storeStr = new String(msg.getReq().getRwb().getChunk().getChunkData().toByteArray(), "ASCII");
				System.out.println("No. of chunks" + String.valueOf(msg.getReq().getRwb().getNumOfChunks()));
				// storeRedisData(msg);

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
					lstMsg = new ArrayList<WorkMessage>();
					futuresList = new ArrayList<WriteChannel>();

				}
			}

			if (debug)
				PrintUtil.printWork(msg);

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
						state.getLocalhostJedis().set(Constants.clusterId + "",
								msg.getLeaderStatus().getLeaderHost() + ":4568");
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

			System.out.flush();
		}catch(Exception e){
			e.printStackTrace();
		}
		

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
	protected void channelRead0(ChannelHandlerContext ctx, WorkMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}
