package gash.router.server;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.protobuf.ByteString;

import database.Chunk;
import database.DBHandler;
import gash.router.client.MessageCreator;
import gash.router.client.WriteChannel;
import gash.router.server.edges.EdgeMonitor;
import global.Constants;
import global.Utility;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import pipe.common.Common.Header;
import pipe.common.Common.Node;
import pipe.common.Common.ReadBody;
import pipe.common.Common.Request;
import pipe.common.Common.TaskType;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.RequestType;
import routing.Pipe.CommandMessage;

public class InboundCommandMessageQueueHandler implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (true) {
			CommandAndChannel cch = QueueHandler.dequeueInboundCommandAndChannel();

			CommandMessage msg = cch.getMsg();
			Channel channel = cch.getChannel();
			if (ServerState.isStealReq() && msg.getRequest().getRequestType() == TaskType.REQUESTREADFILE) {
				// convert to command first
				WorkMessage wmsg = convertStealToWork(msg);
				EdgeMonitor.sendToNode(wmsg, ServerState.getStealNode());
				ServerState.setStealNode(0);
				ServerState.setStealReq(false);
			} else {
				if (msg.getRequest().getRrb().getFilename().equals("*")) {
					// readFileNamesCmd(msg, channel);
				} else {
					DBHandler mysql_db = new DBHandler();
					String fileId = Utility.getHashFileName(msg.getRequest().getRrb().getFilename());

					if (mysql_db.checkFileExists(fileId)) {
						readFileCmd(msg, channel);

					} else {
						ServerState.getNext().writeAndFlush(msg);
					}

					/*
					 * EventLoopGroup workerGroup = new NioEventLoopGroup();
					 * 
					 * try { String host=msg.getReq().getClient().getHost(); int
					 * port=msg.getReq().getClient().getPort(); Bootstrap b =
					 * new Bootstrap(); // (1) b.group(workerGroup); // (2)
					 * b.channel(NioSocketChannel.class); // (3)
					 * b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
					 * b.handler(new CommandInit(null,false));
					 * 
					 * // Start the client. //System.out.
					 * println("===============Stole Read Request From Leader============================="
					 * );
					 * 
					 * ChannelFuture cha = b.connect(host, port).sync();
					 * readFileCmd(msg, cha.channel()); }catch(Exception e){
					 * System.out.
					 * println("Leader unable to connect to client for proceesing read request"
					 * ); }
					 */

				}
			}

		}
	}

	private WorkMessage convertStealToWork(CommandMessage msg) {
		// TODO Auto-generated method stub

		Header.Builder header = Header.newBuilder();
		header.setNodeId(ServerState.getConf().getNodeId());
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		ReadBody.Builder body = ReadBody.newBuilder();
		// body.setClientAddress(msg.getRequest().getClient().getHost()+":"+msg.getRequest().getClient().getPort());
		body.setFilename(msg.getRequest().getRrb().getFilename());
		LeaderStatus.Builder leaderStatus = LeaderStatus.newBuilder();
		leaderStatus.setState(LeaderState.LEADERKNOWN);
		Node.Builder client=Node.newBuilder();
		client.setHost(msg.getRequest().getClient().getHost());
		client.setPort(msg.getRequest().getClient().getPort());
		Request.Builder req = Request.newBuilder();
		req.setRequestType(TaskType.REQUESTREADFILE);
		req.setRrb(body);
		req.setClient(client);

		WorkMessage.Builder wmsg = WorkMessage.newBuilder();
		wmsg.setHeader(header);
		// ToDO: Decide on a secret
		wmsg.setSecret(1);
		wmsg.setLeaderStatus(leaderStatus);
		wmsg.setRequestType(RequestType.READFILE);
		wmsg.setReq(req);

		return wmsg.build();
	}

	private void readFileCmd(CommandMessage msg, Channel channel) {
		// TODO Auto-generated method stub

		// Read a specific file
		String fileName = msg.getRequest().getRrb().getFilename();
		// long filesize = msg.getReqMsg().getRrb().getFil
		long filesize = 0; // TODO: update this
		String fileId = Utility.getHashFileName(fileName);
		// File file = new File(Constants.dataDir + fileName);

		ArrayList<ByteString> chunksFile = new ArrayList<ByteString>();
		ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		List<WriteChannel> futuresList = new ArrayList<WriteChannel>();
		double sizeChunks = Constants.sizeOfChunk;
		byte[] buffer = new byte[(int) sizeChunks];
		System.out.println("Reading From Leader!!");
		System.out.println("Reading From Leader!!");
		System.out.println("Reading From Leader!!");
		System.out.println("Reading From Leader!!");
		System.out.println("Reading From Leader!!");
		System.out.println("Reading From Leader!!");
		System.out.println("Reading From Leader!!");
		System.out.println("Reading From Leader!!");
		System.out.println("Reading From Leader!!");

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

}
