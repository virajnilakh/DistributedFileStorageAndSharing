package gash.router.server;

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
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class InboundWorkMessageQueueHandler implements Runnable {
	ServerState state=null;
	public InboundWorkMessageQueueHandler(ServerState s){
		state=s;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			WorkAndChannel wch = QueueHandler.dequeueInboundWorkAndChannel();
			WorkMessage msg = wch.getMsg();
			Channel channel = wch.getChannel();
			if (msg.getReq().getRrb().getFilename().equals("*")) {
				//readFileNamesCmd(msg, channel);
			} else {
				DBHandler mysql_db = new DBHandler();
				String fileId = Utility.getHashFileName(msg.getReq().getRrb().getFilename());

				if (mysql_db.checkFileExists(fileId)) {
					EventLoopGroup workerGroup = new NioEventLoopGroup();
					 
	 		        try {
	 		        	String host=msg.getReq().getClient().getHost();
	 					int port=msg.getReq().getClient().getPort();
	 		            Bootstrap b = new Bootstrap(); // (1)
	 		            b.group(workerGroup); // (2)
	 		            b.channel(NioSocketChannel.class); // (3)
	 		            b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
	 		            b.handler(new CommandInit(null,false));
	 
	 		            // Start the client.
	 		            if(!state.isLeader()){
		 		            System.out.println("===============Stole Read Request From Leader=============================");
		 		           System.out.println("===============Stole Read Request From Leader=============================");
		 		          System.out.println("===============Stole Read Request From Leader=============================");
		 		         System.out.println("===============Stole Read Request From Leader=============================");
		 		        System.out.println("===============Stole Read Request From Leader=============================");
		 		       System.out.println("===============Stole Read Request From Leader=============================");
		 		      System.out.println("===============Stole Read Request From Leader=============================");
		 		     System.out.println("===============Stole Read Request From Leader=============================");
		 		    System.out.println("===============Stole Read Request From Leader=============================");
		 		        System.out.println("===============Stole Read Request From Leader=============================");
	 		            }
	 
	 		             ChannelFuture cha = b.connect(host, port).sync();
	 		            readFileCmd(msg, cha.channel());
	 		        }catch(Exception e){
	 		        	System.out.println("Problem to connecting to client on steal node");
	 		        	e.printStackTrace();
	 		        }
				}else{
					EventLoopGroup workerGroup = new NioEventLoopGroup();
					 
	 		        try {
	 		        	String host=state.getLocalhostJedis().get(Constants.clusterId+"").split(":")[0];
	 					int port=Integer.parseInt(state.getLocalhostJedis().get(Constants.clusterId+"").split(":")[1]);
	 		            Bootstrap b = new Bootstrap(); // (1)
	 		            b.group(workerGroup); // (2)
	 		            b.channel(NioSocketChannel.class); // (3)
	 		            b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
	 		            b.handler(new CommandInit(null,false));
	 
	 		            // Start the client.
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 		            System.out.println("===============Stole request sent back to leader because no file exists=============================");
	 
	 		             ChannelFuture cha = b.connect(host, port).sync();
	 		            readFileCmd(msg, cha.channel());
	 		        }catch(Exception e){
	 		        	System.out.println("Problem to connecting to client on steal node");
	 		        }
				}
				
				
			}
			
		}
	}
	private void readFileCmd(WorkMessage msg, Channel channel) {
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
			QueueHandler.enqueueOutboundCommandAndChannel(commMsg, channel);
			//WriteChannel myCallable = new WriteChannel(commMsg, channel);
			//futuresList.add(myCallable);
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
