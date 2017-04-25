package gash.router.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.protobuf.ByteString;

import global.Constants;
import global.Utility;
import io.netty.channel.Channel;
import pipe.common.Common.Header;
import routing.Pipe.CommandMessage;

public class MessageSender {
	static Channel channel = CommConnection.getInstance().connect();

	public static void SendReadRequest(String fileName) throws UnknownHostException {
		// TODO Auto-generated method stub
		CommandMessage msg = MessageCreator.createReadMessage(fileName);
		channel.writeAndFlush(msg);
		System.out.println("Read request sent");

	}
	public static void createCommandPing(int clusterId) {
		// TODO Auto-generated method stub
		CommandMessage.Builder command = CommandMessage.newBuilder();
		Boolean ping = true;
		command.setPing(ping);

		Header.Builder header = Header.newBuilder();
		header.setNodeId(2);
		header.setTime(System.currentTimeMillis());
		header.setDestination(clusterId);
		command.setHeader(header);

		 channel.writeAndFlush(command.build());
	}
	public static void SendReadAllFileInfo() {
		// TODO Auto-generated method stub
		// Channel channel = CommConnection.getInstance().connect();
		List<String> response = null;
		CommandMessage msg = MessageCreator.CreateReadAllMessage();
		channel.writeAndFlush(msg);
		System.out.println("Read all files request sent");
	}

	public static void sendReadCommand(File file) {
		// TODO Auto-generated method stub
		// Channel channel = CommConnection.getInstance().connect();
		ArrayList<ByteString> chunksFile = new ArrayList<ByteString>();
		ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		List<WriteChannel> futuresList = new ArrayList<WriteChannel>();
		double sizeChunks = Constants.sizeOfChunk;
		int numChunks = 0;
		byte[] buffer = new byte[(int) sizeChunks];

		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			String name = file.getName();
			long filesize = file.length();
			String hash = Utility.getHashFileName(name);
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
				CommandMessage commMsg = MessageCreator.createWriteRequest(chunksFile.get(i), hash, name, numChunks,
						i + 1, filesize);

				WriteChannel myCallable = new WriteChannel(commMsg, channel);
				CommConnection.getInstance().enqueueWrite(myCallable);
				// futuresList.add(myCallable);
			}

			// System.out.println("No. of chunks: " + futuresList.size());

			long start = System.currentTimeMillis();
			System.out.print(start);
			System.out.println("Start send");

			try {
				List<Future<Long>> futures = service.invokeAll(CommConnection.getInstance().outboundWriteQueue);
			} catch (NullPointerException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();

			}
			System.out.println("Completed tasks");
			service.shutdown();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

}
