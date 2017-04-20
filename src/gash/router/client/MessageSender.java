package gash.router.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.protobuf.ByteString;

import global.Constants;
import global.Utility;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class MessageSender {

	public static void SendReadRequest(String fileName, Channel channel) {
		// TODO Auto-generated method stub
		CommandMessage msg = MessageCreator.createReadMessage(fileName);
		channel.writeAndFlush(msg);
		System.out.println("Read request sent");

	}

	public static void SendReadAllFileInfo(Channel channel) {
		// TODO Auto-generated method stub
		List<String> response = null;
		CommandMessage msg = MessageCreator.CreateReadAllMessage();
		channel.writeAndFlush(msg);
		System.out.println("Read all files request sent");
	}

	public static void sendReadCommand(File file, Channel channel) {
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

}