package gash.router.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.google.protobuf.ByteString;

import global.Constants;
import routing.Pipe.CommandMessage;

public class MessageHandler {

	protected static ArrayList<CommandMessage> lstMsg = new ArrayList<CommandMessage>();
	protected static ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();

	/**
	 * @param msg
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void handleRead(CommandMessage msg) throws IOException, FileNotFoundException {
		System.out.println("Receiving chunks");
		lstMsg.add(msg);
		System.out.println("ChunkId Before sort:" + msg.getReqMsg().getRwb().getChunk().getChunkId());
		// System.out.println("List Message size is" + lstMsg.size());
		System.out.println("List msg size:" + lstMsg.size());
		System.out.println("No of chunks:" + msg.getReqMsg().getRwb().getNumOfChunks());
		if (lstMsg.size() == msg.getReqMsg().getRwb().getNumOfChunks()) {
			System.out.println("All chunks received");
			// Sorting
			Collections.sort(lstMsg, new Comparator<CommandMessage>() {
				@Override
				public int compare(CommandMessage o1, CommandMessage o2) {
					return o1.getReqMsg().getRwb().getChunk().getChunkId()
							- o2.getReqMsg().getRwb().getChunk().getChunkId();
				}
			});

			System.out.println("All chunks sorted");
			for (CommandMessage message : lstMsg) {
				System.out.println("ChunkId:" + message.getReqMsg().getRwb().getChunk().getChunkId());
				System.out.println("ChunkSize:" + message.getReqMsg().getRwb().getChunk().getChunkSize());
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
			System.out.println(chunkedFile.size());
			ByteString bs = ByteString.copyFrom(chunkedFile);

			outputStream.write(bs.toByteArray());
			outputStream.flush();
			outputStream.close();

			System.out.println("Cleaning up");
			// Cleanup
			chunkedFile = new ArrayList<ByteString>();
			lstMsg = new ArrayList<CommandMessage>();

			System.out.println("New chunkedFile size:" + chunkedFile.size());
			System.out.println("New lstMsg size:" + lstMsg.size());
			// futuresList = new ArrayList<WriteChannel>();

		}
	}
}
