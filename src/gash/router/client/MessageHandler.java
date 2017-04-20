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

	/**
	 * @param msg
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void handleRead(CommandMessage msg) throws IOException, FileNotFoundException {
		ArrayList<CommandMessage> lstMsg = new ArrayList<CommandMessage>();
		ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();

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
			// futuresList = new ArrayList<WriteChannel>();

		}
	}
}
