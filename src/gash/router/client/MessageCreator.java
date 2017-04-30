package gash.router.client;

import java.net.UnknownHostException;

import com.google.protobuf.ByteString;

import discovery.LocalAddress;
import global.Constants;
import pipe.common.Common.Chunk;
import pipe.common.Common.Header;
import pipe.common.Common.Node;
import pipe.common.Common.ReadBody;
import pipe.common.Common.Request;
import pipe.common.Common.TaskType;
import pipe.common.Common.WriteBody;
import routing.Pipe.CommandMessage;

public class MessageCreator {

	public static CommandMessage CreateReadAllMessage() {
		// TODO Auto-generated method stub
		Header.Builder header = Header.newBuilder();
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		ReadBody.Builder body = ReadBody.newBuilder();
		body.setFilename("*");

		Request.Builder req = Request.newBuilder();
		// req.setRequestType(TaskType.REQUESTREADALLFILEDETAILS);
		req.setRequestType(TaskType.REQUESTREADFILE);
		req.setRrb(body);

		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setRequest(req);
		return comm.build();

	}

	public static CommandMessage createReadMessage(String fileName) throws UnknownHostException {
		// TODO Auto-generated method stub
		Header.Builder header = Header.newBuilder();
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		ReadBody.Builder body = ReadBody.newBuilder();
		body.setFilename(fileName);
		
		//body.setClientAddress(LocalAddress.getLocalHostLANAddress().getHostAddress()+":"+Constants.clientPort);
		Request.Builder req = Request.newBuilder();
		// req.setRequestType(Request.RequestType.READFILE);
		req.setRequestType(TaskType.REQUESTREADFILE);
		req.setRrb(body);
		Node.Builder node=Node.newBuilder();
		node.setNodeId(0);
		node.setHost(LocalAddress.getLocalHostLANAddress().getHostAddress());
		node.setPort(Constants.clientPort);
		req.setClient(node);
		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setRequest(req);
		return comm.build();

	}

	public static CommandMessage createWriteRequest(ByteString bs, String hash, String fileName, int chunkCount,
			int chunkId, long filesize) throws Exception {

		Header.Builder header = Header.newBuilder();
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		Chunk.Builder chunk = Chunk.newBuilder();
		System.out.println("Chunk Id while creating:" + chunkId);
		System.out.println("Chunk Size:" + bs.size());
		chunk.setChunkId(chunkId);
		chunk.setChunkData(bs);
		chunk.setChunkSize(bs.size());

		WriteBody.Builder body = WriteBody.newBuilder();
		body.setFilename(fileName);
		// File Id is the MD5 hash in string format of the file name
		body.setFileId(hash);
		
		body.setNumOfChunks(chunkCount);
		body.setChunk(chunk);
		//body.setFileSize(filesize);
		
		Request.Builder req = Request.newBuilder();
		req.setRequestType(TaskType.REQUESTWRITEFILE);
		req.setRwb(body);

		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setRequest(req);
		return comm.build();
	}

}
