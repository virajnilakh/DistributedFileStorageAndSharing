package gash.router.server;

import java.util.ArrayList;

import pipe.common.Common.Chunk;
import pipe.common.Common.Header;
import pipe.common.Common.ReadResponse;
import pipe.common.Common.Request;
import pipe.common.Common.Response;
import pipe.common.Common.WriteBody;
import pipe.common.Common.WriteResponse;
import pipe.common.Common.Response.ResponseType;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class WorkMessageCreator {

	public static WorkMessage SendWriteWorkMessage(CommandMessage msg) {
		// TODO Auto-generated method stub

		Header.Builder header = Header.newBuilder();
		// ToDO: set correct nodeId
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		Chunk.Builder chunk = Chunk.newBuilder();
		chunk.setChunkId(msg.getReqMsg().getRwb().getChunk().getChunkId());
		chunk.setChunkData(msg.getReqMsg().getRwb().getChunk().getChunkData());

		WriteBody.Builder body = WriteBody.newBuilder();
		body.setFilename(msg.getReqMsg().getRwb().getFilename());
		// File Id is the MD5 hash in string format of the file name
		body.setFileId(msg.getReqMsg().getRwb().getFileId());
		body.setNumOfChunks(msg.getReqMsg().getRwb().getNumOfChunks());
		body.setChunk(chunk);

		Request.Builder req = Request.newBuilder();
		req.setRequestType(Request.RequestType.WRITEFILE);
		req.setRwb(body);

		LeaderStatus.Builder status = LeaderStatus.newBuilder();
		status.setState(LeaderState.LEADERALIVE);

		WorkMessage.Builder comm = WorkMessage.newBuilder();
		comm.setHeader(header);
		comm.setSecret(0);
		comm.setLeaderStatus(status);
		comm.setReq(req);
		return comm.build();

	}

	public static CommandMessage createAllFilesResponse(ArrayList<String> fileNames) {
		// TODO Auto-generated method stub
		Header.Builder header = Header.newBuilder();
		// ToDO: Set actual Node Id and hash as well
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);
		ReadResponse.Builder body = ReadResponse.newBuilder();
		// body.setChunkId(chunkId, chunkId);
		System.out.println("File names size:" + fileNames.size());
		int j = 0;
		String filenames = "";
		for (String name : fileNames) {
			System.out.println(++j + ") " + name);
			filenames += name;
			// fileName += "," + fileNames.get(i);

		}

		body.setFilename(filenames);
		// body.setResponseType(Response.ResponseType.READFILENAMES);

		Response.Builder res = Response.newBuilder();
		res.setResponseType(ResponseType.READFILENAMES);

		// req.setRwb(body);
		// res.setFilename(fileName);
		res.setReadResponse(body);
		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setResMsg(res);
		return comm.build();
		// return fileName;
	}

	public static CommandMessage createAckWriteRequest(String hash, String fileName, int chunkId) throws Exception {

		Header.Builder header = Header.newBuilder();
		// ToDO: Set actual Node Id and hash as well

		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		WriteResponse.Builder body = WriteResponse.newBuilder();
		// body.setChunkId(chunkId, chunkId);

		Response.Builder res = Response.newBuilder();

		// res.setResponseType(Response.ResponseType.WRITEFILE);
		// req.setRwb(body);
		res.setFilename(fileName);
		res.setWriteResponse(body);

		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setResMsg(res);
		return comm.build();
	}

}
