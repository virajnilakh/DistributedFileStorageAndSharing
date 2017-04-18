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
package gash.router.client;

import java.util.List;

import com.google.protobuf.ByteString;

import pipe.common.Common.Chunk;
import pipe.common.Common.Header;
import pipe.common.Common.ReadBody;
import pipe.common.Common.Request;
import pipe.common.Common.WriteBody;
import routing.Pipe.CommandMessage;

/**
 * front-end (proxy) to our service - functional-based
 * 
 * @author gash
 * 
 */
public class MessageClient {
	// track requests
	private long curID = 0;

	public MessageClient(String host, int port) {
		init(host, port);
	}

	private void init(String host, int port) {
		CommConnection.initConnection(host, port);
	}

	public void addListener(CommListener listener) {
		CommConnection.getInstance().addListener(listener);
	}

	public void ping() {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setPing(true);

		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static CommandMessage CreateReadAllMessage() {
		// TODO Auto-generated method stub
		Header.Builder header = Header.newBuilder();
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		ReadBody.Builder body = ReadBody.newBuilder();
		body.setFilename("*");

		Request.Builder req = Request.newBuilder();
		req.setRequestType(Request.RequestType.READFILE);
		req.setRrb(body);

		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setReqMsg(req);
		return comm.build();

	}
	public static CommandMessage createReadMessage(String fileName) {
		// TODO Auto-generated method stub
		Header.Builder header = Header.newBuilder();
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		ReadBody.Builder body = ReadBody.newBuilder();
		body.setFilename(fileName);

		Request.Builder req = Request.newBuilder();
		req.setRequestType(Request.RequestType.READFILE);
		req.setRrb(body);

		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setReqMsg(req);
		return comm.build();

	}
	public static CommandMessage sendWriteRequest(ByteString bs, String hash, String fileName, int chunkCount,
			int chunkId) throws Exception {

		Header.Builder header = Header.newBuilder();
		header.setNodeId(99);
		header.setTime(System.currentTimeMillis());
		header.setDestination(-1);

		Chunk.Builder chunk = Chunk.newBuilder();
		chunk.setChunkId(chunkId);
		chunk.setChunkData(bs);
		// chunk.setChunkSize(value);

		WriteBody.Builder body = WriteBody.newBuilder();
		body.setFilename(fileName);
		// File Id is the MD5 hash in string format of the file name
		body.setFileId(hash);
		body.setNumOfChunks(chunkCount);
		body.setChunk(chunk);

		Request.Builder req = Request.newBuilder();
		req.setRequestType(Request.RequestType.WRITEFILE);
		req.setRwb(body);

		CommandMessage.Builder comm = CommandMessage.newBuilder();
		comm.setHeader(header);
		comm.setReqMsg(req);
		return comm.build();
	}

	public void release() {
		CommConnection.getInstance().release();
	}

	/**
	 * Since the service/server is asychronous we need a unique ID to associate
	 * our requests with the server's reply
	 * 
	 * @return
	 */
	private synchronized long nextId() {
		return ++curID;
	}
}
