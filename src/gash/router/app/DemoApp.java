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
package gash.router.app;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import gash.router.client.WriteChannel;
import gash.router.server.CommandInit;
import gash.router.server.WorkInit;
import global.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import redis.clients.jedis.Jedis;
import routing.Pipe.CommandMessage;

public class DemoApp implements CommListener {
	private static MessageClient mc;
	public static Channel channel = null;
	private static Jedis jedisHandler1=new Jedis("169.254.214.175",6379);
	private static Jedis jedisHandler2=new Jedis("169.254.56.202",6379);
	private static Jedis jedisHandler3=new Jedis("169.254.80.87",6379);

	public static Jedis getJedisHandler1() {
		return jedisHandler1;
	}
	public static Jedis getJedisHandler2() {
		return jedisHandler2;
	}
	public static Jedis getJedisHandler3() {
		return jedisHandler3;
	}

	public DemoApp(MessageClient mc) {
		init(mc);
	}

	private void init(MessageClient mc) {
		this.mc = mc;
		this.mc.addListener(this);
	}

	private void ping(int N) {
		// test round-trip overhead (note overhead for initial connection)
		final int maxN = 10;
		long[] dt = new long[N];
		long st = System.currentTimeMillis(), ft = 0;
		for (int n = 0; n < N; n++) {
			mc.ping();
			ft = System.currentTimeMillis();
			dt[n] = ft - st;
			st = ft;
		}

		System.out.println("Round-trip ping times (msec)");
		for (int n = 0; n < N; n++)
			System.out.print(dt[n] + " ");
		System.out.println("");
	}

	@Override
	public String getListenerID() {
		return "demo";
	}

	@Override
	public void onMessage(CommandMessage msg) {
		System.out.println("---> " + msg);
	}

	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String host = "127.0.0.1";
		int port = 4568;
		Boolean affirm = true;
		Boolean mainAffirm = true;
		try {
			// MessageClient mc = new MessageClient(host, port);
			// DemoApp da = new DemoApp(mc);

			// do stuff w/ the connection
			// da.ping(2);

			// Logger.info("trying to connect to node " + );
			if(jedisHandler1.ping().equals("PONG")){
				host = jedisHandler1.get("1").split(":")[0];
				port = Integer.parseInt(jedisHandler1.get("1").split(":")[1]);
			}else if(jedisHandler2.ping().equals("PONG")){
				host = jedisHandler2.get("1").split(":")[0];
				port = Integer.parseInt(jedisHandler2.get("1").split(":")[1]);
			}else if(jedisHandler3.ping().equals("PONG")){
				host = jedisHandler3.get("1").split(":")[0];
				port = Integer.parseInt(jedisHandler3.get("1").split(":")[1]);
			}
			
			EventLoopGroup workerGroup = new NioEventLoopGroup();

			Bootstrap b = new Bootstrap(); // (1)
			b.group(workerGroup); // (2)
			b.channel(NioSocketChannel.class); // (3)
			b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
			b.handler(new CommandInit(null, false));

			// Start the client.
			System.out.println("Connect to a node.");

			ChannelFuture f = b.connect(host, port).sync(); // (5)
			channel = f.channel();

			Scanner reader = new Scanner(System.in);
			try {
				int option = 0;
				System.out.println("Welcome to Gossamer Distributed");
				System.out.println("Please choose an option to continue: ");
				System.out.println("1. Read a file");
				System.out.println("2. Write a file");
				System.out.println("0. Exit");
				option = reader.nextInt();
				// option = 2;
				// reader.nextLine();
				System.out.println("You entered " + option);
				System.out.println("Press Y to continue or any other key to cancel");

				String ans = "";
				// ans=reader.nextLine();
				// ans = "Y";
				if (ans.equals("Y")) {
					mainAffirm = false;
				}

				String path = "";
				switch (option) {
				case 1:
					break;
				case 2:
					// For testing writes
					// String path = runWriteTest();

					System.out.println("Please enter directory (path) to upload:");

					// path = reader.nextLine();
					// path = "C:\\Songs\\1.mp4";
					System.in.read();

					System.out.println("You entered" + path);

					System.out.println("Press Y to continue or any other key to cancel");
					affirm = false;
					String ans1 = "";
					ans1 = reader.next();
					// ans1 = "Y";
					if (ans1.equals("Y")) {
						affirm = true;
					}
					reader.close();
					File file = new File(path);
					long begin = System.currentTimeMillis();
					System.out.println("Begin time");
					System.out.println(begin);
					sendFile(file);
					System.out.println("File sent");
					System.out.flush();
					break;
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("Error occurred...");
				Thread.sleep(10 * 1000);
				e.printStackTrace();
			} finally {
				System.out.println("Exiting...");
				// Thread.sleep(10 * 1000);

			}

			// System.out.println("\n** exiting in 10 seconds. **");
			// System.out.flush();
			// Thread.sleep(10 * 1000);
		} catch (

		Exception e) {
			e.printStackTrace();
		} finally {
			// CommConnection.getInstance().release();
		}
	}

	private static String runWriteTest() {
		String path = "C:\\1\\Natrang.avi";
		return path;
	}

	private static void sendFile(File file) {
		// TODO Auto-generated method stub

		/*
		 * Task 1: Send Read Command Task 2: Receive Ack Task 3: Split into
		 * chunks Task 4: Send chunks Task 5: Recv Ack Task 6: Resend chunks not
		 * ack
		 */

		sendReadCommand(file);

	}

	private static void sendReadCommand(File file) {
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
			String hash = getHashFileName(name);
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
				CommandMessage commMsg = MessageClient.sendWriteRequest(chunksFile.get(i), hash, name, numChunks,
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

	private static String getHashFileName(String name) {
		// TODO Auto-generated method stub

		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		digest.reset();

		digest.update(name.getBytes());
		byte[] bs = digest.digest();

		BigInteger bigInt = new BigInteger(1, bs);
		String hashText = bigInt.toString(16);

		// Zero pad until 32 chars
		while (hashText.length() < 32) {
			hashText = "0" + hashText;
		}

		return hashText;
	}
}
