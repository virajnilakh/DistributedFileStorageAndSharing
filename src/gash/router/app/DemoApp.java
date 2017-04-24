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

import java.io.File;
import java.util.HashMap;
import java.util.Scanner;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import gash.router.client.MessageSender;
import global.Constants;
import io.netty.channel.Channel;
import redis.clients.jedis.Jedis;
import routing.Pipe.CommandMessage;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import pipe.common.Common.Header;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import gash.router.server.CommandInit;
import gash.router.client.CommInit;
import java.util.HashMap;

public class DemoApp implements CommListener {
	private static MessageClient mc;
	public static Channel channel = null;

	static Jedis jedisHandler1 = new Jedis(Constants.jedis1, Constants.redisPort);
	static Jedis jedisHandler2 = new Jedis(Constants.jedis2, Constants.redisPort);
	static Jedis jedisHandler3 = new Jedis(Constants.jedis3, Constants.redisPort);

	public Jedis getJedisHandler1() {
		return jedisHandler1;
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
		String host = Constants.localhost;
		// String host = "169.254.214.175";
		int port = Constants.workPort;

		Boolean affirm = true;
		Boolean mainAffirm = true;
		Scanner reader = new Scanner(System.in);
		try {

			try {
				if (jedisHandler1.ping().equals("PONG")) {
					jedisHandler1.select(0);
					host = jedisHandler1.get(String.valueOf(Constants.clusterId)).split(":")[0];
					port = Integer.parseInt(jedisHandler1.get(String.valueOf(Constants.clusterId)).split(":")[1]);
				}
			} catch (Exception e) {
				System.out.println("Connection to redis failed at 169.254.214.175:4567");
			}
			try {
				if (jedisHandler2.ping().equals("PONG")) {
					jedisHandler2.select(0);
					host = jedisHandler2.get(String.valueOf(Constants.clusterId)).split(":")[0];
					port = Integer.parseInt(jedisHandler2.get(String.valueOf(Constants.clusterId)).split(":")[1]);
				}
			} catch (Exception e) {
				System.out.println("Connection to redis failed at 169.254.56.202:4567");
			}
			try {
				if (jedisHandler3.ping().equals("PONG")) {
					jedisHandler3.select(0);
					host = jedisHandler3.get(String.valueOf(Constants.clusterId)).split(":")[0];
					port = Integer.parseInt(jedisHandler3.get(String.valueOf(Constants.clusterId)).split(":")[1]);
				}
			} catch (Exception e) {
				System.out.println("Connection to redis failed at 169.254.80.87:4567");
			}
			new Thread(new ClientAsServer()).start();
			MessageClient msgClient = new MessageClient(host, port);
			DemoApp app = new DemoApp(msgClient);

			do {
				int option = 0;
				System.out.println("Welcome to Gossamer Distributed");
				System.out.println("Please choose an option to continue: ");

				System.out.println("[1] Write - Write a file to the cluster");
				System.out.println("[2] Read  - Read file from cluster");
				System.out.println("[3] Fetch Names - Fetch File Names stored in cluster");
				System.out.println("[4] Ping - Ping to cluster");
				System.out.println("[5] Read Multiple - Read Multiple files from cluster");
				System.out.println("[0] Exit  - Exit cluster");

				try {
					option = reader.nextInt();
				} catch (Exception e) {
					option = 0;
					// TODO Auto-generated catch block
					// e.printStackTrace();
				}
				// option = 2;
				// reader.nextLine();
				System.out.println("You entered " + option);
				CommConnection.getInstance().clearQueue();
				System.out.println("Queue size is " + CommConnection.getInstance().outboundWriteQueue.size());
				System.out.println("Press Y to continue or any other key to cancel");
				String ans = "";
				ans = reader.nextLine();
				// ans = "Y";
				if (ans.equals("Y")) {
					mainAffirm = false;
				}
				String path = "";
				switch (option) {
				case 3:

					System.out.println("Retreiving list of files stored...");
					MessageSender.SendReadAllFileInfo();
					System.out.println("Please enter option of which file to fetch");

					/*
					 * for (int i = 0; i < files.size(); i++) {
					 * System.out.print(i + ".");
					 * System.out.println(files.get(i)); }
					 * 
					 * int fileOption = reader.nextInt();
					 * 
					 * System.out.println("You entered " +
					 * files.get(fileOption));
					 * 
					 * System.out.
					 * println("Press Y to continue or any other key to cancel"
					 * ); affirm = false; String ans1 = ""; ans1 =
					 * reader.next(); ans1 = "Y"; if (ans1.equals("Y")) { affirm
					 * = true; }
					 */
					break;
				case 1:
					// For testing writes
					// String path = runWriteTest();

					System.out.println("Please enter directory (path) to upload:");

					path = reader.nextLine();
					// path = "C:\\Songs\\1.mp4";
					// path = "C:\\JS\\test\\test.js";
					// path = "C:\\Songs\\2.mp4";

					System.in.read();

					System.out.println("You entered" + path);

					System.out.println("Press Y to continue or any other key to cancel");
					affirm = false;
					String ans2 = "";
					ans2 = reader.next();
					ans2 = "Y";
					if (ans2.equals("Y")) {
						affirm = true;
					}
					// reader.close();
					File file = new File(path);
					if (path.trim().equals("") || file == null) {
						break;
					}
					long begin = System.currentTimeMillis();
					System.out.println("Begin time");
					System.out.println(begin);
					sendFile(file, channel);
					System.out.println("File sent");
					System.out.flush();
					break;
				case 2:
					System.out.println("Please enter name of file to fetch");
					String fileName = reader.nextLine();
					MessageSender.SendReadRequest(fileName);
					break;
				case 4:
					break;
				case 5:
					System.out.println("Please enter names of file to fetch seperated by comma");
					String fileNames = reader.nextLine();
					String[] fileArray = fileNames.split(",");
					for (String fileName1 : fileArray) {
						MessageSender.SendReadRequest(fileName1);
					}
					break;
				default:
					break;
				}
			} while (true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error occurred...");
			// Thread.sleep(10 * 1000);
			e.printStackTrace();
		} finally {
			System.out.println("Exiting...");
			// Thread.sleep(10 * 1000);

		}

		// System.out.println("\n** exiting in 10 seconds. **");
		// System.out.flush();
		// Thread.sleep(10 * 1000);
	}

	private static String runWriteTest() {
		String path = "C:\\1\\Natrang.avi";
		return path;
	}

	public static void clientAcceptConnections() {

	}

	private CommandMessage createCommandPing(int clusterId) {
		// TODO Auto-generated method stub
		CommandMessage.Builder command = CommandMessage.newBuilder();
		Boolean ping = true;
		command.setPing(ping);

		Header.Builder header = Header.newBuilder();
		header.setNodeId(2);
		header.setTime(System.currentTimeMillis());
		header.setDestination(Constants.whomToConnect);
		command.setHeader(header);

		return command.build();
	}
	
	private static void sendFile(File file, Channel channel) {
		// TODO Auto-generated method stub

		/*
		 * Task 1: Send Read Command Task 2: Receive Ack Task 3: Split into
		 * chunks Task 4: Send chunks Task 5: Recv Ack Task 6: Resend chunks not
		 * ack
		 */

		MessageSender.sendReadCommand(file);

	}

}
