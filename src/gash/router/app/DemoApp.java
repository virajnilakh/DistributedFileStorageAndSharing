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
import java.io.IOException;
import java.util.Scanner;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import gash.router.client.MessageSender;
import global.Constants;
import io.netty.channel.Channel;
import redis.clients.jedis.Jedis;
import routing.Pipe.CommandMessage;

public class DemoApp implements CommListener {
	private static MessageClient mc;
	public static Channel channel = null;

	static Jedis jedisHandler1 = new Jedis("192.168.1.40", Constants.redisPort);
	static Jedis jedisHandler2 = new Jedis("192.168.1.40", Constants.redisPort);
	static Jedis jedisHandler3 = new Jedis("192.168.1.40", Constants.redisPort);

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
				System.out.println("Please specify which id of cluster you want to connect to");
				System.out.println("If u don't know this information, type 0");
				int clusterId = reader.nextInt();

				if (clusterId != 0) {
					Constants.whomToConnect = clusterId;
				}

				System.out.println("Please choose an option to continue: ");

				System.out.println("[1] Write - Write a file to the cluster");
				System.out.println("[2] Read  - Read file from cluster");
				System.out.println("[3] Fetch Names - Fetch File Names stored in cluster");
				System.out.println("[4] Ping - Ping to cluster");
				System.out.println("[5] Read Multiple - Read Multiple files from cluster");
				System.out.println("[6] Write Multiple - Write Multiple files from cluster");
				System.out.println("[0] Exit  - Exit cluster");

				try {
					option = reader.nextInt();
				} catch (Exception e) {
					option = 0;
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("You entered " + option);
				CommConnection.getInstance().clearQueue();
				System.out.println("Queue size is " + CommConnection.getInstance().outboundWriteQueue.size());
				System.out.println("Press Y to continue or any other key to cancel");
				String ans = "";
				ans = reader.nextLine();
				if (ans.equals("Y")) {
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
					System.out.println("Please enter directory (path) to upload:");

					path = reader.nextLine();
					System.in.read();

					System.out.println("You entered" + path);

					System.out.println("Press Y to continue or any other key to cancel");
					String ans2 = "";
					ans2 = reader.next();
					ans2 = "Y";
					if (ans2.equals("Y")) {
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
				case 6:

					System.out.println("Please enter directories (paths) to upload seperated by a comma (,) :");

					path = reader.nextLine();
					System.in.read();

					System.out.println("You entered" + path);

					System.out.println("Press Y to continue");
					String[] paths = path.split(",");
					int j = 0;
					File file1 = null;
					String ans3 = "";
					while (j < paths.length) {
						ans3 = reader.next();
						ans3 = "Y";
						if (ans3.equals("Y")) {
						}
						file1 = new File(path);
						if (path.trim().equals("") || file1 == null) {
							break;
						}
						sendFile(file1, channel);
						System.out.println("File sent");
						System.out.flush();
					}

					break;
				case 2:
					System.out.println("Please enter name of file to fetch");
					String fileName = reader.nextLine();
					MessageSender.SendReadRequest(fileName);
					break;
				case 4:
					MessageSender.createCommandPing(Constants.clusterId);

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
			reader.close();
			System.out.println("Error occurred at client...");
			e.printStackTrace();
		} finally {
			System.out.println("Exiting...");

		}
	}

	private static void sendFile(File file, Channel channel) {
		try {
			MessageSender.sendReadCommand(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
