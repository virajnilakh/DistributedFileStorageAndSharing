package discovery;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * @author Ashutosh Singh 
 * Code created with the help of from Michiel De Mey's blog
 * http://michieldemey.be/blog/network-discovery-using-udp-broadcast/
 * 1. Open a socket on the server that listens to the UDP requests.(I’ve chosen 8888) 
 * 2. Make a loop that handles the UDP requests and responses 
 * 3. Inside the loop, check the received UPD packet to see if it’s valid 
 * 4. Still inside the loop, send a response to the IP and Port of the received packet
 * 
 */
public class Server implements Runnable {

	DatagramSocket socket;
	private volatile boolean exit = false;
	static ArrayList<String> addresses = new ArrayList<String>();

	@Override
	public void run() {
		try {

			// Keep a socket open to listen to all the UDP traffic that is
			// destined for this port
			socket = new DatagramSocket(8888, InetAddress.getByName("0.0.0.0"));
			socket.setBroadcast(true);

			while (!exit) {
				System.out.println(getClass().getName() + ">>>Ready to receive broadcast packets!");

				// Receive a packet
				byte[] recvBuf = new byte[15000];
				DatagramPacket packet = new DatagramPacket(recvBuf, recvBuf.length);
				socket.receive(packet);

				// Packet received
				System.out.println(getClass().getName() + ">>>Discovery packet received from: "
						+ packet.getAddress().getHostAddress());
				System.out.println(getClass().getName() + ">>>Packet received; data: " + new String(packet.getData()));

				addresses.add(packet.getAddress().getHostAddress());

				// See if the packet holds the right command (message)
				String message = new String(packet.getData()).trim();
				if (message.equals("DISCOVER_FUIFSERVER_REQUEST")) {
					byte[] sendData = "DISCOVER_FUIFSERVER_RESPONSE".getBytes();

					// Send a response
					DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, packet.getAddress(),
							packet.getPort());
					socket.send(sendPacket);
					addresses.add(sendPacket.getAddress().getHostAddress());
					System.out.println(
							getClass().getName() + ">>>Sent packet to: " + sendPacket.getAddress().getHostAddress());
				}
			}

		} catch (IOException ex) {
			Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws InterruptedException, IOException {
		try {

			Thread server = new Thread(Server.getInstance());
			server.start();
			System.out.println("stopping Server thread");

			TimeUnit.MINUTES.sleep(3);
			server.stop();
			System.out.println("Server >>>Stopped accepting incoming connections");
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt(); // very important
			System.out.println("Server >>>Stopped accepting incoming connections");
			writeToFile();

			// break;
		}
	}

	private static void writeToFile() {
		BufferedWriter out = null;
		
		try {
			JSONObject obj = new JSONObject();
			obj.put("nodeId", 1);
			obj.put("internalNode", "false");
			obj.put("heartBeatDt", 3000);
			obj.put("workPort", 4567);
			obj.put("commandPort", 4568);
			JSONArray arr = new JSONArray();
			for (String address : addresses) {

				JSONObject route = new JSONObject();

				route.put("id", address.split('.')[3]);
				route.put("host", address);
				route.put("port", 4567);

				arr.add(route);
			}
			obj.put("routing", arr);

			
			File file = new File("C:\netty-pipe3.tar\netty-pipe3\runtime\route-6.conf");
			FileWriter fstream = new FileWriter(file, true); 
																																// data.
			out = new BufferedWriter(fstream);
			out.write(obj.toJSONString());
			System.out.println("Connection details written to file");
			System.out.println("JSON Object "+ obj);

		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
		catch (Exception e) {

		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	public static Server getInstance() {
		return ServerThreadHolder.INSTANCE;
	}

	public void stop() {
		exit = true;
	}

	private static class ServerThreadHolder {
		private static final Server INSTANCE = new Server();
	}
}
