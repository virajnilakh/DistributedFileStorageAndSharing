/**
 * 
 */
package gash.router.server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Server implements Runnable {

	DatagramSocket socket;
	private volatile boolean exit = false;

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

				// See if the packet holds the right command (message)
				String message = new String(packet.getData()).trim();
				if (message.equals("DISCOVER_FUIFSERVER_REQUEST")) {
					byte[] sendData = "DISCOVER_FUIFSERVER_RESPONSE".getBytes();

					// Send a response
					DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, packet.getAddress(),
							packet.getPort());
					socket.send(sendPacket);

					System.out.println(
							getClass().getName() + ">>>Sent packet to: " + sendPacket.getAddress().getHostAddress());
				}
			}

		} catch (IOException ex) {
			Logger.getLogger(Server.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws InterruptedException {
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
			// break;
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
