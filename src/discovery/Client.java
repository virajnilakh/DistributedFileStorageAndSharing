package discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Ashutosh Singh
 * Code created with the help of from Michiel De Mey's blog
 * http://michieldemey.be/blog/network-discovery-using-udp-broadcast/
 *
 */

public class Client {

	public static void main(String args[]) {
		// Find the server using UDP broadcast
		try {
			// Open a random port to send the package
			DatagramSocket c = new DatagramSocket();
			c.setBroadcast(true);

			byte[] sendData = "DISCOVER_FUIFSERVER_REQUEST".getBytes();

			// Try the 255.255.255.255 first
			/*
			try {
				DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length,
						InetAddress.getByName("255.255.255.255"), 8888);
				c.send(sendPacket);
				System.out.println(Client.class.getName() + ">>> Request packet sent to: 255.255.255.255 (DEFAULT)");
			} catch (Exception e) {
			}
			*/
			// Broadcast the message over all the network interfaces
			Enumeration interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()) {
				NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();
					
				if (networkInterface.isLoopback() || !networkInterface.isUp()|| networkInterface.getDisplayName().contains("VirtualBox")) {
					continue; // Don't want to broadcast to the loopback
								// interface
				}

				for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
					
					//System.out.println("Name-->"+networkInterface.getName());
					
					InetAddress broadcast = interfaceAddress.getBroadcast();
					//Hack: Manually excluding addresses starting with 10 and 192
					//ToDo: Make broadcast over LAN address less crude.
					if (broadcast == null || broadcast.getHostAddress().startsWith("10.") || broadcast.getHostAddress().startsWith("192.") ) {
						continue;
					}

					// Send the broadcast package!
					try {
						DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, broadcast, 8888);
						c.send(sendPacket);
					} catch (Exception e) {
					}

					System.out.println(Client.class.getName() + ">>> Request packet sent to: "
							+ broadcast.getHostAddress() + "; Interface: " + networkInterface.getDisplayName());
				}
			}

			System.out.println(
					Client.class.getName() + ">>> Done looping over all network interfaces. Now waiting for a reply!");

			// Wait for a response
			byte[] recvBuf = new byte[15000];
			DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
			c.receive(receivePacket);

			// We have a response
			System.out.println(Client.class.getName() + ">>> Broadcast response from server: "
					+ receivePacket.getAddress().getHostAddress());

			// Check if the message is correct
			String message = new String(receivePacket.getData()).trim();
			if (message.equals("DISCOVER_FUIFSERVER_RESPONSE")) {
				// DO SOMETHING WITH THE SERVER'S IP (for example, store it in
				// your controller)
				// Controller_Base.setServerIp(receivePacket.getAddress());
				System.out.println("Server IP Address found -->" + receivePacket.getAddress());
			}

			// Close the port!
			c.close();
		} catch (IOException ex) {
			Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
		}

	}
}
