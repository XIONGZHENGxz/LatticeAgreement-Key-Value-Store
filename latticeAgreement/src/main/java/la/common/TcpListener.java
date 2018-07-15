package la.common;
import java.io.*;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.util.Random;

public class TcpListener extends Thread{
	public ServerSocket serverSocket;
	public Server server;
	
	public TcpListener(Server server, int port) {
		this.server = server;
		try {
			serverSocket = new ServerSocket(port);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		while(true) {
			try {
				Socket packet  = serverSocket.accept();
				Thread t = new Thread(new TcpRequestHandler(this.server, packet));
				t.start();
			} catch (Exception e) {
				break;
			}
		}
	}
}


