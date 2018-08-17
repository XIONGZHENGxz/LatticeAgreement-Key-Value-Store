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
	public boolean fail;
	public Random rand;
	
	public TcpListener(Server server, int port) {
		this.server = server;
		this.fail = false;
		this.rand = new Random();
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
				/*
				if(this.fail) {
					try {
					Thread.sleep(Util.fail + rand.nextInt());
					} catch (Exception e) {}
					this.fail = false;
					continue;
				}
				*/
				if(Util.DELAY && this.server.me == Util.delayReplica) {
					try {
						Thread.sleep(5);
					} catch (Exception e) {}
				}

				Thread t = new Thread(new TcpRequestHandler(this.server, packet));
				t.start();
			} catch (Exception e) {
				break;
			}
		}
	}
}


