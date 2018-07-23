package la.common;
import java.io.*;
import java.net.DatagramSocket;
import java.net.Socket;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;
import java.util.PriorityQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class UdpListener extends Thread{
	public DatagramSocket serverSocket;
	public Server server;
	public boolean fail;
	public Random rand;
	public ExecutorService es; 

	public UdpListener(Server server, int port) {
		this.server = server;
		this.fail = false;
		this.es = Executors.newFixedThreadPool(50);
		this.rand = new Random();
		try {
			serverSocket = new DatagramSocket(port);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		while(true) {
			try {
				byte[] data = new byte[40480];
				DatagramPacket packet  = new DatagramPacket(data, data.length);
				serverSocket.receive(packet);
				/*
				if(this.fail) {
					try {
						Thread.sleep(Util.fail + rand.nextInt());
					} catch (Exception e) {}
					this.fail = false;
					continue;
				}
				*/
				Thread t = new Thread(new UdpRequestHandler(this.server, packet));
				t.start();
			} catch (Exception e) {
				//e.printStackTrace();
			}
		}
	}
}
