package la.common;

import java.io.ObjectInputStream;
import java.io.ByteArrayInputStream;
import java.net.DatagramPacket;
import java.util.Random;

public class UdpRequestHandler implements Runnable {
	public Server server;
	public DatagramPacket s;
	public Random rand;

	public UdpRequestHandler(Server server, DatagramPacket s) {
		this.server = server;
		this.s = s;
		rand = new Random();
	}

	public void run() {
		byte[] data = s.getData();
		Object obj = null;
		try {
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
			obj = ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}

		//simulate delay of message
		if(rand.nextDouble() < Util.fp)  {
			long start = Util.getCurrTime();
			long curr = Util.getCurrTime();
			while(curr - start < Util.loop) {
				curr = Util.getCurrTime();
			}
		}

		if(obj != null)
		server.handleRequest(obj);
	}
}
