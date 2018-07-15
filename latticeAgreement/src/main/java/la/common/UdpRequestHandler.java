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
		Request req = new Request();
		try {
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
			req = (Request) ois.readObject();
		} catch (Exception e) {
			return;
		}

		int from = req.me;
		if(Util.DEBUG) System.out.println("get request: "+req.toString() + " from " + from);

		//simulate delay of message
		if(rand.nextDouble() < Util.fp)  {
			long start = Util.getCurrTime();
			long curr = Util.getCurrTime();
			while(curr - start < Util.loop) {
				curr = Util.getCurrTime();
			}
		}

		server.handleRequest(req);
	}
}
