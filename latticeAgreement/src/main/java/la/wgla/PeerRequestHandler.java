package la.wgla;

import la.common.Op;
import la.common.Server;
import la.common.Response;
import la.common.OpSocketPair;
import la.common.Util;
import la.common.Messager;

public class PeerRequestHandler extends Thread {
	Server server;
	public int counter;

	public PeerRequestHandler (Server server) {
		this.server = server;
		this.counter = 0;
	}

	public void run() {
		while(true) {
			try {
			//	server.peerlock.lock();
				//if(server.peerRequests.size() == 0) {
				//	server.peercond.await();
				//}
				while(this.counter < Util.threadLimit && !server.peerRequests.isEmpty()) {
					this.counter ++;
					System.out.println("handling" + this.counter);
					Object req = server.peerRequests.poll();
					Thread t = new Thread(new PeerHandler(server, req));
					t.start();
				}

				try {
					Thread.sleep(Util.requestWaitTime);
				} catch (Exception e) {}
			} catch (Exception e) {
			} finally {
			//	this.server.peerlock.unlock();
			}
		}
	}
}

class PeerHandler implements Runnable {
	public Server server;
	public Object req;

	public PeerHandler (Server server, Object req) {
		this.server = server;
		this.req = req;
	}

	public void run() {
		server.handleRequest(req);
	}
}

