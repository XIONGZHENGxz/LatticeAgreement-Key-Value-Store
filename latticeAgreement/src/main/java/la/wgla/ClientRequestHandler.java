package la.wgla;

import la.common.Op;
import la.common.Server;
import la.common.Response;
import la.common.OpSocketPair;
import la.common.Util;
import la.common.Messager;

public class ClientRequestHandler extends Thread {
	Server server;
	public int counter;

	public ClientRequestHandler (Server server) {
		this.server = server;
		this.counter = 0;
	}

	public void run() {
		while(true) {
			try {
				this.server.reqlock.lock();
				if(server.requests.size() == 0) {
					server.reqcond.await();
				}
				while(this.counter < Util.threadLimit && !server.requests.isEmpty()) {
					this.counter ++;
					OpSocketPair osp = (OpSocketPair) server.requests.poll();
					Thread t = new Thread(new Handler(server, osp));
					t.start();
				}

				try {
					Thread.sleep(Util.requestWaitTime);
				} catch (Exception e) {}
			} catch (Exception e) {
			} finally {
				this.server.reqlock.unlock();
			}
		}
	}
}

class Handler implements Runnable {
	public Server server;
	public OpSocketPair osp;

	public Handler (Server server, OpSocketPair osp) {
		this.server = server;
		this.osp = osp;
	}

	public void run() {
		Response resp = server.handleRequest(osp.op);
		if(resp != null) Messager.sendMsg(resp, osp.socket);
	}
}

