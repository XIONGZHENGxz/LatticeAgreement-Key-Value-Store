package la.wgla;

import la.common.Messager;
import la.common.Response;
import la.common.Op;

public class WriteResponder extends Thread {
	public GlaServer server;

	public WriteResponder (GlaServer s) {
		server = s;
	}

	public void run () {
		while(true) {
			try {
				server.wlock.lock();
				while(server.writeQueue.size() == 0) {
					server.wcond.await();
				}
				while(!server.writeQueue.isEmpty()) {
					Op write = server.writeQueue.poll();
					if(!server.requests.containsKey(write)) continue;
					Messager.sendMsg(new Response(true, ""), server.requests.get(write));
					server.requests.remove(write);
				}
			} catch (Exception e) {}
			finally {
				server.wlock.unlock();
			}
		}
	}
}
