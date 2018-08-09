package la.wgla;
import la.common.Messager;
import la.common.Response;
import la.common.Op;
import java.util.Set;

public class ReadResponder extends Thread {
	public GlaServer server;

	public ReadResponder (GlaServer s) {
		server = s;
	}

	public void run () {
		while(true) {
			try {
				server.rlock.lock();
				while(server.readQueue.size() == 0) {
					server.rcond.await();
				}
				while(!server.readQueue.isEmpty()) {
					Op read = server.readQueue.poll();
					Set<Op> reads = server.reads.get(read.key);
					for(Op r : reads) {
						Response resp = server.get(r.key);
						Messager.sendMsg(resp, server.requests.get(r));
						server.requests.remove(r);
					}
					server.reads.remove(read.key);
				}
			} catch (Exception e) {}
			finally {
				server.rlock.unlock();
			}
		}
	}
}
