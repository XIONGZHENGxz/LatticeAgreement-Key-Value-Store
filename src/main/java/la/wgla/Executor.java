package la.wgla;

public class Executor extends Thread {
	public GlaServer server;

	public Executor (GlaServer s) {
		server = s;
	}

	public void run() {
		try {
			server.elock.lock();
			while(true) {
				server.econd.await();
				int seq = server.gla.learntMaxSeq;
				server.apply(seq);
			}
		} catch (Exception e) {
		} finally {
			server.elock.unlock();
		}
	}
}
