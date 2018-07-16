package la.common;
import java.util.Random;
import java.net.Socket;

class TcpRequestHandler implements Runnable {
	public Server server;
	public Socket s;
	public Random rand;
	public TcpRequestHandler(Server server, Socket s) {
		this.server = server;
		this.s = s;
		rand = new Random();
	}

	public void run() {
		Object obj = Messager.getMsg(s);

		//simulate delay of message
		if(rand.nextDouble() < Util.fp)  {
			long start = Util.getCurrTime();
			long curr = Util.getCurrTime();
			while(curr - start < Util.loop) {
				curr = Util.getCurrTime();
			}
		}
		Response resp = server.handleRequest(obj);
		if(resp != null) Messager.sendMsg(resp, s);
	}
}

