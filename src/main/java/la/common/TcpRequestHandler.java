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
		while(true) {
			Object obj = Messager.getMsg(s);
			if(obj == null) break;

			Response resp = server.handleRequest(obj);
			if(resp != null) Messager.sendMsg(resp, s);
		}
		try {
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

