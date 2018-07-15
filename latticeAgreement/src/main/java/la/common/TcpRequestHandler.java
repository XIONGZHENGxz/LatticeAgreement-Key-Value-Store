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
		Request req = (Request) Messager.getMsg(s);

		//simulate delay of message
		if(rand.nextDouble() < Util.fp)  {
			long start = Util.getCurrTime();
			long curr = Util.getCurrTime();
			while(curr - start < Util.loop) {
				curr = Util.getCurrTime();
			}
		}
		if(Util.DEBUG) System.out.println("get request: "+ req.toString());
		Response resp = server.handleRequest(req);
		if(resp != null) Messager.sendMsg(resp, s);
	}
}

