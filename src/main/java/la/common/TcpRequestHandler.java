package la.common;

import java.util.Random;
import java.net.Socket;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class TcpRequestHandler implements Runnable {
	public Server server;
	public SelectionKey key;
	public Op op;
	public Random rand;
	public TcpRequestHandler(Server server, SelectionKey key, Op op) {
		this.server = server;
		this.key = key;
		this.op = op;
		rand = new Random();
	}

	public void run() {

		//simulate delay of message
		Response resp = server.handleRequest(op);
		if(resp != null) {
			if(Util.DEBUG) System.out.println("complete handling..." + resp);
			Messager.sendMsg(resp, key);
		}
	}
}

