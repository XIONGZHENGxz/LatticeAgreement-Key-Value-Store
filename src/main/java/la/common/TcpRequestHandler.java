package la.common;

import java.util.Random;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import la.network.Socket;

public class TcpRequestHandler implements Runnable {
	public Server server;
	public Op op;
	public Random rand;
	public Socket socket;
	public TcpRequestHandler(Server server, Socket socket, Op op) {
		this.server = server;
		this.socket = socket;
		this.op = op;
		rand = new Random();
	}

	public void run() {
		Response resp = server.handleRequest(socket.socketId, op);
		if(resp != null) {
			if(Util.DEBUG) System.out.println("complete handling..." + resp);
			Messager.sendMsg(resp, socket.socket);
		}
	}
}

