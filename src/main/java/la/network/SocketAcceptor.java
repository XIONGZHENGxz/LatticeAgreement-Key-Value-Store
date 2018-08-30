package la.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.util.Queue;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import la.common.Util;
import la.common.Server;

/**
 * Created by jjenkov on 19-10-2015.
 */
public class SocketAcceptor extends Thread {

	private int tcpPort = 0;
	private ServerSocketChannel serverSocket = null;
	private int socketId = 0;
	private ExecutorService es;
	private List<SocketProcessor> sps;
	private Server server;
	public Map<Integer, SocketChannel> socketMap;

	public SocketAcceptor(Server server, int tcpPort) {
		this.tcpPort = tcpPort;
		this.server = server;
		this.sps = new ArrayList<>();
		this.es = Executors.newFixedThreadPool(Util.threadLimit);
		this.socketMap = new HashMap<>();
	}

	public void run() {
		try{
			this.serverSocket = ServerSocketChannel.open();
			this.serverSocket.bind(new InetSocketAddress(tcpPort));
		} catch(IOException e){
			e.printStackTrace();
			return;
		}

		while(true){
			try{
				SocketChannel socketChannel = this.serverSocket.accept();

				System.out.println("Socket accepted: " + socketChannel);

				//todo check if the queue can even accept more sockets.
				socketMap.put(socketId, socketChannel);
				Socket socket = new Socket(socketId, socketChannel);
				SocketProcessor sp;
				if(sps.size() < Util.processors) {
					sp = new SocketProcessor(server);
					sps.add(sp);
					sp.add(socket);
					Thread t = new Thread(sp);
					es.execute(t);
				} else {
					sp = sps.get(socketId % sps.size());
					sp.add(socket);
				}
				socketId ++;
			} catch(IOException e){
				e.printStackTrace();
			}
		}

	}
}
