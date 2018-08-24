package la.network;

import la.common.*;

import java.io.IOException;
import java.util.Queue;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.Selector;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.Iterator;

public class SocketProcessor implements Runnable {
	public Server server;
	public Selector selector;
	public ExecutorService es;
	public Queue<SocketChannel> inQueue;
	public long socketId;
	public ByteBuffer ready;

	public SocketProcessor (Server s) {
		try {
			selector = Selector.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
		server = s;
		es = Executors.newFixedThreadPool(Util.threadLimit);
		inQueue = new LinkedList<>();
		socketId = 0;
	}

	public void add(SocketChannel channel) {
		this.inQueue.offer(channel);
	}	

	public void takeNewSockets() {
		SocketChannel channel = this.inQueue.poll();
		try {
			while(channel != null){
				channel.configureBlocking(false);

				SelectionKey key = channel.register(this.selector, SelectionKey.OP_READ);
				channel = this.inQueue.poll();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		while(true) {
			takeNewSockets();
			int readReady = -1;
			try {
				readReady = this.selector.select();
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(readReady > 0){
				Set<SelectionKey> selectedKeys = this.selector.selectedKeys();
				Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

				while(keyIterator.hasNext()) {
					SelectionKey key = keyIterator.next();
					Op op = Messager.getMsg(key);
					TcpRequestHandler req = new TcpRequestHandler(server, key, op);
					es.execute(req);
					keyIterator.remove();
				}
				selectedKeys.clear();
			}	
		}
	}
}
