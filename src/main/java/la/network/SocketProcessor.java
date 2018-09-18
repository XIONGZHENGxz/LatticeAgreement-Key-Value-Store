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

import java.io.DataInputStream;
import java.io.ByteArrayInputStream;

public class SocketProcessor implements Runnable {
	public Server server;
	public Selector selector;
	public ExecutorService es;
	public Queue<Socket> inQueue;
	public long socketId;
	public ByteBuffer ready;
	public SocketAcceptor acceptor;

	public SocketProcessor (Server s, SocketAcceptor acceptor) {
		try {
			selector = Selector.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.acceptor = acceptor;
		server = s;
		es = Executors.newFixedThreadPool(Util.threadLimit);
		inQueue = new LinkedList<>();
	}

	public void add(Socket channel) {
		this.inQueue.offer(channel);
	}	

	public void takeNewSockets() {
		Socket socket = this.inQueue.poll();
		try {
			while(socket != null){
				socket.socket.configureBlocking(false);

				SelectionKey key = socket.socket.register(this.selector, SelectionKey.OP_READ);
				key.attach(socket);
				socket = this.inQueue.poll();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void read(SelectionKey key) {
		Socket socket = (Socket) key.attachment();
		ByteBuffer buffer = socket.buffer;
		byte[] res = socket.res;
		int bytesRead = 0;
		try {
			bytesRead = socket.socket.read(buffer);
		} catch (Exception e) {
			this.cancel(key);
			return;
		} 

		if(bytesRead == -1) {
			this.cancel(key);
			return;
		}

		buffer.flip();
		while(buffer.remaining() > 0) {
			if(res == null) {
				if(buffer.remaining() < 4) {
					break;
				}

				//System.out.println(socket);
				socket.want = buffer.getInt();
				if(socket.want > Util.requestSize) {
				//System.out.println(socket);
				System.out.println(bytesRead +" want >>..." +socket.want);
				this.cancel(key);
				return;
				}
				res = new byte[socket.want];
				continue;
			}

			int bytesToCopy = Math.min(socket.want, buffer.remaining());
			buffer.get(res, res.length - socket.want, bytesToCopy);
			socket.want -= bytesToCopy;
			if(socket.want == 0) {
				DataInputStream input = new DataInputStream(new ByteArrayInputStream(res));
				Op op = null;
				try {
					op = new Op(input);
				} catch (Exception e) {}
				if(op != null) {
					Thread t = new Thread(new TcpRequestHandler(server, socket, op));
					es.execute(t);
				}
				res = null;
				break;
			}
		}
		if(res == null) buffer.clear();
		else 
		buffer.compact();			
	}

	public void cancel(SelectionKey key) {
		try {
			Socket socket = (Socket) key.attachment();
			this.acceptor.socketMap.remove(socket.socketId);
			key.cancel();
			key.channel().close();
		} catch (Exception e) {}
	}

	public void run() {
		while(true) {
			takeNewSockets();
			int readReady = -1;
			try {
				readReady = this.selector.select(10);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(readReady > 0){
				Set<SelectionKey> selectedKeys = this.selector.selectedKeys();
				Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

				while(keyIterator.hasNext()) {
					SelectionKey key = keyIterator.next();
					if(!key.isValid()) continue;
					this.read(key);
					keyIterator.remove();
				}
				selectedKeys.clear();
			}	

		}
	}
}
