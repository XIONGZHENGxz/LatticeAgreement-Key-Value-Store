package la.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CyclicBarrier;
import java.net.Socket;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import la.common.Util;

import java.net.InetSocketAddress;

public class Client extends Thread{

	public List<String> servers;
	public List<Integer> ports;
	public List<String> ops;
	public CyclicBarrier gate;
	public int num_prop;
	public double latency;
	public SocketChannel socket;
	public int id;
	public DataOutputStream output;
	public DataInputStream input;
	public Random rand;

	public Client(List<String> ops, String config, CyclicBarrier gate, int num_prop, int id) { 
		this.servers = new ArrayList<>();
		this.ports = new ArrayList<>();
		Util.readConf(servers, ports, config);
		this.ops = ops;
		this.num_prop = num_prop;
		this.gate = gate;
		this.id = id;
		this.rand = new Random();
	}

	public Response handleRequest(Request req) {
		return null;
	}

	public void shutDown() {
		Request req = new Request("down");
		int s = Util.decideServer(this.servers.size());
		Messager.sendMsg(req, this.servers.get(s), this.ports.get(s));
	}

	public boolean execute (Op op) {
		ByteBuffer buffer = op.toBytes();
		buffer.flip();
		try {
			socket.write(buffer);
			ByteBuffer bb = ByteBuffer.allocate(48);
			long start = Util.getCurrTime();
			int bytes = socket.read(bb);
			while(bytes != -1 && bytes == 0 && Util.getCurrTime() - start < Util.TIMEOUT) {
				bytes = socket.read(bb);
			}
			if(bytes < 1) return false;

			bb.flip();
			Result res = Result.values()[bb.getInt()];
			System.out.println("get input ..." + res);
			if(res == Result.TRUE) return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean connect(int replica) {
		try {
			InetSocketAddress addr = new InetSocketAddress(this.servers.get(replica), this.ports.get(replica));
			socket = SocketChannel.open(addr);
			socket.configureBlocking(false);
			//output = new DataOutputStream(socket.getOutputStream());
			//input = new DataInputStream(socket.getInputStream());
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	public boolean reconnect() {
		int replica = rand.nextInt(this.servers.size());
		return this.connect(replica);
	}

	@Override
		public void run() {
			try {
				this.gate.await();
			} catch (Exception e) {}

			int replica = this.id % this.num_prop;
			long start = Util.getCurrTime();
			boolean connected = this.connect(replica);

			while(!connected) {
				connected = this.reconnect();
			}

			for(String op : this.ops) {
				String[] item = op.split("\\s");
				Op ope = null;
				if(item[0].equals("put")) 
					ope = new Op(Type.PUT, item[1], item[2]);
				else ope = new Op(Type.GET, item[1], "");

				while(!this.execute(ope)) {
					this.reconnect();
					break;
				}
			}
			long time = Util.getCurrTime() - start;
			this.latency = time / (double) this.ops.size();
			try {
				socket.close();
			} catch (Exception e) {}
		}
}
