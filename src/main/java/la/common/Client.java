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
	//public Socket socket;
	public SocketChannel socket;
	public int id;
	public DataOutputStream output;
	public DataInputStream input;
	public long count;
	public Random rand;

	public Client(List<String> ops, String config, CyclicBarrier gate, int num_prop, int id) { 
		this.servers = new ArrayList<>();
		this.ports = new ArrayList<>();
		Util.readConf(servers, ports, config);
		this.ops = ops;
		this.num_prop = num_prop;
		this.gate = gate;
		this.rand = new Random();
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
		//System.out.println(this.id + " executing..." +op);
		ByteBuffer buffer = op.toBytes();
		buffer.flip();
		try {
			socket.write(buffer);
			ByteBuffer bb = ByteBuffer.allocate(48);
			int bytes = socket.read(bb);
			long start = Util.getCurrTime();
			while(bytes < 1 && Util.getCurrTime() - start < Util.TIMEOUT) {
				try {
					Thread.sleep(5);
				} catch (Exception e) {}
				bytes = socket.read(bb);
			}
			if(bytes < 1) return false;
			bb.flip();
			Result res = Result.values()[bb.getInt()];
			if(res == Result.TRUE) return true;
			/*
			byte[] req = buffer.array();
			output.write(req);
			output.flush();
			socket.setSoTimeout(Util.TIMEOUT);
			Result res = Result.values()[input.readInt()];
			if(res == Result.TRUE) return true;
			*/
			//System.out.println("get input ..." + res);
		} catch (Exception e) {
			return false;
		}
		return false;
	}

	public void clean() {
		if(socket != null) {
			try {
				socket.close();
				socket = null;
			} catch (Exception e) {}
		}
	}

	public boolean connect(int replica) {
		this.clean();
		try {
			InetSocketAddress addr = new InetSocketAddress(this.servers.get(replica), this.ports.get(replica));
			socket = SocketChannel.open();
			socket.connect(addr);
			long start = Util.getCurrTime();
			while(!socket.finishConnect()) {
				if(Util.getCurrTime() - start > Util.CONNECT_TIMEOUT) 
					return false;
			}

			socket.configureBlocking(false);
			/*
			socket = new Socket();
			socket.connect(addr, Util.CONNECT_TIMEOUT);
			socket.setSoTimeout(Util.TIMEOUT);
			output = new DataOutputStream(socket.getOutputStream());
			input = new DataInputStream(socket.getInputStream());
			*/
			//output = new DataOutputStream(socket.getOutputStream());
			//input = new DataInputStream(socket.getInputStream());
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	public boolean reconnect() {
		try {
			Thread.sleep(rand.nextInt(Util.TIMEOUT));
		} catch (Exception e) {}
		int replica = rand.nextInt(this.servers.size());
		return this.connect(replica);
	}

	@Override
		public void run() {
			try {
				this.gate.await();
			} catch (Exception e) {}

			int replica = this.id % this.num_prop;
			boolean connected = this.connect(replica);
			while(!connected) {
				connected = this.reconnect();
			}
			long timeout = Util.testTime;
			this.cut(Util.cutTime);
			int i = 0;
			long start = Util.getCurrTime();
			while(Util.getCurrTime() - start < timeout) {
				if(i >= this.ops.size()) i = 0;
				String op = this.ops.get(i ++);
				String[] item = op.split("\\s");
				Op ope = null;
				if(item[0].equals("put")) 
					ope = new Op(Type.PUT, item[1], item[2]);
				else ope = new Op(Type.GET, item[1], "");

				while(!this.execute(ope)) {
					int next = (replica + 1 + rand.nextInt(this.servers.size() - 1)) % this.servers.size();
					this.reconnect();
				}
				this.count ++;
			}
			this.latency = timeout / (double) this.count;
			this.cut(Util.cutTime);
		}

	public void cut(int timeout) {
		int i = 0;
		long start = Util.getCurrTime();
		while(Util.getCurrTime() - start < timeout) {
			if(i >= this.ops.size()) i = 0;
			String op = this.ops.get(i ++);
			String[] item = op.split("\\s");
			Op ope = null;
			if(item[0].equals("put")) 
				ope = new Op(Type.PUT, item[1], item[2]);
			else ope = new Op(Type.GET, item[1], "");

			while(!this.execute(ope)) {
				this.reconnect();
			}
		}
	}

}
