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
	public int TIMEOUT;
	public int failure;
	public int replica;
	private static final int TO_MULTIPLIER = 3;
	private static final int INITIAL_TIMEOUT = 3000 / TO_MULTIPLIER;
	/* Maximum time to wait for an answer from the replica before reconnect */
	public static final int MAX_TIMEOUT = 5000;
	/* Minimum time to wait for an answer from the replica before reconnect */
	public static final int MIN_TIMEOUT = 500;
	private final MovingAverage average = new MovingAverage(0.2, INITIAL_TIMEOUT);
	public Socket socket;
	//public SocketChannel socket;
	public int id;
	public DataOutputStream output;
	public DataInputStream input;
	public long count;
	public Random rand;
	public List<Long> counts;
	public List<Double> latencies;

	public Client(List<String> ops, String config, CyclicBarrier gate, int num_prop, int id) { 
		this.failure = -1;
		this.servers = new ArrayList<>();
		this.ports = new ArrayList<>();
		this.TIMEOUT = Util.TIMEOUT;
		Util.readConf(servers, ports, config);
		this.ops = ops;
		this.num_prop = num_prop;
		this.gate = gate;
		this.rand = new Random();
		this.id = id;
		this.rand = new Random();
		this.counts = new ArrayList<>();
		this.latencies = new ArrayList<>();
	}

	public Response handleRequest(Request req) {
		return null;
	}

	public void shutDown() {
		Request req = new Request("down");
		int s = Util.decideServer(this.servers.size());
		Messager.sendMsg(req, this.servers.get(s), this.ports.get(s));
	}
	/** Modifies socket timeout basing on previous reply times */
	private void updateTimeout() throws Exception {
		TIMEOUT = (int) (TO_MULTIPLIER * average.get());
		TIMEOUT = Math.min(TIMEOUT, MAX_TIMEOUT);
		TIMEOUT = Math.max(TIMEOUT, MIN_TIMEOUT);
		socket.setSoTimeout(TIMEOUT);
	}

	public boolean execute (Op op) {
		//System.out.println(this.id + " executing...." + op);
		long s = System.currentTimeMillis();
		ByteBuffer buffer = op.toBytes();
		buffer.flip();
		try {
			/*
			   socket.write(buffer);
			   ByteBuffer bb = ByteBuffer.allocate(48);
			   int bytes = socket.read(bb);
			   long start = Util.getCurrTime();
			   while(bytes < 4 && Util.getCurrTime() - start < this.TIMEOUT) {
			   try {
			   Thread.sleep(5);
			   } catch (Exception e) {}
			   bytes = socket.read(bb);
			   }
			   if(bytes < 1) {
			   increaseTimeout();
			   return false;
			   }
			   bb.flip();
			   Result res = Result.values()[bb.getInt()];
			   long time = System.currentTimeMillis() - s;
			   average.add(time);
			   updateTimeout();
			   if(res == Result.TRUE) return true;
			 */
			byte[] req = buffer.array();
			output.write(req);
			output.flush();
			//System.out.println(this.id + " wrote...");
			Response reply = new Response(input);
			long time = System.currentTimeMillis() - s;
			average.add(time);
			updateTimeout();
			if(reply.ok == Result.TRUE) return true;
		} catch (Exception e) {
			//e.printStackTrace();
			increaseTimeout();
			return false;
		}
		increaseTimeout();
		return false;
	}

	public void increaseTimeout() {
		int timeout = (int) (TO_MULTIPLIER * average.get());
		timeout = Math.min(timeout, MAX_TIMEOUT);
		average.add(timeout);
	}

	public void clean() {
		if(socket != null) {
			try {
				socket.close();
				socket = null;
			} catch (Exception e) {}
		}
	}

	public boolean connect() {
		this.clean();
		try {
			InetSocketAddress addr = new InetSocketAddress(this.servers.get(replica), this.ports.get(replica));
			/*
			   socket = SocketChannel.open();
			   socket.connect(addr);
			   long start = Util.getCurrTime();
			   while(!socket.finishConnect()) {
			   if(Util.getCurrTime() - start > Util.CONNECT_TIMEOUT) 
			   return false;
			   }

			   socket.configureBlocking(false);
			 */
			socket = new Socket();
			socket.connect(addr, Util.TIMEOUT);
			this.updateTimeout();
			output = new DataOutputStream(socket.getOutputStream());
			input = new DataInputStream(socket.getInputStream());
			//output = new DataOutputStream(socket.getOutputStream());
			//input = new DataInputStream(socket.getInputStream());
		} catch (Exception e) {
			//e.printStackTrace();
			this.failure = this.replica;
			return false;
		}
		return true;
	}

	public boolean reconnect() {
		//System.out.println(this.id +" reconnecting...");
		try {
			Thread.sleep(Util.TIMEOUT / 4 + rand.nextInt(Util.TIMEOUT / 2));
		} catch (Exception e) {}
		return this.connect();
	}

	@Override
		public void run() {
			try {
				this.gate.await();
			} catch (Exception e) {
				e.printStackTrace();
			}

			replica = this.id % this.num_prop;
			boolean connected = this.connect();
			while(!connected) {
				replica = (replica + 1 + rand.nextInt(this.servers.size() - 1)) % this.servers.size();
				connected = this.reconnect();
			}
			long timeout = 1000;
			int num = (int) Util.failTime * (1000 / (int)Util.timeout);
			this.cut(Util.cutTime);
			int i = 0;
			for (int j = 0; j < num; j ++) {
				this.count = 0;
				if(j == num / 2 && this.id == 50) { 
					Op op = new Op(Type.FAIL, "", "");
					while(!this.execute(op)) {
						this.reconnect();
					}
				}
				long start = Util.getCurrTime();
				while(Util.getCurrTime() - start < timeout) {
					if(i >= this.ops.size()) i = 0;
					String op = this.ops.get(i ++);
					String[] item = op.split("\\s");
					Op ope = null;
					if(item[0].equals("put")) 
						ope = new Op(Type.PUT, item[1], item[2]);
					else ope = new Op(Type.GET, item[1], "");

					while(!this.execute(ope) && Util.getCurrTime() - start < timeout) {
						replica = (replica + 1 + rand.nextInt(this.servers.size() - 1)) % this.servers.size();
						while(this.replica == this.failure) {
							replica = (replica + 1 + rand.nextInt(this.servers.size() - 1)) % this.servers.size();
						}
						this.reconnect();
					}
					this.count ++;
				}
				this.latency = timeout / (double) this.count;
				this.counts.add(count * (1000 / timeout));
				this.latencies.add(this.latency);
			}
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

			while(!this.execute(ope) && Util.getCurrTime() - start < timeout) {
				replica = (replica + 1 + rand.nextInt(this.servers.size() - 1)) % this.servers.size();
				this.reconnect();
			}
		}
	}

}
