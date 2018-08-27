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

import la.common.Util;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Client extends Thread{

	public List<String> servers;
	public List<Integer> ports;
	public List<String> ops;
	public CyclicBarrier gate;
	public int num_prop;
	public double latency;
	public long count = 0;
	public Socket socket;
	public ObjectOutputStream out;
	public ObjectInputStream in;
	public int id;
	public int replica;

	public Client(List<String> ops, String config, CyclicBarrier gate, int num_prop, int id) { 
		this.servers = new ArrayList<>();
		this.ports = new ArrayList<>();
		Util.readConf(servers, ports, config);
		this.ops = ops;
		this.num_prop = num_prop;
		this.id = id;
		this.replica = this.id % this.servers.size();
		this.gate = gate;
	}

	public Response handleRequest(Request req) {
		return null;
	}

	public void connect() {
		this.clean();
		while(true) {
			int replica = Util.decideServer(num_prop);
			socket = new Socket();
			try {
				socket.connect(new InetSocketAddress(this.servers.get(replica), this.ports.get(replica)), Util.TIMEOUT);
				socket.setSoTimeout(Util.TIMEOUT);
				break;
			} catch (Exception e) {
			}
		}
	}

	public void clean() {
		if(socket != null) {
			try {
				socket.close();
			} catch (Exception e) {}
		}
	}

	public void shutDown() {
		Request req = new Request("down");
		int s = Util.decideServer(this.servers.size());
		Messager.sendMsg(req, this.servers.get(s), this.ports.get(s));
	}

	public Response executeOp (Op op) {
		Request req = new Request("client", op);
		while(true) {
			//int s = Util.decideServer(this.servers.size());
			//Response resp = (Response) Messager.sendAndWaitReply(req, this.servers.get(s), this.ports.get(s));
			Response resp = (Response) Messager.sendAndWaitReply(req, socket);
			if(resp != null) return resp;
			this.connect();
		}
	}

	@Override
		public void run() {
			try {
				this.gate.await();
			} catch (Exception e) {}

			this.cut();
			int i = 0;
			long start = Util.getCurrTime();
			long timeout = Util.testTime;
			this.connect();
			while(Util.getCurrTime() - start < timeout) {
				if(i >= this.ops.size()) i = 0;
				String op = this.ops.get(i++);
				String[] item = op.split("\\s");
				Op ope = null;
				if(item[0].equals("put")) 
					ope = new Op(item[0], item[1], item[2]);
				else ope = new Op(item[0], item[1], "");
				this.executeOp(ope);
				this.count ++;
			}
			this.latency = timeout / (double) this.count;
			this.cut();
		}

		public void cut() {
			int i = 0;
			long tmp = Util.getCurrTime();
			while(Util.getCurrTime() - tmp < Util.cutTime) {
				if(i >= this.ops.size()) i = 0;
				String op = this.ops.get(i++);
				String[] item = op.split("\\s");
				Op ope = null;
				if(item[0].equals("put")) 
					ope = new Op(item[0], item[1], item[2]);
				else ope = new Op(item[0], item[1], "");
				this.executeOp(ope);
			}
		}
			
}
