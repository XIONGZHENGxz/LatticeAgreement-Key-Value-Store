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

	public Client(List<String> ops, String config, CyclicBarrier gate, int num_prop) { 
		this.servers = new ArrayList<>();
		this.ports = new ArrayList<>();
		Util.readConf(servers, ports, config);
		this.ops = ops;
		this.num_prop = num_prop;
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
				out = new ObjectOutputStream(socket.getOutputStream());
				in = new ObjectInputStream(socket.getInputStream());
				break;
			} catch (Exception e) {
				e.printStackTrace();
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
			Response resp  = (Response) Messager.sendAndWaitReply(req, out, in);
			if(resp != null && resp.ok) return resp;
			this.connect();
			System.out.println("fail..."+op);
		}
	}

	@Override
		public void run() {
			try {
				this.gate.await();
			} catch (Exception e) {}

			int i = 0;
			/*
			   while(Util.getCurrTime() - tmp < Util.cutTime) {
			   String op = this.ops.get(i ++);
			   String[] item = op.split("\\s");
			   Op ope = null;
			   if(item[0].equals("put")) 
			   ope = new Op(item[0], item[1], item[2]);
			   else ope = new Op(item[0], item[1], "");
			   this.executeOp(ope);
			   }
			 */
			try {
				Thread.sleep(Util.cutTime);
			} catch (Exception e) {}

			long start = Util.getCurrTime();
			this.connect();
			while(Util.getCurrTime() - start < Util.testTime) {
				String op = this.ops.get(i++);
				String[] item = op.split("\\s");
				Op ope = null;
				if(item[0].equals("put")) 
					ope = new Op(item[0], item[1], item[2]);
				else ope = new Op(item[0], item[1], "");
				this.executeOp(ope);
				this.count ++;
			}
			this.latency = Util.testTime / (double) this.count;
		}
}
