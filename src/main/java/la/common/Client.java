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

public class Client extends Thread{

	public List<String> servers;
	public List<Integer> ports;
	public List<String> ops;
	public CyclicBarrier gate;
	public int num_prop;
	public double latency;

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

	public void shutDown() {
		Request req = new Request("down");
		int s = Util.decideServer(this.servers.size());
		Messager.sendMsg(req, this.servers.get(s), this.ports.get(s));
	}
		
	public Response executeOp (Op op) {
		while(true) {
			int s = Util.decideServer(num_prop);
			//System.out.println("executing..."+op + " to "+s);
			Request req = new Request("client", op);
			Response resp  = (Response) Messager.sendAndWaitReply(req, this.servers.get(s), this.ports.get(s));
			if(resp != null && resp.ok) return resp;
			//System.out.println("fail..."+op + " to "+s);
		}
	}
	
	@Override
	public void run() {
		try {
			this.gate.await();
		} catch (Exception e) {}

		long start = Util.getCurrTime();
		for(String op : this.ops) {
			String[] item = op.split("\\s");
			Op ope = null;
			if(item[0].equals("put")) 
				ope = new Op(item[0], item[1], item[2]);
			else ope = new Op(item[0], item[1], "");
			this.executeOp(ope);
			//System.out.println("completed executing..."+ope);
		}
		long time = Util.getCurrTime() - start;
		this.latency = time / (double) this.ops.size();
	}
}
