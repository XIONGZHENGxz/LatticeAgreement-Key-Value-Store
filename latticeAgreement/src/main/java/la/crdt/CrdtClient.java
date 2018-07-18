package la.crdt;

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

import la.common.*;

class CrdtClient extends Client{

	public ClientListener cl;
	public int check_count;

	public CrdtClient(List<String> ops, int port, CyclicBarrier gate) {
		super(ops, Util.la_config, gate);
		this.check_count = 0;
		cl = new ClientListener(this, port);
		cl.start();
	}
		
	public CrdtClient(List<String> ops) { 
		super(ops, Util.la_config);
	}

	public Response executeOp(Op op) {
		while(true) {
			int s = Util.decideServer(this.servers.size());
			CrdtRequest req = new CrdtRequest("client", op);
			Response resp = (Response) Messager.sendAndWaitReply(req, this.servers.get(s), this.ports.get(s));
			if(resp != null && resp.ok) return resp;
		}
	}

	public Response handleRequest(Object req) {
		if(req != null) this.check_count ++;
		return null;
	}

	public void sendReq(CrdtRequest req, int peer) {
		while(true) {
			boolean sent = Messager.sendMsg(req, this.servers.get(peer), this.ports.get(peer));
			if(sent) break;
		}
	}

	public long calcTime(String op) {
		String[] item = op.split("\\s");

		Op ope = new Op(item[0], item[1], item.length < 3 ? "": item[2]);
		CrdtRequest r = new CrdtRequest("client", ope);

		long start = Util.getCurrTime();
		while(true) {
			int s = Util.decideServer(this.servers.size());
			Response resp  = (Response) Messager.sendAndWaitReply(r, this.servers.get(s), this.ports.get(s));
			if(resp != null && resp.ok) break;
		}

		if(item[0].equals("get")) {
			return Util.getCurrTime() - start;
		} else if(item[0].equals("put")) {
			start = Util.getCurrTime();
			Op predicate = new Op("check", item[1], item[2]);
			CrdtRequest req = new CrdtRequest("check", predicate);
			for(int i = 0; i < this.servers.size(); i++) this.sendReq(req, i);

			while(this.check_count < this.servers.size()) {
				if(Util.DEBUG) System.out.println("put "+this.servers.size()+ " "+ this.check_count);
				try{
					Thread.sleep(5);
				} catch(Exception e) {
				}
			}
			this.check_count = 0;
			if(Util.DEBUG) System.out.println("put check successful...");
			return Util.getCurrTime() - start;
		}

		return 0;
	}

	public static void main(String...args) {
		int num_ops = Integer.parseInt(args[0]);
		long max = Long.parseLong(args[1]);
		int val_len = Integer.parseInt(args[2]);
		double coef = Double.parseDouble(args[3]);
		double ratio = Double.parseDouble(args[4]);
		int num_threads = Util.THREADS;
		CyclicBarrier gate = new CyclicBarrier(num_threads);

		if(args[5].equals("t")) {
			List<String>[] ops = new ArrayList[num_threads];
			CrdtClient[] clients = new CrdtClient[num_threads];

			for(int i = 0; i < num_threads; i++) {
				ops[i] = Util.ops_generator(num_ops, max, val_len, coef, ratio);
				clients[i] = new CrdtClient(ops[i], gate);
			}

			ExecutorService es = Executors.newFixedThreadPool(num_threads);
			for(int i = 0; i < num_threads; i++) {
				es.execute(clients[i]);
			}

			boolean ok = false;
			long start = Util.getCurrTime();
			try {
				es.shutdown();
				ok = es.awaitTermination(2, TimeUnit.MINUTES);
			} catch(Exception e) {}

			if(!ok) System.out.println("incomplete simulation....");

			long time = Util.getCurrTime() - start;

			System.out.println("time taken to complete "+ time);
			System.out.println("throughtput "+ (double)num_threads*num_ops / (double)time);

		} else if(args[5].equals("l")){
			List<String> ops = Util.ops_generator(num_ops, max, val_len, coef, ratio);
			CrdtClient c = new CrdtClient(ops);
			long start = Util.getCurrTime();
			c.start();
			try {
				c.join();
			} catch (Exception e) {}

			long time = Util.getCurrTime() - start;

			double avg = time/(double)num_ops; 
			System.out.println("latency: "+ avg);
		} else if(args[5].equals("c")) {
			List<String> ops = Util.ops_generator(num_ops, max, val_len, coef, ratio);
			CrdtClient c = new CrdtClient(ops, Util.c_port);
			long time = 0;
			for(String op : c.ops) {
				time += c.calcTime(op);
			}
			double avg = time / (double) num_ops;
			System.out.println("convergence: "+ avg);
		} else System.err.println("invalid option...");
			
	}

}
