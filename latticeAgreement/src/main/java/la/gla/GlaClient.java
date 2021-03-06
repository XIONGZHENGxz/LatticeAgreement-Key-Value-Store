package la.gla;

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
import java.text.DecimalFormat;

import la.common.Client;
import la.common.Util;
import la.common.Client;
import la.common.Response;
import la.common.Op;
import la.common.Messager;

class GlaClient extends Client {

	public GlaClient(List<String> ops, String config, CyclicBarrier gate) { 
		super(ops, config, gate);
	}

	public boolean checkComp() {
		Map<Integer, Set<Op>>[] LVs = new Map[this.servers.size()];
		Op op = new Op("checkComp");

		for(int i = 0; i < this.servers.size(); i++) {
			while(true) {
				Response resp = (Response) Messager.sendAndWaitReply(op, this.servers.get(i), this.ports.get(i));
				if(resp != null && resp.ok) {
					LVs[i] = resp.lv;
					break;
				}
			}
		}
		Set<Integer> keys = LVs[0].keySet();
		Map<Integer, Set<Op>>[] values = new Map[this.servers.size()];
		for(int i = 0; i < this.servers.size(); i++) {
			values[i] = new HashMap<>();
		}

		for(int seq = 0 ; LVs[0].containsKey(seq); seq ++) {
			for(int i = 0; i < this.servers.size(); i++) {
				if(seq == 0) values[i].put(seq, LVs[i].get(seq));
				else {
					Set<Op> tmp = new HashSet<>(values[i].get(seq - 1));
					tmp.addAll(LVs[i].get(seq));
					values[i].put(seq, tmp);
				}
			}
		}

		for(int seq = 0; LVs[0].containsKey(seq); seq ++) {
			for(int i = 0; i < this.servers.size(); i++) {
				for(int j = i + 1; j < this.servers.size(); j++) {
					if(!comparable(values[i].get(seq), values[j].get(seq))) {
						System.out.println(i + " and " + j + " incomparable "+ seq+ " \n" + LVs[i].get(seq)+ "\n" + LVs[j].get(seq));
						Set<Op> tmp1 = new HashSet<>(values[i].get(seq));

						values[i].get(seq).removeAll(values[j].get(seq));
						values[j].get(seq).removeAll(tmp1);
						System.out.println("\n" + values[i].get(seq)+ "\n" + values[j].get(seq));
						return false;
					}
				}
			}
		}
		return true;
	}

	public boolean comparable(Set<Op> s1, Set<Op> s2) {
		return s1.containsAll(s2) || s2.containsAll(s1);
	}

	public static void main(String...args) {
		int num_ops = Integer.parseInt(args[0]);
		long max = Long.parseLong(args[1]);
		int val_len = Integer.parseInt(args[2]);
		double coef = Double.parseDouble(args[3]);
		double ratio = Double.parseDouble(args[4]);
		String config = args[5];

		int num_threads = Integer.parseInt(args[6]);
		CyclicBarrier gate = new CyclicBarrier(num_threads);

		List<String>[] ops = new ArrayList[num_threads];
		GlaClient[] clients = new GlaClient[num_threads];

		for(int i = 0; i < num_threads; i++) {
			ops[i] = Util.ops_generator(num_ops, max, val_len, coef, ratio);
			clients[i] = new GlaClient(ops[i], config, gate);
		}

		ExecutorService es = Executors.newFixedThreadPool(num_threads);
		for(int i = 0; i < num_threads; i++) {
			es.execute(clients[i]);
		}

		boolean ok = false;
		long start = Util.getCurrTime();
		try {
			es.shutdown();
			ok = es.awaitTermination(10, TimeUnit.MINUTES);
		} catch(Exception e) {}
	
		if(!ok) System.out.println("incomplete simulation....");

		DecimalFormat df = new DecimalFormat("#.00"); 
		long time = Util.getCurrTime() - start;
		double th = (double) num_threads * 1000 * num_ops / (double) time;

		System.out.println(df.format(th));
		double sum = 0.0;
		for(int i = 0; i < num_threads; i++) {
			sum += clients[i].latency;
		}
		double avgLatency = sum / num_threads;
		System.out.println(df.format(avgLatency));
	}
}
