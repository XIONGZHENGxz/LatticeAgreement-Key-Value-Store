package la.mgla;

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

import la.common.Client;
import la.common.Util;
import la.common.Client;
import la.common.Response;
import la.common.Op;
import la.common.Messager;

class MglaClient extends Client {

	public MglaClient(List<String> ops) { 
		super(ops, Util.la_config);
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

		List<String> ops1 = Util.ops_generator(num_ops, max, val_len, coef, ratio);
		List<String> ops2 = Util.ops_generator(num_ops, max, val_len, coef, ratio);
		List<String> ops3 = Util.ops_generator(num_ops, max, val_len, coef, ratio);
		MglaClient c1 = new MglaClient(ops1);
		MglaClient c2 = new MglaClient(ops2);
		MglaClient c3 = new MglaClient(ops3);

		ExecutorService es = Executors.newFixedThreadPool(5);
		es.execute(c1);
		es.execute(c2);
		es.execute(c3);
		boolean ok = false;
		long start = Util.getCurrTime();
		try {
			es.shutdown();
			ok = es.awaitTermination(2, TimeUnit.MINUTES);
		} catch(Exception e) {}

		if(!ok) System.out.println("incomplete simulation....");

		long time = Util.getCurrTime() - start;

		System.out.println("time taken to complete "+ time);
		System.out.println("throughtput "+ (double)3*num_ops / (double)time);
	}
}
