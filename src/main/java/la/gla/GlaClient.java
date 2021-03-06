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

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import la.common.Client;
import la.common.Util;
import la.common.Client;
import la.common.Response;
import la.common.Op;
import la.common.Request;
import la.common.Messager;

class GlaClient extends Client {

	public GlaClient(List<String> ops, String config, CyclicBarrier gate, int num_prop, int id) { 
		super(ops, config, gate, num_prop,id);
	}

	/*
	   public boolean checkComp() {
	   Map<Integer, Set<Op>>[] LVs = new Map[this.servers.size()];
	   Request op = new Request("", new Op("checkComp"));
	   int correct = -1;

	   for(int i = 0; i < this.servers.size(); i++) {
	   Response resp = (Response) Messager.sendAndWaitReply(op, this.servers.get(i), this.ports.get(i));
	   if(resp != null && resp.ok) {
	   correct = i;
	   LVs[i] = resp.lv;
	   }
	   }
	   Set<Integer> keys = LVs[correct].keySet();

	   Map<Integer, Set<Op>>[] values = new Map[this.servers.size()];
	   for(int i = 0; i < this.servers.size(); i++) {
	   values[i] = new HashMap<>();
	   }

	   for(int seq = 0 ; LVs[correct].containsKey(seq); seq ++) {
	   for(int i = 0; i < this.servers.size(); i++) {
	   if(LVs[i] == null) continue;
	   if(seq == 0) values[i].put(seq, LVs[i].get(seq));
	   else {
	   Set<Op> tmp = new HashSet<>(values[i].get(seq - 1));
	   if(LVs[i].get(seq) != null)
	   tmp.addAll(LVs[i].get(seq));
	   values[i].put(seq, tmp);
	   }
	   }
	   }

	   for(int seq = 0; LVs[correct].containsKey(seq); seq ++) {
	   for(int i = 0; i < this.servers.size(); i++) {
	   if(LVs[i] == null) continue;
	   for(int j = i + 1; j < this.servers.size(); j++) {
	   if(LVs[j] == null) continue;
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
	 */

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
		int num_prop = Integer.parseInt(args[7]);
		int num_clients = Integer.parseInt(args[8]);
		int id = Integer.parseInt(args[9]);
		CyclicBarrier gate = new CyclicBarrier(num_threads);

		List<String>[] ops = new ArrayList[num_threads];
		GlaClient[] clients = new GlaClient[num_threads];

		for(int i = 0; i < num_threads; i++) {
			ops[i] = Util.ops_generator(num_ops, max, val_len, coef, ratio);
			clients[i] = new GlaClient(ops[i], config, gate, num_prop, id * num_threads + i);
		}

		ExecutorService es = Executors.newFixedThreadPool(num_threads);
		for(int i = 0; i < num_threads; i++) {
			es.execute(clients[i]);
		}

		boolean ok = false;
		long start = Util.getCurrTime();
		try {
			es.shutdown();
			//System.out.println(id + " shutdown...");
			ok = es.awaitTermination(4, TimeUnit.MINUTES);
		} catch(Exception e) {}

		//	if(!ok) System.out.println(id + " incomplete simulation....");

		DecimalFormat df = new DecimalFormat("#.00"); 
		int num = (int) Util.failTime * (1000 / (int) Util.timeout);

		for(int j = 0; j < num; j++) {
			long sum_count = 0;
			double sum = 0.0;
			for(int i = 0; i < num_threads; i++) {
				sum += clients[0].latencies.get(j);
				sum_count += clients[i].counts.get(j);
			}
			double avgLatency = sum / num_threads;
			double th = 0.0;
			th = (double) num_clients * sum_count;
			System.out.println(df.format(th));
			System.out.println(df.format(avgLatency));
		}


		//write("wgla," + clients[0].num_prop + "," + num_threads + "," +num_ops + "," + ratio + "," + coef + "," + th + "," + avgLatency + "\n");

		if(Util.TEST) {
			System.out.println("checking...");
			try {
				Thread.sleep(1000);
			} catch (Exception e) {}
			//boolean comp = clients[0].checkComp();
			//System.out.println("comparable: "+ comp);
		}
	}
	public static void write(String str) {
		try {
			FileWriter fw = new FileWriter("results.csv", true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(str);
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
