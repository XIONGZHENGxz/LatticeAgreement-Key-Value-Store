package la.jpaxos;

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

import lsr.paxos.client.Client;
import lsr.common.Configuration;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import la.common.Util;

class JpaxosClient extends Thread{
	public List<String> ops;
	public Client client;
	public CyclicBarrier gate;
	public double latency;
	public long count;
	public List<Double> latencies;
	public List<Long> counts;
	public int id;

	public JpaxosClient(List<String> ops, String file, CyclicBarrier gate, int id) { 
		this.ops = ops;
		this.gate = gate;
		this.count = 0;
		this.counts = new ArrayList<>();
		this.latencies = new ArrayList<>();
		this.id = id;
		try {
			client = new Client(new Configuration(file));
		} catch (Exception e) {}
	}

	@Override
		public void run() {
			try {
				this.gate.await();
			} catch (Exception e) {}

			client.connect();
			this.cut(Util.cutTime);
			//long timeout = Util.testTime;
			long timeout = 500;
			int i = 0;
			for(int j = 0; j < 60; j++) {
				this.count = 0;
				this.latency = 0;
				long start = Util.getCurrTime(); 
				if(this.id == 50 && j == 30) {
					String op = "fail";
					this.execute(op);
				}
				while(Util.getCurrTime() - start < timeout) {
					if(i >= this.ops.size()) i = 0;
					String op = this.ops.get(i ++);	
					this.execute(op);
					this.count ++;
				}
				this.counts.add(count);
				this.latency = timeout / (double) this.count;
				this.latencies.add(latency);
			}
			this.cut(Util.cutTime);
		}

	public void cut(int timeout) {
		int i = 0;
		long start = Util.getCurrTime();
		while(Util.getCurrTime() - start < timeout) {
			if(i >= this.ops.size()) i = 0;
			String op = this.ops.get(i ++);
			this.execute(op);
		}
	}

	public void execute(String op) {
		String[] item = op.split("\\s");
		MapCommand cmd = null;
		if(item[0].equals("put")) 
			cmd = new MapCommand(item[1], item[2]);
		else if(item[0].equals("get")) cmd = new MapCommand(item[1], "");
		else cmd = new MapCommand("fail", "");
		ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;

		try {
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(cmd);
			oos.flush();
			byte[] request = baos.toByteArray();
			byte[] response = this.client.execute(request); 
			ByteArrayInputStream bais = new ByteArrayInputStream(response);
			DataInputStream dis = new DataInputStream(bais);
			String s = dis.readUTF();
			//System.out.println("get response..." +s);
		} catch (Exception e) {
		} finally {
			if(baos != null) {
				try {
					baos.close();
				} catch (Exception e) {}
			} 
			if(oos != null) {
				try {
					oos.close();
				} catch (Exception e) {}
			}
		}
	}

	public static void main(String...args) {
		int num_ops = Integer.parseInt(args[0]);
		long max = Long.parseLong(args[1]);
		int val_len = Integer.parseInt(args[2]);
		double coef = Double.parseDouble(args[3]);
		double ratio = Double.parseDouble(args[4]);

		String configFile = args[5];
		int num_threads = Integer.parseInt(args[6]);
		int num_clients = Integer.parseInt(args[7]);
		int id = Integer.parseInt(args[8]);
		CyclicBarrier gate = new CyclicBarrier(num_threads);

		List<String>[] ops = new ArrayList[num_threads];
		JpaxosClient[] clients = new JpaxosClient[num_threads];

		for(int i = 0; i < num_threads; i++) {
			ops[i] = Util.ops_generator(num_ops, max, val_len, coef, ratio);
			clients[i] = new JpaxosClient(ops[i], configFile, gate, id * num_threads + i);
		}

		ExecutorService es = Executors.newFixedThreadPool(num_threads);
		for(int i = 0; i < num_threads; i++) {
			es.execute(clients[i]);
		}

		boolean ok = false;
		long start = Util.getCurrTime();
		try {
			es.shutdown();
			ok = es.awaitTermination(3, TimeUnit.MINUTES);
		} catch(Exception e) {}

		if(!ok) System.out.println("incomplete simulation....");
		DecimalFormat df = new DecimalFormat("#.00"); 
		for(int j = 0; j < 60; j++) {
			double sum = 0.0;
			long sum_count = 0;
			for(int i = 0; i < num_threads; i++) {
				sum += clients[i].latencies.get(j);
				sum_count += clients[i].counts.get(j);
			}
			double avgLatency = sum / num_threads;
			System.out.println(df.format((double) num_clients * sum_count));
			//System.out.println(df.format((double) num_clients * 1000 * sum_count / (double)Util.testTime));
			System.out.println(df.format(avgLatency));
		}
	}
}
