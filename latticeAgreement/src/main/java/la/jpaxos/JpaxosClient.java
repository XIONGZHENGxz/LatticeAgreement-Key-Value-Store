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

import lsr.paxos.client.Client;
import lsr.common.Configuration;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;

import la.common.Util;

class JpaxosClient extends Thread{
	public List<String> ops;
	public Client client;
	public CyclicBarrier gate;

	public JpaxosClient(List<String> ops, String file, CyclicBarrier gate) { 
		this.ops = ops;
		this.gate = gate;
		try {
			client = new Client(new Configuration(file));
		} catch (Exception e) {}
	}

	@Override
		public void run() {
			this.gate.await();
			client.connect();
			for(String op : this.ops) {
				String[] item = op.split("\\s");
				MapCommand cmd = null;
				if(item[0].equals("put")) 
					cmd = new MapCommand(item[1], item[2]);
				else cmd = new MapCommand(item[1], "");
				ObjectOutputStream oos = null;
				ByteArrayOutputStream baos = null;

				try {
					baos = new ByteArrayOutputStream();
					oos = new ObjectOutputStream(baos);
					oos.writeObject(cmd);
					oos.flush();
					byte[] request = baos.toByteArray();
					byte[] response = client.execute(request); 
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
		}

	public static void main(String...args) {
		int num_ops = Integer.parseInt(args[0]);
		long max = Long.parseLong(args[1]);
		int val_len = Integer.parseInt(args[2]);
		double coef = Double.parseDouble(args[3]);
		double ratio = Double.parseDouble(args[4]);

		String configFile = args[5];
		int num_threads = Util.THREADS;
		CyclicBarrier gate = new CyclicBarrier(num_threads);

		if(args[6].equals("t")) {
			List<String>[] ops = new ArrayList[num_threads];
			JpaxosClient[] clients = new JpaxosClient[num_threads];

			for(int i = 0; i < num_threads; i++) {
				ops[i] = Util.ops_generator(num_ops, max, val_len, coef, ratio);
				clients[i] = new JpaxosClient(ops[i], configFile, gate);
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
			System.out.println("throughtput "+ (double) num_threads*num_ops / (double)time);
		} else {
			List<String> ops = Util.ops_generator(num_ops, max, val_len, coef, ratio);
			JpaxosClient c = new JpaxosClient(ops, configFile);
			long start = Util.getCurrTime();
			c.start();
			try {
				c.join();
			} catch (Exception e) {}
			long time = Util.getCurrTime() - start;
			double avg = time / (double) num_ops;
			System.out.println("latency: " +avg);
		}
	}
}
