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

	public JpaxosClient(List<String> ops, String file, CyclicBarrier gate) { 
		this.ops = ops;
		this.gate = gate;
		try {
			client = new Client(new Configuration(file));
		} catch (Exception e) {}
	}

	@Override
		public void run() {
			try {
				this.gate.await();
			} catch (Exception e) {}

			long start = Util.getCurrTime(); 
			for(String op : this.ops) {
				client.connect();
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
					ByteArrayInputStream bais = new ByteArrayInputStream(response);
					DataInputStream dis = new DataInputStream(bais);
					String s = dis.readUTF();
				//	System.out.println("get response for "+ cmd + " " + s); 
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
			long time = Util.getCurrTime() - start;
			this.latency = time / (double) this.ops.size();
		}

	public static void main(String...args) {
		int num_ops = Integer.parseInt(args[0]);
		long max = Long.parseLong(args[1]);
		int val_len = Integer.parseInt(args[2]);
		double coef = Double.parseDouble(args[3]);
		double ratio = Double.parseDouble(args[4]);

		String configFile = args[5];
		int num_threads = Integer.parseInt(args[6]);
		CyclicBarrier gate = new CyclicBarrier(num_threads);

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
			ok = es.awaitTermination(10, TimeUnit.MINUTES);
		} catch(Exception e) {}

		if(!ok) System.out.println("incomplete simulation....");
		DecimalFormat df = new DecimalFormat("#.00"); 
		double sum = 0.0;
		for(int i = 0; i < num_threads; i++) {
			sum += clients[i].latency;
		}
		double avgLatency = sum / num_threads;
		long time = Util.getCurrTime() - start;

		System.out.println(df.format((double) num_threads*1000*num_ops / (double)time));
		System.out.println(df.format(avgLatency));
	}
}
