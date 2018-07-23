package la.cassandra;

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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.ConsistencyLevel;

import la.common.Util;
import la.common.Client;
import la.common.Response;
import la.common.Op;

public class CassClient extends Client{

	private Cluster cluster;

	private Session sess;

	private static String tableName = "lattice";

	private static String keyspaceName = "xiong";

	private int factor;

	public CassClient(List<String> ops, int factor, int max, CyclicBarrier gate) { 
		super(ops, Util.cass_config, gate);
		this.factor = factor;
		String[] points = this.servers.toArray(new String[this.servers.size()]);
		this.connect(points, this.ports.get(0));
		createTable(max);
	}

	public void connect(String[] contactPoints, int port) {
		cluster = Cluster.builder().addContactPoints(contactPoints).withPort(port).build();
		System.out.println("connected to cluster "+ cluster.getMetadata().getClusterName());

		sess = cluster.connect();
	}

	public void createTable(int max) {
		/* create the keyspace */
		sess.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspaceName+ " WITH REPLICATION " + 
				"= {'class': 'SimpleStrategy', 'replication_factor': " + String.valueOf(factor) + 
				"}");

		/* create table */
		sess.execute("CREATE TABLE IF NOT EXISTS " + keyspaceName + "." + tableName + " (" + 
				"key text PRIMARY KEY, val text)");

		/* initialize table */
		for(int i = 0; i < max; i++) {
			String key = String.valueOf(i);
			String val = Util.randomString(3);
			this.put(key, val);
		}
	}

	public void get(String key) {
		String cmd = "SELECT * from " + keyspaceName + "." + 
			tableName + " WHERE key = " + "'" + key + "'";

		SimpleStatement query = new SimpleStatement(cmd); 
		query.setConsistencyLevel(ConsistencyLevel.QUORUM);
		ResultSet results = sess.execute(query);
		if(Util.DEBUG) System.out.println(cmd);
		Row row = results.one();
		System.out.println(row.getString("val"));
	}

	public void put(String key, String value) {
		String cmd = "INSERT INTO " + keyspaceName + "." + tableName + " (key, val) " +
			" values ('" + key + "', '" + value+ "');";
		SimpleStatement query = new SimpleStatement(cmd);
		if(Util.DEBUG) System.out.println(cmd);
		query.setConsistencyLevel(ConsistencyLevel.QUORUM);
		sess.execute(query);
	}

	public void close() {
		sess.close();
		cluster.close();
	}

	public Response executeOp (Op op) {
		if(op.type.equals("get")) 
			this.get(op.key);
		else if(op.type.equals("put")) this.put(op.key, op.val);
		return null;
	}

	public static void main(String...args) {
		int num_ops = Integer.parseInt(args[0]);
		long max = Long.parseLong(args[1]);
		int val_len = Integer.parseInt(args[2]);
		double coef = Double.parseDouble(args[3]);
		double ratio = Double.parseDouble(args[4]);
		int replica_factor = Integer.parseInt(args[5]);
		int num_threads = Util.THREADS;
		CyclicBarrier gate = new CyclicBarrier(num_threads);

		if(args[6].equals("t")) {
			List<String>[] ops = new ArrayList[num_threads];
			CassClient[] clients = new CassClient[num_threads];

			for(int i = 0; i < num_threads; i++) {
				ops[i] = Util.ops_generator(num_ops, max, val_len, coef, ratio);
				clients[i] = new CassClient(ops[i], replica_factor, (int) max, gate);
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
			for(int i = 0; i < num_threads; i++) {
				clients[i].close();
			}

			System.out.println("time taken to complete "+ time);
			System.out.println("throughtput "+ (double)num_threads*num_ops / (double)time);

			double sum = 0.0;
			for(int i = 0; i < num_threads; i++) {
				sum += clients[i].latency;
			}
			double avgLatency = sum / num_threads;
			System.out.println("latency "+ avgLatency);
		}
	}
}
