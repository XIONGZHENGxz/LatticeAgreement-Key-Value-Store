package la.common;

import java.io.IOException;
import java.io.FileWriter;
import java.util.Random;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;

import la.crdt.*;

public class Util {
	public static int c_port = 1886;
	public static int interval = 10;
	public static int syncFreq = 5000;
	public static int loop = 1000;
	public static int freq = 7;
	public static int threshold = 500;
	public static int requestSize = 48;
	public static int threadLimit = 100;
	public static int cutTime = 15 * 1000;
	public static int testTime = 20 * 1000;
	public static int processors = 3;
	public static int writer = 15;
	public static int batchSize = 0;
	public static double fp = 0.00;
	public static boolean DEBUG = false;
	public static boolean TEST = false;
	public static boolean DELAY = false; //simulate remote replica 
	public static int delayReplica = 2; //remote replica 
	public static int clock = 0;
	public static String checker_host = "192.168.12.13";
	public static final String[] types = {"get", "put", "remove"};
	public static String opFile = "ops.txt";
	public static String la_config = "config/la_config.txt";
	public static String cass_config = "config/cassandra_config.txt";
	public static Random rand = new Random();
	public static final int TIMEOUT = 2000;
	public static final int CONNECT_TIMEOUT = 500;
	public static final int session_timeout = 1000 * 1000;
	public static final int THREADS = 100;
	public static final int fail = 10000;
	public static final int wait = 0;
	public static final int sigTimeout = 5;
	public static final int gap = 2;

	public static long getCurrTime() {
		return System.currentTimeMillis();
	}

	public static int compare(TimeStamp ts1, TimeStamp  ts2) {
		if(ts1.clock < ts2.clock) return -1;
		else if(ts1.clock > ts2.clock) return 1;
		else {
			if(ts1.id < ts2.id) return -1;
			else if(ts1.id > ts2.id) return 1;
			else return 0;
		}
	}

	public static List<String> readOps() {
		List<String> ops = new ArrayList<>();
		try {
			BufferedReader bf = new BufferedReader(new FileReader(opFile));
			String line = "";
			while((line = bf.readLine()) != null) {
				ops.add(line);
			}
			bf.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
		return ops;
	}

	public static void readConf(List<String> peers, List<Integer> ports, String config) {
		try {
			BufferedReader bf = new BufferedReader(new FileReader(config));
			String line = "";
			while((line = bf.readLine()) != null) {
				String[] tmp = line.split(":");		
				peers.add(tmp[0]);
				ports.add(Integer.parseInt(tmp[1]));
			}
			bf.close();
		} catch(Exception e) {}
	}

	public static void readConf(List<String> peers, List<Integer> ports, List<Integer> s_ports, String config) {
		try {
			BufferedReader bf = new BufferedReader(new FileReader(config));
			String line = "";
			while((line = bf.readLine()) != null) {
				String[] tmp = line.split(":");		
				peers.add(tmp[0]);
				ports.add(Integer.parseInt(tmp[1]));
				s_ports.add(Integer.parseInt(tmp[2]));
			}
			bf.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static Set<String> put_generator(int num, int key_len, int val_len) {
		ArrayList<String> ops = new ArrayList<>();
		Set<String> keys = new HashSet<String>();

		for(int i = 0; i < num; i++) {
			String key = randomString(key_len);
			String val = randomString(val_len);
			ops.add("put" + " "+ key + " " + val);
			keys.add(key);
		}
		try {
			FileWriter w = new FileWriter(opFile);
			for(int i = 0; i < ops.size(); i++) {
				w.write(ops.get(i) + "\n");
			}
			w.close();
		} catch(Exception e) {

		}
		return keys;
	}

	public static List<String> ops_generator(int count, long max, int val_len, double coef, double read_ratio) {
		String[] types = {"put", "get"};
		ZipfianGenerator zpf = new ZipfianGenerator(max, coef);
		List<String> ops = new ArrayList<>();
		Random rand = new Random();
		if(coef == -1.0) {
			for(int i = 0; i < count; i ++) {
				String type = "";
				String key = String.valueOf(rand.nextInt((int)max));
				String val = randomString(val_len);
				if(rand.nextDouble() < read_ratio) {
					type = "get";
					ops.add(type + " " + key);
				} else {
					type = "put";
					ops.add(type + " " + key + " " + val);
				}
			}
		} else {
			for(int i = 0; i < count; i ++) {
				String type = "";
				String key = String.valueOf(zpf.nextValue());
				String val = randomString(val_len);
				if(rand.nextDouble() < read_ratio) {
					type = "get";
					ops.add(type + " " + key);
				} else {
					type = "put";
					ops.add(type + " " + key + " " + val);
				}
			}
		}

		return ops;
	}

	public static void ops_generator(int num, int key_len, int val_len, boolean morePut) {
		Set<String> keys = new HashSet<String>();
		ArrayList<String> ops = new ArrayList<>();
		boolean append = false;
		if(!morePut) {
			keys = put_generator(num, key_len, val_len);
			append = true;
		}

		for(int i = 0; i < num; i++) {
			String cmd = "";
			if(morePut) {
				if(i % 10 == 9) cmd = "remove";
				else if(i % 10 == 8) cmd = "get";
				else cmd = "put";
			} else {
				if(i % 10 == 9) cmd = "remove";
				else if(i % 10 == 8) cmd = "put";
				else cmd = "get";
			}

			if(cmd.equals("put")) {
				String key = randomString(key_len);
				String val = randomString(val_len);
				keys.add(key);
				ops.add(cmd+" "+key+" "+val);
			} else {
				if(keys.size() == 0) continue;
				List<String> tmp = new ArrayList<>(keys);
				int ind = rand.nextInt(tmp.size());
				String key = tmp.get(ind);
				ops.add(cmd+" "+key);
				if(cmd.equals("remove")) keys.remove(key);
			}
		}

		try {
			FileWriter w = new FileWriter(opFile, append);
			for(int i = 0; i < ops.size(); i++) {
				w.write(ops.get(i) + "\n");
			}

			w.close();
		} catch(Exception e) {
		}
	}

	public static void initMap(LWWMap store, int max) {
		TimeStamp ts = new TimeStamp(store.uid, -1);
		for(int i = 0; i < max; i++) {
			String key = String.valueOf(i);
			String val = randomString(3);
			Entry e = new Entry(key, val, ts);
			store.A.put(key, e);
		}
	}


	public static HashMap<String, String> initMap(long max) {
		HashMap<String, String> map = new HashMap<>();
		for(int i = 0; i < max; i++) {
			map.put(String.valueOf(i), Util.randomString(3));
		}
		return map;
	}

	public static String randomString(int len) {
		StringBuilder sb = new StringBuilder();

		for(int i = 0; i < len; i++) {
			sb.append('0' + rand.nextInt(10));
		}

		return sb.toString();

	}

	public static int decideServer(int n) {
		if(Util.DELAY) return rand.nextInt(n - 1);
		else return rand.nextInt(n);
	}

	public static byte[] toByteArray(String str) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			dos.writeBytes(str);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return baos.toByteArray();
	}

}
