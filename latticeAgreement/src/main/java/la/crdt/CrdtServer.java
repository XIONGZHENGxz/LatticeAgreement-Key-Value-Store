package la.crdt;

import java.lang.Runnable;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import la.common.Server;
import la.common.Util;
import la.common.Response;
import la.common.Request;
import la.common.Messager;
import la.common.TcpListener;
import la.common.Messager;
import la.common.Op;

public class CrdtServer extends Server implements Runnable {

	public List<Integer> nbs;
	public LWWMap store;
	public ReentrantLock lock;
	public ReentrantLock lock_up;
	public ReentrantLock lock_put;
	public ReentrantLock lock_rm;
	public boolean update;
	public String[] check;
	public int k;
	public int freq;
	public HashMap<String, Integer> a_count;
	public HashMap<String, Integer> r_count;

	public CrdtServer(int id, String config, int freq, boolean fail) {
		super(id, config, fail);
		this.freq = freq;

		this.nbs = new ArrayList<>();
		for(int i = 0; i < this.peers.size(); i++) {
			if(i != id) this.nbs.add(i);
		}

		this.store = new LWWMap(id);
		TcpListener c = new TcpListener(this, this.port);
		TcpListener l = new TcpListener(this, this.ports.get(id));
		update  = false;
		this.k = 7;
		lock = new ReentrantLock();
		lock_up = new ReentrantLock();
		lock_put = new ReentrantLock();
		lock_rm = new ReentrantLock();
		a_count = new HashMap<>();
		r_count = new HashMap<>();
		this.check = new String[2];
		l.start();
		c.start();
	}

	public Response handleRequest(Object obj) {
		CrdtRequest req = (CrdtRequest) obj;
		if(req.type.equals("merge")) return this.merge(req.map, req.logic_time);
		else if(req.type.equals("check")) { 
			this.check[0] = req.op.key;	
			this.check[1] = req.op.val;
			if(Util.DEBUG) System.out.println(check[0]);
			if(this.check()) this.sendCheck();
		} else {
			Op op = req.op;
			if(op.type.equals("get")) return this.get(op.key);
			else if(op.type.equals("put")) return this.put(op.key, op.val);
			else if(op.type.equals("remove")) return this.remove(op.key);
			else System.out.println("invalid operation!!!");
		}
		return null;
	}

	public void sync() {
		try {
			lock_up.lock();
			lock_put.lock();
			lock_rm.lock();
			CrdtRequest req = new CrdtRequest("merge", this.store, Util.clock);
			for(int i = 0; i < this.nbs.size(); i++){
				int nb = this.nbs.get(i);
				while(true) {	
					boolean sent  = Messager.sendMsg(req, this.peers.get(nb), this.ports.get(nb));
					if(sent) {
						if(Util.DEBUG) System.out.println("sent merge to "+this.nbs.get(i));
						break;
					}
				}
			}
		} finally {
			lock_up.unlock();
			lock_put.unlock();
			lock_rm.unlock();
		}
	}

	public void run() {
		int counter = this.k;
		int syncCount = 0;
		while(true) {
			syncCount ++;
			try {
				Thread.sleep(Util.interval);
			} catch (Exception e) {
			}
			if(syncCount % Util.syncFreq == 0) {
				sync();
				continue;
			}
			try {
				lock_up.lock();
				if(!update && counter <= 0) {
					continue;
				} else if(update) {
					counter = this.k;
				}
				if(this.nbs.size() == 0) return;

				Random rand = new Random();
				HashMap<String, Entry> tem_a = new HashMap<>();
				HashMap<String, TimeStamp> tem_r = new HashMap<>();
				try {
					lock_put.lock();
					for(String key: this.store.A.keySet()) {
						int c_a = a_count.getOrDefault(key, 0);
						if(c_a < Util.freq) {
							a_count.put(key, c_a + 1);
							tem_a.put(key, this.store.A.get(key));
						}
					}
				} finally {
					lock_put.unlock();
				}

				try {
					lock_rm.lock();
					for(String key: this.store.R.keySet()) {
						int c_r = r_count.getOrDefault(key, 0);
						if(c_r < Util.freq) {
							r_count.put(key, c_r + 1);
							tem_r.put(key, this.store.R.get(key));
						}
					}
				} finally {
					lock_rm.unlock();
				}

				CrdtRequest req = new CrdtRequest("merge", new LWWMap(tem_a, tem_r), Util.clock);

				while(true) {	
					int s = rand.nextInt(this.nbs.size());
					int nb = this.nbs.get(s);
					boolean sent  = Messager.sendMsg(req, this.peers.get(nb), this.ports.get(nb));
					if(sent) {
						if(Util.DEBUG) System.out.println("sent merge to "+ nb);
						break;
					}
				}
				update = false;

				counter --;
			} finally {
				lock_up.unlock();
			}
		}
	}

	public void sendMerge () {
		int counter = 0;
		try {
			lock_up.lock();
			if(!update && counter <= 0) {
				return;
			} else if(update) {
				counter = this.k;
			}
			if(this.nbs.size() == 0) return;

			Random rand = new Random();
			HashMap<String, Entry> tem_a = new HashMap<>();
			HashMap<String, TimeStamp> tem_r = new HashMap<>();
			try {
				lock_put.lock();
				for(String key: this.store.A.keySet()) {
					int c_a = a_count.getOrDefault(key, 0);
					if(c_a < Util.freq) {
						a_count.put(key, c_a + 1);
						tem_a.put(key, this.store.A.get(key));
					}
				}
			} finally {
				lock_put.unlock();
			}

			try {
				lock_rm.lock();
				for(String key: this.store.R.keySet()) {
					int c_r = r_count.getOrDefault(key, 0);
					if(c_r < Util.freq) {
						r_count.put(key, c_r + 1);
						tem_r.put(key, this.store.R.get(key));
					}
				}
			} finally {
				lock_rm.unlock();
			}

			CrdtRequest req = new CrdtRequest("merge", new LWWMap(tem_a, tem_r), Util.clock);

			while(true) {	
				int s = rand.nextInt(this.nbs.size());
				int nb = this.nbs.get(s);
				boolean sent  = Messager.sendMsg(req, this.peers.get(nb), this.ports.get(nb));
				if(sent) {
					if(Util.DEBUG) System.out.println("sent merge to "+this.nbs.get(s));
					break;
				}
			}
			update = false;

			counter --;
		} finally {
			lock_up.unlock();
		}
	}

	public Response get(String key) {
		Response res = new Response(false, "");
		String val = this.store.get(key);
		if(val != null) {
			res.ok = true;
			res.val = val;
		}
		return res;
	}

	public boolean check() {
		boolean res = false;
		try {
			lock.lock();
			//System.out.println("checking locked...");
			if(this.check[0] == null) {
				lock.unlock();
				return false;
			}
			//System.out.println("inside checking...");
			if(this.check[1].equals("-1")) {
				String val = this.store.get(check[0]);
				if(Util.DEBUG) System.out.println("check remove "+ check[0]+ " "+val);
				res = (val == null);
			}
			else {
				String val = this.store.get(check[0]);
				res = ( val != null && val.equals(this.check[1]) );
			}
			//System.out.println("checking result "+ res);
			if(res) this.check = new String[2];
		} finally {
			lock.unlock();
		}
		return res;
	}

	public void sendCheck() {
		Request req = new Request("check_result");
		while(true) {
			boolean sent  = Messager.sendMsg(req, Util.checker_host, Util.c_port);
			if(sent) break;
		}
		if(Util.DEBUG) System.out.println("check ok for "+ this.check[0]);
	}

	public Response put(String key, String val) {
		try {
			lock_up.lock();
			lock_put.lock();
			this.store.put(key, val);
			this.update = true;
			a_count.put(key, 0);
			//this.sendMerge();
		} finally {
			lock_put.unlock();
			lock_up.unlock();
		}
		return new Response(true, "");
	}

	public Response remove(String key) {
		try {
			lock_up.lock();
			lock_rm.lock();
			this.store.remove(key);
			this.update = true;
			r_count.put(key, 0);
			this.sendMerge();
		} finally {
			lock_rm.unlock();
			lock_up.unlock();
		}
		return new Response(true, "");
	}

	public Response merge(LWWMap remote, int clock) {	
		try {
			lock_up.lock();
			boolean temp_up = this.store.merge(remote);
			if(clock > Util.clock) Util.clock = clock;
			if(temp_up && this.check[0] != null && this.check()) { 
				if(Util.DEBUG) System.out.println("check successful...");
				this.sendCheck(); 
			}
			if(temp_up) this.update = true;
		} finally {
			lock_up.unlock();
		}
		return new Response(true, "");
	}

	public static void main(String...args) {
		int id = Integer.parseInt(args[0]);
		int max = Integer.parseInt(args[1]);
		int freq = Integer.parseInt(args[2]);
		boolean fail = args[4].equals("f") ? true : false;
		CrdtServer s = new CrdtServer(id, args[3], freq, fail);

		Thread t = new Thread(s);
		Util.initMap(s.store, max);

		for(int i = 0; i < max; i++) {
			s.a_count.put(String.valueOf(i), freq);
		}
		t.start();
	}

}




