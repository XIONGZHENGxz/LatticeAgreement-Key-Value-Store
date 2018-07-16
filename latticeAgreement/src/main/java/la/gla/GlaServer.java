package la.gla;

import java.lang.Runnable;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import la.crdt.LWWMap;
import la.common.TcpListener;
import la.crdt.LWWMap;
import la.common.Op;
import la.common.Server;
import la.common.Response;
import la.common.Request;
import la.common.Util;
import la.crdt.TimeStamp;
import la.crdt.Entry;


public class GlaServer extends Server{

	public LWWMap store;
	public ReentrantLock lock_put;
	public ReentrantLock lock_rm;
	public GLAAlpha gla;
	public String[] check;
	public ReentrantLock lock;
	public Condition learnt;
	public int f;
	public int exeInd; //executed operation index
	public TcpListener l;
	public Set<Op> log; //executed ops

	public GlaServer(int id, int f) {
		super(id);
		this.store = new LWWMap(id);

		this.exeInd = -1;
		this.f = f;
		this.log = new HashSet<>();
		l = new TcpListener(this, this.port);
		lock_put = new ReentrantLock();
		lock_rm = new ReentrantLock();
		this.check = new String[2];
		lock = new ReentrantLock();
		learnt = lock.newCondition();
		gla = new GLAAlpha(this);
		l.start();
	}

	public void close() {
		while(!l.serverSocket.isClosed()) {
			if(Util.DEBUG) System.out.println("trying to close socket...");
			try {
				l.serverSocket.close();
			} catch (Exception e) {
				break;
			}
		}

		while(!gla.l.serverSocket.isClosed()) {
			if(Util.DEBUG) System.out.println("trying to close socket...");
			try {
				gla.l.serverSocket.close();
			} catch (Exception e) {
				break;
			}
		}
	}

	public Response handleRequest(Object obj) {
		Request request = (Request) obj;	
		Op req = request.op;

		if (req.type.equals("checkComp")) {
			return new Response(true, this.gla.LV);
		} else {
			if(req.type.equals("get")) return this.get(req.key);
			else if(req.type.equals("put") || req.type.equals("remove")) {
				this.executeUpdate(req);
				return new Response(true, "");
			}
			else System.out.println("invalid operation!!!");
		}
		return null;
	}

	public Response get(String key) {
		Response res = new Response(false, "");
		Random rand = new Random();
		String kid = String.valueOf(rand.nextInt(Integer.MAX_VALUE));
		Op noop = new Op("noop", kid, "");
		this.executeUpdate(noop);
		String val = this.store.get(key);
		if(val != null) {
			res.ok = true;
			res.val = val;
		}
		return res;
	}

	public void apply(int seq) {
		for(int i = this.exeInd + 1; i <= seq; i++) {
			for(Op o : this.gla.learntVal(i)) {
				if(this.log.contains(o)) continue;
				if(o.type.equals("put")) this.put(o.key, o.val);
				else if(o.type.equals("remove")) this.remove(o.key);
				this.log.add(o);
			}
		}
		this.exeInd = seq;
	}


	public void executeUpdate(Op op)  {
		try { 
			lock.lock();
			this.gla.receiveClient(op);
			while(!this.gla.learntValues.contains(op)) {
				if(Util.DEBUG) System.out.println("execution waiting");
				learnt.await();
			} 

			//execute ops 
			int currSeq = this.gla.seq - 1;
			this.apply(currSeq);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}

		if(Util.DEBUG) System.out.println("complete execution of " + op);
	}

	public boolean check() {
		boolean res = false;
		try {
			lock.lock();
			System.out.println("checking locked...");
			if(this.check[0] == null) {
				lock.unlock();
				return false;
			}
			System.out.println("inside checking...");
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

	public Response put(String key, String val) {
		try {
			lock_put.lock();
			this.store.put(key, val);
		} finally {
			lock_put.unlock();
		}
		return new Response(true, "");
	}

	public Response remove(String key) {
		try {
			lock_rm.lock();
			this.store.remove(key);
		} finally {
			lock_rm.unlock();
		}
		return new Response(true, "");
	}
	
	public void init(long max) {
		TimeStamp ts = new TimeStamp(this.me, Util.clock);
		for(int i = 0; i < max; i++) {
			String key = String.valueOf(i);
			this.store.A.put(key, new Entry(key, Util.randomString(3), ts));
		}
	}

	public static void main(String...args) {
		int id = Integer.parseInt(args[0]);
		int f = Integer.parseInt(args[1]);	
		long max = Long.parseLong(args[2]);

		GlaServer s = new GlaServer(id, f);
		s.init(max);
	}

}




