package la.pgla;

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
	public ReentrantLock lock;
	public Condition learnt;
	public int f;
	public int exeInd; //executed operation index
	public TcpListener l;
	public Set<Op> log; //executed ops
	public ReentrantLock applock;
	public Set<Op> previous;

	public GlaServer(int id, int f, String config, boolean fail) {
		super(id, config, fail);
		this.store = new LWWMap(id);

		this.exeInd = -1;
		this.f = f;
		this.log = new HashSet<>();
		l = new TcpListener(this, this.port);
		lock_put = new ReentrantLock();
		lock_rm = new ReentrantLock();
		lock = new ReentrantLock();
		applock = new ReentrantLock();
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
		//System.out.println(this.me + " get request from client: "+ req);

		if (req.type.equals("checkComp")) {
			return new Response(true, this.gla.LV);
		} else if(req.type.equals("down")){
			this.l.fail = true;
			this.gla.l.fail = true;
		} else {
			if(req.type.equals("get")) {
				return this.get(req.key);
			}
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
				Set<Op> prev = this.gla.learntVal(i - 1);
				if(prev.contains(o) || this.log.contains(o)) continue;
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
			if(this.gla.receiveClient(op)) return;
			if(Util.DEBUG) System.out.println(this.me + " waiting for "+op);
			while(this.gla.buffVal.contains(op)) {
				learnt.await();
			} 

			//execute ops 
			//int currSeq = this.gla.seq - 1;
			//this.apply(currSeq);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}

		if(Util.DEBUG) System.out.println("complete execution of " + op);
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
		String config = args[3];
		boolean fail = args[4].equals("f") ? true : false;

		GlaServer s = new GlaServer(id, f, config, fail);
		s.init(max);
	}

}




