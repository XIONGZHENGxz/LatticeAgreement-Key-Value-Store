package la.wgla;

import java.lang.Runnable;
import java.util.Random;
import java.util.List;
import java.util.Queue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.net.Socket;

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
	public ReentrantLock wlock;
	public ReentrantLock rlock;
	public ReentrantLock reqLock;
	public Condition reqcond;
	public Condition wcond;
	public Condition rcond;
	public int f;
	public int exeInd; //executed operation index
	public TcpListener l;
	public Set<Op> log; //executed ops
	public ReentrantLock applock;
	public Set<Op> previous;
	public Random rand;
	public Map<Op, Socket> requests; 
	public WriteResponder writeResponder;
	public ReadResponder readResponder;
	public Queue<Op> writeQueue;
	public Queue<Op> readQueue;
	public Map<String, Set<Op>> reads;

	public GlaServer(int id, int f, String config, boolean fail) {
		super(id, config, fail);
		this.store = new LWWMap(id);
		this.rand = new Random();

		this.exeInd = -1;
		this.f = f;
		this.log = new HashSet<>();
		this.writeResponder = new WriteResponder(this);
		this.readResponder = new ReadResponder(this);
		this.writeQueue = new ConcurrentLinkedQueue<>();
		this.readQueue = new ConcurrentLinkedQueue<>();
		this.reads = new HashMap<>();
		this.requests = new HashMap<>();
		l = new TcpListener(this, this.port);
		lock_put = new ReentrantLock();
		lock_rm = new ReentrantLock();
		wlock = new ReentrantLock();
		rlock = new ReentrantLock();
		wcond = wlock.newCondition();
		rcond = rlock.newCondition();
		reqcond = rlock.newCondition();
		gla = new GLAAlpha(this);
		l.start();
		writeResponder.start();
		readResponder.start();
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

	public Response handleRequest(Object obj, Socket socket) {
		if(obj == null) return null;
		Request request = (Request) obj;	
		Op req = request.op;
		//System.out.println(this.me + " get request from client: "+ req);

		//	if (req.type.equals("checkComp")) {
		//		return new Response(true, this.gla.LV);
		//	} else if(req.type.equals("down")){
		//		this.l.fail = true;
		//		this.gla.l.fail = true;
		//	} else {
		requests.put(req, socket);
		if(req.type.equals("get")) {
			String kid = this.me + "" + this.gla.seq;
			Op noop = new Op("noop", kid, "");
			if(!this.reads.containsKey(kid)) reads.put(kid, new HashSet<Op>());
			reads.get(kid).add(req);
			this.gla.receiveRead(noop);	
		}
		else if(req.type.equals("put") || req.type.equals("remove")) {
			this.gla.receiveWrite(req);
		}
		else System.out.println("invalid operation!!!");
		//	}
		return null;
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

	public void apply(int seq) {
		for(int i = this.exeInd + 1; i <= seq; i++) {
			for(Op o : this.gla.learntVal(i)) {
				Set<Op> prev = this.gla.learntVal(i - 1);
				if(prev.contains(o)) continue;
				if(o.type.equals("put")) this.put(o.key, o.val);
				else if(o.type.equals("remove")) this.remove(o.key);
			}
		}
		this.exeInd = seq;
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



