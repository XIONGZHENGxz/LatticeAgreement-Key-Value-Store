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
import la.common.Type;
import la.common.Server;
import la.common.Response;
import la.common.Result;
import la.common.Request;
import la.common.Util;
import la.crdt.TimeStamp;
import la.crdt.Entry;

import la.network.*;

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
	public SocketAcceptor socketAcceptor;
	public Map<Op, Socket> requests; 
	public Map<String, Set<Op>> reads;

	public GlaServer(int id, int f, String config, boolean fail) {
		super(id, config, fail);
		this.store = new LWWMap(id);
		this.rand = new Random();

		this.exeInd = -1;
		this.socketAcceptor = new SocketAcceptor(this, this.port);
		this.f = f;
		this.log = new HashSet<>();
		//this.writeResponder = new WriteResponder(this);
		//this.readResponder = new ReadResponder(this);
		this.reads = new HashMap<>();
		this.requests = new HashMap<>();
		lock_put = new ReentrantLock();
		lock_rm = new ReentrantLock();
		wlock = new ReentrantLock();
		rlock = new ReentrantLock();
		wcond = wlock.newCondition();
		rcond = rlock.newCondition();
		reqcond = rlock.newCondition();
		gla = new GLAAlpha(this);
		this.socketAcceptor.start();
		//writeResponder.start();
		//readResponder.start();
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
		if(obj == null) return null;
		Op req = (Op) obj;
		//System.out.println(this.me + " get request from client: "+ req);

		//	if (req.type.equals("checkComp")) {
		//		return new Response(true, this.gla.LV);
		//	} else if(req.type.equals("down")){
		//		this.l.fail = true;
		//		this.gla.l.fail = true;
		//	} else {
		if(Util.DEBUG) System.out.println(this.me +" get request from client..." + req);
		//if(req != null) return new Response(Result.TRUE, "");
		if(req.type == Type.GET) {
			String kid = this.me + "" + this.gla.seq;
			Op noop = new Op(Type.GET, kid, "");
			//if(!this.reads.containsKey(kid)) reads.put(kid, new HashSet<Op>());
			//this.gla.receiveRead(noop);	
			this.executeUpdate(noop, true);
			return this.get(req.key);
		}
		else if(req.type == Type.PUT || req.type == Type.REMOVE) {
			this.executeUpdate(req, false);
			return new Response(Result.TRUE, "");
		}
		//	}
		return null;
	}
	
	public void executeUpdate(Op op, boolean read) {
		if(read) {
			this.read(op);
		} else this.write(op);
	}

	public void read(Op op) {
		try {
			this.rlock.lock();
			this.gla.receiveRead(op);
			while(this.gla.readBuffVal.contains(op)) {
				this.rcond.await();
			}
		} catch (Exception e) {
		} finally {
			this.rlock.unlock();
		}
	}

	public void write(Op op) {
		try {
			this.wlock.lock();
			this.gla.receiveWrite(op);
			while(this.gla.writeBuffVal.contains(op)) {
				this.wcond.await();
			}
		} catch (Exception e) {
		} finally {
			this.wlock.unlock();
		}
	}

	public Response get(String key) {
		Response res = new Response(Result.FALSE, "");

		String val = this.store.get(key);
		if(val != null) {
			res.ok = Result.TRUE;
			res.val = val;
		}
		return res;
	}

	public void apply(int seq) {
		for(int i = this.exeInd + 1; i <= seq; i++) {
			for(Op o : this.gla.learntVal(i)) {
				Set<Op> prev = this.gla.learntVal(i - 1);
				if(prev.contains(o)) continue;
				if(o.type == Type.PUT) {
					this.put(o.key, o.val);
				else if(o.type == Type.REMOVE) this.remove(o.key);
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
		return new Response(Result.TRUE, "");
	}

	public Response remove(String key) {
		try {
			lock_rm.lock();
			this.store.remove(key);
		} finally {
			lock_rm.unlock();
		}
		return new Response(Result.TRUE, "");
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



