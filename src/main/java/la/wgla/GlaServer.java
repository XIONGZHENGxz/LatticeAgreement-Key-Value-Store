package la.wgla;

import java.lang.Runnable;
import java.util.Random;
import java.util.List;
import java.util.Queue;
import java.util.ArrayList;
import java.util.LinkedList;
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
import la.common.Message;
import la.common.Type;
import la.common.Server;
import la.common.Response;
import la.common.Result;
import la.common.Request;
import la.common.Util;
import la.common.Messager;
import la.crdt.TimeStamp;
import la.crdt.Entry;

import la.network.*;

public class GlaServer extends Server{

	public LWWMap store;
	public ReentrantLock lock_rm;
	public GLAAlpha gla;
	public ReentrantLock wlock;
	public ReentrantLock rlock;
	public ReentrantLock elock;
	public Condition wcond;
	public Condition rcond;
	public Condition econd;
	public int f;
	public int exeInd; //executed operation index
	public int rmInd; //executed operation index
	public TcpListener l;
	public Random rand;
	public SocketAcceptor socketAcceptor;
	public Map<String, Set<Op>> reads;
	public ReadExecutor[] readExecutor;
	public WriteExecutor[] writeExecutor;
	public Executor executor;

	public Queue<Message> outQueue; 
	public volatile boolean readable;
	public Set<Op> log;
	public Map<Op, Response> readRes;

	public GlaServer(int id, int f, String config, boolean fail) {
		super(id, config, fail);
		this.store = new LWWMap(id);
		this.rand = new Random();
		this.outQueue = new LinkedList<>();
		this.log = new HashSet<>();
		this.readRes = new HashMap<>();

		this.exeInd = -1;
		this.socketAcceptor = new SocketAcceptor(this, this.port);
		this.f = f;
		this.log = new HashSet<>();
		this.reads = new HashMap<>();
		lock_rm = new ReentrantLock();
		wlock = new ReentrantLock();
		rlock = new ReentrantLock();
		elock = new ReentrantLock();
		wcond = wlock.newCondition();
		rcond = rlock.newCondition();
		econd = elock.newCondition();
		gla = new GLAAlpha(this);
		this.readExecutor = new ReadExecutor[Util.writer];
		for(int i = 0; i < Util.writer; i++) {
			readExecutor[i] = new ReadExecutor(this);
			readExecutor[i].start();
		}
		this.executor = new Executor(this);
		this.executor.start();
		this.socketAcceptor.start();
	}

	public void close() {
		while(!socketAcceptor.serverSocket.isOpen()) {
			System.out.println("trying to close socket...");
			try {
				socketAcceptor.serverSocket.close();
			} catch (Exception e) {
				break;
			}
		}

		while(!gla.l.serverSocket.isClosed()) {
			System.out.println("trying to close socket...");
			try {
				gla.l.serverSocket.close();
			} catch (Exception e) {
				break;
			}
		}
	}

	public Response handleRequest(int socketId, Object obj) {
		if(obj == null) return null;
		Op req = (Op) obj;
		req.id = socketId;
	//	System.out.println("get request..." + req);
		if(req.type == Type.GET) {
			String kid = this.me + "" + this.gla.seq;
			Op noop = new Op(Type.GET, kid, "");
			if(!this.reads.containsKey(kid)) reads.put(kid, new HashSet<Op>());
			reads.get(kid).add(req);
			this.gla.receiveRead(noop);
			return null;
			/*
			this.read(noop);
			return this.get(req.key);
			*/
		}
		else if(req.type == Type.PUT || req.type == Type.REMOVE) {
			this.write(req);
			//this.gla.receiveWrite(req);
			return new Response(Result.TRUE, "");
		} else {
			this.fail = true;
			this.close();
			//System.exit(1);
			return new Response(Result.TRUE, "");
		}
	}


	public void read(Op op) {
		try {
			this.rlock.lock();
			this.gla.receiveWrite(op);
			while(this.gla.writeBuffVal.contains(op)) {
				this.rcond.await();
			}
			//this.apply(this.gla.seq);
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
			//System.out.println(this.me + " apply...." + i);
			for(Op o : this.gla.learntWrites(i)) {
				Set<Op> prev = this.gla.learntWrites(i - 1);
				if(prev.contains(o)) continue;
				this.put(o.key, o.val);
			}
			for (Op o : this.gla.learntReads(i)) {
				if(!this.reads.containsKey(o.key)) continue;
				for(Op read : this.reads.get(o.key)) {
					Response resp = this.get(read.key);
					int ind = read.id % readExecutor.length;
					readExecutor[ind].add(new Message(resp, socketAcceptor.socketMap.get(read.id)));
				}
			}
		}
		this.exeInd = seq;
		while(this.rmInd < this.exeInd - 1000) {
			this.gla.LV.remove(rmInd);
			this.gla.learntReads.remove(rmInd);
			this.gla.decided.remove(rmInd);
			this.rmInd ++;
		}
	}
	
	public void wakeResponder () {
		try {
			this.rlock.lock();
			this.rcond.signalAll();
		} catch (Exception e) {}
		finally {
			this.rlock.unlock();
		}
	}

	public Response put(String key, String val) {
		this.store.put(key, val);
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



