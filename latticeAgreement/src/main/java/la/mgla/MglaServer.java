package la.mgla;

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
import la.common.UdpListener;
import la.crdt.LWWMap;
import la.common.Op;
import la.common.Server;
import la.common.Response;
import la.common.Request;
import la.common.Util;
import la.crdt.TimeStamp;
import la.crdt.Entry;

	
public class MglaServer extends Server{

	public LWWMap store;

	public ReentrantLock lock_put;
	public ReentrantLock lock_rm;

	public Proposer proposer;

	public Acceptor acceptor;

	public String[] check;

	public ReentrantLock lock;
	public Condition learnt;

	public int exeInd; //executed operation index

	public Set<Op> log; //executed ops

	public TcpListener tcp;

	public UdpListener udp;

	public MglaServer(int id, String config, boolean fail) {
		super(id, config, fail);
		this.store = new LWWMap(id);
		this.exeInd = -1;
		this.log = new HashSet<>();
		TcpListener tcp = new TcpListener(this, this.port);
		UdpListener udp = new UdpListener(this, this.ports.get(id));

		lock_put = new ReentrantLock();
		lock_rm = new ReentrantLock();
		this.check = new String[2];
		lock = new ReentrantLock();
		learnt = lock.newCondition();

		proposer = new Proposer(this);
		acceptor = new Acceptor(this);
		tcp.start();
		udp.start();
	}

	public void close() {
		while(!tcp.serverSocket.isClosed()) {
			if(Util.DEBUG) System.out.println("trying to close socket...");
			try {
				tcp.serverSocket.close();
			} catch (Exception e) {
				break;
			}
		}

		while(!udp.serverSocket.isClosed()) {
			if(Util.DEBUG) System.out.println("trying to close socket...");
			try {
				udp.serverSocket.close();
			} catch (Exception e) {
				break;
			}
		}
	}

	public Response handleRequest(Object obj) {
		if(obj == null) return null;
		Request request = (Request) obj;	
		if(Util.DEBUG) System.out.println("get request: "+ request);

		if(request.type.equals("proposal")) 
			this.acceptor.handleProposal(request);
		else if(request.type.equals("down")) {
			this.tcp.fail = true;
			this.udp.fail = true;
		} else if(request.type.equals("client")) {
			Op req = request.op;
			if(req.type.equals("checkComp")) 
				return null;
			else if(req.type.equals("get")) return this.get(req.key);
			else if(req.type.equals("put") || req.type.equals("remove")) {
				this.executeUpdate(req);
				return new Response(true, "");
			}
		} else 
			this.proposer.handleRequest(request);

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

	public void apply() {
		synchronized (this.proposer.learntValues) {
		for(Op o : this.proposer.learntValues) {
			if(this.log.contains(o)) continue;
			if(o.type.equals("put")) this.put(o.key, o.val);
			else if(o.type.equals("remove")) this.remove(o.key);
			this.log.add(o);
		}
		}
	}


	public void executeUpdate(Op op)  {
		try { 
			lock.lock();
			this.proposer.receiveClient(op);
			while(!this.proposer.learntValues.contains(op)) {
				if(Util.DEBUG) System.out.println("execution waiting");
				learnt.await();
			} 
			this.apply();
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
		long max = Long.parseLong(args[1]);
		boolean fail = args[3].equals("f") ? true : false;

		MglaServer s = new MglaServer(id, args[2], fail);
		s.init(max);
	}

}




