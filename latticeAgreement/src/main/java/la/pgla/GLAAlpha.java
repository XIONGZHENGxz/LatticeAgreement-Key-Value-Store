package la.pgla;

import java.util.Set;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.ConcurrentHashMap;

import la.common.UdpListener;
import la.common.Op;
import la.common.Messager;
import la.common.Util;
import la.common.Server;

import la.common.Request;
import la.common.Response;
import la.common.Server;
import la.gla.LearntRequest;

public class GLAAlpha extends Server implements Runnable {

	public int seq; //next available sequence number 
	public int r; //round number 
	public Set<Op> buffVal; //values need to be learned
	public ConcurrentHashMap<Integer, Set<Op>> LV; //sequence number to learned values mapping
	public Set<Op> acceptVal; //current accept value
	public Set<Op> oldAccept; //current accept value
	public boolean active; //proposing or not
	public UdpListener l; 
	public volatile int tally; //number of accept acks
	public int n; //# peers
	public Set<Op> learntValues; //history of learned values
	public GlaServer s; 
	public Set<Integer> received; 
	public int minSeq; 
	public int[] learntSeq; //largest learned sequence number for each peer
	public ReentrantLock lock;
	public ReentrantLock ltLock;
	public Map<Integer, Set<Op>> decided;
	public List<int[]> maxSeq; //store max seq seen for each peer
	public int max_seq;
	public Request[] maxProp;
	public Set<Integer> reqs;
	public Map<Integer, Boolean> changed;

	public GLAAlpha (GlaServer s) {
		super(s.me, s.peers, s.port, s.ports);

		this.seq = 0;
		this.max_seq = 0;
		learntValues = ConcurrentHashMap.newKeySet();

		this.lock = new ReentrantLock();
		this.ltLock = new ReentrantLock();
		tally = 0;
		this.s = s;
		this.reqs = new HashSet<>();
		this.received = new HashSet<>();
		this.changed = new HashMap<>();
		this.minSeq = -1;

		this.n = this.s.peers.size(); 

		this.learntSeq = new int[n];
		this.maxSeq = new ArrayList<>();
		this.maxProp = new Request[this.n];
		Arrays.fill(learntSeq, -1);
		Arrays.fill(maxProp, new Request("", null, -1, -1, -1));
		for(int i = 0; i < n; i++) this.maxSeq.add(new int[]{-1, -1});

		this.lock = new ReentrantLock();

		decided = new ConcurrentHashMap<>();
		buffVal = ConcurrentHashMap.newKeySet();
		LV = new ConcurrentHashMap<>();
		LV.put(-1, new HashSet<Op>());
		LV.put(0, new HashSet<Op>());
		acceptVal = ConcurrentHashMap.newKeySet();
		this.active = false;
		l = new UdpListener(this, this.ports.get(this.me));
		l.start();
		this.minSeq = Integer.MAX_VALUE;
		this.r = 0;
		this.oldAccept = ConcurrentHashMap.newKeySet();
	}

	public void minLearntSeq() {
		int minId = -1;
		int prev = this.minSeq;
		for(int i = 0; i < n; i++) {
			if(learntSeq[i] < this.minSeq) {
				this.minSeq = learntSeq[i];
				minId = i;
			}
		}
	}

	public void start() {
		Set<Op> val = new HashSet<>();
		this.oldAccept = ConcurrentHashMap.newKeySet();
		for(Op o : this.acceptVal) {
			oldAccept.add(o);
			val.add(o);
		}

		for(Op o : this.buffVal) {
			this.acceptVal.add(o);
			val.add(o);
		}
		this.seq ++;
		System.out.println(this.s.me + " start seq: " + this.seq );
		this.handleAllProp();
		
		for(this.r = 0; this.r < this.s.f + 1; this.r ++) {
			received = new HashSet<>();
			if(Util.DEBUG) System.out.println(this.me + " propose " + val.toString());
			Request req = new Request("proposal", val, this.LV.get(this.seq - 1), this.r, this.seq, this.me);
			this.tally = 1;
			this.broadCast(req);
			int loop = 0;
			int want = 0;
			if(this.r == 0) want = this.n - this.s.f - 1;
			else want = n / 2;

			while(this.received.size() < want && !this.decided.containsKey(this.seq)) {
				if(Util.DEBUG) System.out.println(this.me + " waiting for ack received :"+ " "+ this.received + " active +" +this.active + " seq "+ this.seq + " round "+ this.r);
				loop ++;

				if(loop % 4 == 0) this.broadCast(req, this.received);
				try {
					Thread.sleep(6);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if(Util.DEBUG) System.out.println(this.me + " got n - f acks");
				
			/* get any decide message, take join of all decided values */
			if(this.r < this.s.f && this.decided.containsKey(this.seq)) {
				if(Util.DEBUG) System.out.println(this.me +" got decide messages");
				val = this.decided.get(this.seq);
				break;
			} else if(this.r < this.s.f && this.tally > this.n / 2) 
				break;
			else { 
				if(Util.DEBUG) System.out.println(this.me + " reject by majority...");
				//synchronized (this.acceptVal) {
				for(Op o : this.acceptVal) val.add(o);
				if(received.size() == this.n - 1) break;
				//}
			}
		}

		//if(val.size() == 0) return;

		LV.put(this.seq , val);

		if(Util.DEBUG) System.out.println(this.me + " complete seq " + this.seq + " " + val);
		this.acceptVal.removeAll(oldAccept);

		this.buffVal.removeAll(val);

		this.s.apply(this.seq);

	//	this.sleep(2);

		this.wake();
	}

	public void sleep(int t) {
		try {
			Thread.sleep(t);
		} catch (Exception e) {}
	}

	public void run() {
		try {
			lock.lock();
			synchronized (this) {
				this.active = true;
			}

			while(true) { 
				this.seq = this.catchUp();
				this.start();
				if(this.buffVal.size() == 0) break;
			}
			this.active = false;
		} finally {
			this.active = false;
			lock.unlock();
		}
	}

	public void sendLearnt(int seq, Set<Op> val) {
		Request learntReq = new Request("decided", val, seq, this.me);
		for(int i = 0; i < this.n; i++) {
			if(i == this.me) continue;
			int[] tmp = this.maxSeq.get(i);
			if(tmp[0] >= this.seq) continue;
			this.sendUdp(learntReq, i);
		}
	}

	public void broadCast(Request req, Set<Integer> ignore) {
		for(int i = 0; i < this.n; i++) {
			if(i == this.me || ignore.contains(i)) continue;
			this.sendUdp(req, i);
		}
	}

	public void wake() {
		try {
			this.s.lock.lock();
			this.s.learnt.signalAll();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.s.lock.unlock();
		}
	}

	public void broadCast(Request req) {
		for(int i = 0; i < this.n; i++) {
			if(i == this.me) continue;
			this.sendUdp(req, i);
		}
	}

	public boolean receiveClient(Op op) {
		return this.receive(op);
	}

	public void receive(Set<Op> val) {
		this.buffVal.addAll(val);
		if(!this.active) {
			Thread t = new Thread(this);
			t.start();
		}
	}

	public boolean receive(Op op) {
		//	if(this.learntValues.contains(op)) return true;
		//the agree procedure may break while loop, but not check this.active yet.
		this.buffVal.add(op);
		if(!this.active) {
			Thread t = new Thread(this);
			t.start();
		}
		return false;
	}

	public void sendUdp(Request req, int peer) {
		Messager.sendPacket(req, this.s.peers.get(peer), this.ports.get(peer));
	}
	
	public void catchUp(int s) {
		while(this.seq < s) {
			this.seq ++;
			//this.handleAllProp();
			if(this.decided.containsKey(this.seq)) {
				this.LV.put(this.seq, this.decided.get(this.seq));
			} else { 
				this.LV.put(this.seq, new HashSet<Op>());
			}
			this.acceptVal.removeAll(this.LV.get(this.seq - 1));
		}
		this.handleAllProp();
	}
			
	public int catchUp() {
		int currSeq = this.seq + 1;
		while(this.decided.containsKey(currSeq)) {
			Set<Op> tmpVal = this.decided.get(currSeq);
			this.acceptVal.removeAll(this.LV.get(currSeq - 1));
			//tmpVal.removeAll(this.LV.get(this.seq - 1));
			this.buffVal.removeAll(tmpVal);
			//this.learntValues.addAll(tmpVal);
			this.LV.put(currSeq ++, tmpVal);
		}
		return currSeq - 1;
	}

	public void handleAllProp() {
		for(int i = 0; i < this.n; i++) {
			if(i == this.me) continue;
			Request tmpReq = this.maxProp[i];
			if(tmpReq.seq == this.seq) {
				this.handleProp(tmpReq);
			}
		}
	}

	public void handleProp(Request req) {
		Set<Op> tmpAcc;  
		synchronized (this.acceptVal) {
			tmpAcc = new HashSet<>(this.acceptVal);
		}

		if(req.round > 0 && req.round < this.s.f && req.val.size() >= tmpAcc.size() && req.val.containsAll(tmpAcc)) {
			Request resp = new Request("accept", null, req.round, req.seq, this.me);
			this.sendUdp(resp, req.me);
		} else {
			Request resp = null;
			if(req.round == 0)
				resp = new Request("reject", tmpAcc, this.oldAccept, req.round, req.seq, this.me);
			else 
				resp = new Request("reject", tmpAcc, req.round, req.seq, this.me);
			this.sendUdp(resp, req.me);
		}
		this.acceptVal.addAll(req.val);
	}

	public Response handleRequest(Object obj) {
		Request req = (Request) obj;
		if(Util.DEBUG) System.out.println("get request "+req);

		if(req.type.equals("proposal")) {
			if(req.seq < this.seq) {
				req.val.removeAll(this.LV.get(req.seq));
				this.receive(req.val);
				Request resp = new Request("decided", this.LV.get(req.seq), req.round, req.seq, this.me);
				this.sendUdp(resp, req.me);
			} 
			else if(req.seq == this.seq) {
				this.handleProp(req);
			} 
			else {
				Request tmpProp = this.maxProp[req.me];
				if(tmpProp.seq < req.seq ||
						(tmpProp.seq == req.seq && tmpProp.round < req.round)) 
					this.maxProp[req.me] = req;
				
				if(!this.decided.containsKey(req.seq - 1)) {
					this.decided.put(req.seq - 1, req.learnt);
				}
				
				synchronized (this) {
				if(!this.active) {
					this.catchUp(req.seq);
				}
				}
			}
			return null;
		} else if(req.type.equals("decided")) {
			if(!this.decided.containsKey(req.seq))
				this.decided.put(req.seq, req.val);
		} else if(req.type.equals("reject")) {
			if(req.seq == this.seq && req.round == this.r) {
				this.acceptVal.addAll(req.val);
				if(req.learnt != null) this.oldAccept.addAll(req.learnt); 
				this.received.add(req.me);
			}
		} else if(req.type.equals("accept")) {
			if(req.seq == this.seq && req.round == this.r) {
				this.tally ++;
				this.received.add(req.me);
			}
		} else if(req.type.equals("getLearnt")) {
			int[] tmp = this.maxSeq.get(req.me);
			if(req.seq >= tmp[0] && req.seq < this.seq) {
				Request lr = new Request("decided", this.LV.get(req.seq), req.seq, this.me);
				this.sendUdp(lr, req.me);
			}
		}
		return null;
	}

	public Set<Op> learntVal(int Seq) {
		if(Seq < 0 || !this.LV.containsKey(Seq)) return new HashSet<>();
		return this.LV.get(Seq);
	}
}







