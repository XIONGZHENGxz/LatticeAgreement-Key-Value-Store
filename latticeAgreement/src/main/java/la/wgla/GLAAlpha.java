package la.wgla;

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

	public volatile int seq; //next available sequence number 
	public volatile int r; //round number 
	public Set<Op> buffVal; //values need to be learned
	public ConcurrentHashMap<Integer, Set<Op>> LV; //sequence number to learned values mapping
	public Set<Op> acceptVal; //current accept value
	public boolean active; //proposing or not
	public UdpListener l; 
	public Set<Op> decided; //values in decided messages  
	public Set<Op> rejected; //values in rejected messages 
	public volatile int tally; //number of accept acks
	public int n; //# peers
	public Set<Op> learntValues; //history of learned values
	public GlaServer s; 
	public Set<Integer> received; 
	public int minSeq; 
	public int[] learntSeq; //largest learned sequence number for each peer
	public ReentrantLock lock;
	public ReentrantLock ltLock;
	public int[] maxSeq; //store max seq seen for each peer
	public int max_seq;

	public GLAAlpha (GlaServer s) {
		super(s.me, s.peers, s.port, s.ports);

		this.seq = 0;
		this.max_seq = 0;
		learntValues = ConcurrentHashMap.newKeySet();

		this.lock = new ReentrantLock();
		this.ltLock = new ReentrantLock();
		tally = 0;
		this.s = s;
		this.received = new HashSet<>();
		this.minSeq = -1;

		this.n = this.s.peers.size(); 

		this.learntSeq = new int[n];
		this.maxSeq = new int[n];
		Arrays.fill(learntSeq, -1);
		Arrays.fill(maxSeq, -1);
		this.lock = new ReentrantLock();

		decided = ConcurrentHashMap.newKeySet();
		buffVal = ConcurrentHashMap.newKeySet();
		LV = new ConcurrentHashMap<>();
		acceptVal = ConcurrentHashMap.newKeySet();
		this.active = false;
		l = new UdpListener(this, this.ports.get(this.me));
		l.start();
		this.minSeq = Integer.MAX_VALUE;
		this.r = 0;
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

	public void run() {
		try {
			lock.lock();
			synchronized (this) {
				this.active = true;
			}

			while(this.buffVal.size() > 0) { 

				synchronized (this.acceptVal) {
					for(Op o : this.buffVal) {
						this.acceptVal.add(o);
					}
				}
				this.buffVal = ConcurrentHashMap.newKeySet();

				Set<Op> val = new HashSet<>();
				this.r = 0;
				decided = ConcurrentHashMap.newKeySet();

				for(; this.r < this.s.f + 1; this.r ++) {
					received = new HashSet<>();
					synchronized (this.acceptVal) {
						val = new HashSet<>(this.acceptVal);
					}
					if(val.size() == 0) break;
					if(Util.DEBUG) System.out.println(this.me + " propose " + val.toString());
					Request req = new Request("proposal", val, this.r, this.seq, this.me);
					this.tally = 1;
					this.broadCast(req);

					try {
						this.s.wlock.lock();
						this.s.wcond.signalAll();
					} catch (Exception e) {}
					finally {
						this.s.wlock.unlock();
					}

					int loop = 0;
					int want = 0;
					if(this.r == 0) want = this.n - this.s.f - 1;
					else want = this.n / 2;

					while(this.received.size() < want && this.decided.size() == 0) {
						if(Util.DEBUG) System.out.println(this.me + " waiting for ack received :"+ " "+ this.received + " active +" +this.active + " seq "+ this.seq + " round "+ this.r);
						loop ++;

						if(loop % 6 == 0) this.broadCast(req, this.received);
						try {
							Thread.sleep(6);
						} catch (Exception e) {}
					}

					if(Util.DEBUG) System.out.println(this.me + " got n - f acks");

					if(this.decided.size() > 0 && this.r < this.s.f) {
						if(Util.DEBUG) System.out.println(this.me +" got decide messages");
						val.removeAll(this.decided);
						if(val.size() > 0) {
							this.buffVal.addAll(val);
							Request serverVal = new Request("serverVal", val);
							this.broadCast(serverVal);
						}
						val = new HashSet<Op>(this.decided);
						break;
					} else if(this.tally > this.n / 2) 
						break;
					else { 
						if(Util.DEBUG) System.out.println(this.me + " reject by majority...");
					}
				}
				if(val.size() == 0) continue;

				if(Util.DEBUG) System.out.println(this.me + " complete seq " + this.seq + " " + val);
				LV.put(this.seq, val);
				this.seq ++;
				synchronized (this.acceptVal) {
					if(this.seq > 1)
						this.acceptVal.removeAll(this.LV.get(this.seq - 2));
					learntValues.addAll(val);
					//this.buffVal.removeAll(val);
					while(LV.containsKey(this.seq)) {
						this.acceptVal.removeAll(this.LV.get(this.seq - 1));
						this.seq ++;
					}
				}

				for(int i = 0; i < this.n; i++) {
					if(i == this.s.me || this.maxSeq[i] >= this.seq - 1) continue;
					LearntRequest lr = new LearntRequest("learnt", this.seq - 1, this.seq- 2, this.s.me);
					lr.add(this.seq - 1, val);
					this.sendUdp(lr, i);
				}

				this.s.apply(this.seq - 1);


				//signal all waiting reads 
				try {
					this.s.rlock.lock();
					this.s.rcond.signalAll();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					this.s.rlock.unlock();
				}

			}
			this.active = false;
		} finally {
			this.active = false;
			lock.unlock();
		}
	}

	public void broadCast(Request req, Set<Integer> ignore) {
		for(int i = 0; i < this.n; i++) {
			if(i == this.me || ignore.contains(i)) continue;
			this.sendUdp(req, i);
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
		for (Op op: val) {
			if(this.learntValues.contains(op)) continue;
			this.buffVal.add(op);
		}
		if(!this.active) {
			Thread t = new Thread(this);
			t.start();
		}
	}

	public boolean receive(Op op) {
		if(this.learntValues.contains(op)) return true;
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

	public Response handleRequest(Object obj) {
		Request req = (Request) obj;
		if(Util.DEBUG) System.out.println("get request "+req);
		if(Util.DEBUG) System.out.println("myseq " + this.seq);

		if(req.type.equals("proposal")) {
			if(this.maxSeq[req.me] > req.seq) return null;
			else if(this.maxSeq[req.me] < req.seq) {
				this.maxSeq[req.me] = req.seq;
			}

			if(this.LV.containsKey(req.seq)) {
				Request resp = new Request("decided", this.LV.get(req.seq), req.round, req.seq, this.me);
				this.sendUdp(resp, req.me);
				return null;
			}

			while(this.seq < req.seq) {
				if(this.maxSeq[req.me] > req.seq) return null;
				if(Util.DEBUG) System.out.println(this.me + " waiting for seq "+ this.seq +" "  +req.seq + " active? "+ this.active);
				if(!this.active) {
					Request getLearnt = new Request("getLearnt", null, -1, this.seq, this.me);
					this.sendUdp(getLearnt, req.me);
				}
				try {
					Thread.sleep(8);
				} catch(Exception e) {
				}
			}

			if(req.seq < this.seq) {
				if(this.LV.get(req.seq) == null) System.out.println(req.seq + " " + this.seq);
				Request resp = new Request("decided", this.LV.get(req.seq), req.round, req.seq, this.me);
				this.sendUdp(resp, req.me);
				return null;
			} 
			synchronized (this.acceptVal) {
				if(req.val.containsAll(this.acceptVal)) {
					Request resp = new Request("accept", null, req.round, req.seq, this.me);
					this.sendUdp(resp, req.me);
					this.acceptVal = req.val;
				} else {
					Request resp = new Request("reject", this.acceptVal, req.round, req.seq, this.me);
					this.sendUdp(resp, req.me);
					this.acceptVal.addAll(req.val);
				}
			}
		} else if(req.type.equals("decided")) {
			if(req.seq == this.seq) {
				if(req.val == null) System.out.println("null: " + req.seq);
				this.decided.addAll(req.val);
			} 
		} else if(req.type.equals("reject")) {
			if(req.seq == this.seq && req.round == this.r) {
				synchronized (this.acceptVal) {
					this.acceptVal.addAll(req.val);
				}
				this.received.add(req.me);
			}
		} else if(req.type.equals("accept")) {
			if(req.seq == this.seq && req.round == this.r) {
				this.tally ++;
				this.received.add(req.me);
			}
		} else if(req.type.equals("serverVal")) {
			this.receive(req.val);
		} else if(req.type.equals("getLearnt")) {
			int m = this.maxSeq[req.me] + 1;
			if(m < this.seq) { 
				LearntRequest lr = new LearntRequest("learnt", m, m -1, this.me);
				int i = m;
				while(i < this.seq) {
					lr.add(i, this.LV.get(i));
					if((i - m) % 25 == 0) {
						this.sendUdp(lr, req.me);
						lr = new LearntRequest("learnt", i + 1, i, this.me);
					}
					i ++;
				}
				if(lr.max >= lr.min) this.sendUdp(lr, req.me);
			}
		} else if(req.type.equals("learnt")) {
			LearntRequest lr = (LearntRequest) req;
			if(Util.DEBUG) System.out.println(this.me + " received learnt. mySeq "+this.seq + " hisSeq "+ lr.min + " " + lr.max + " active "+ this.active);
			if(lr.max < this.seq) return null;

			int start = Math.max(lr.min, this.seq);
			for(int i = start; i <= lr.max; i ++) {
				Set<Op> tmp = lr.values.get(i);
				if(!this.LV.containsKey(i)) {
					this.LV.put(i, tmp);
					this.learntValues.addAll(tmp);
					this.buffVal.removeAll(tmp);
				}
			}

			synchronized (this) { 
				if(!this.active) {
					synchronized (this.acceptVal) {
						while(this.LV.containsKey(this.seq)) {
							if(this.seq > 0) this.acceptVal.removeAll(this.LV.get(this.seq - 1));
							this.seq ++;
						}
					}
				} else {
					if(lr.values.get(this.seq) != null)
						this.decided.addAll(lr.values.get(this.seq));
				}
			}
			this.s.apply(this.seq - 1);
		} else {
			System.out.println("Invalid request!!!");
		}
		return null;
	}

	public Set<Op> learntVal(int Seq) {
		if(Seq < 0 || !this.LV.containsKey(Seq)) return new HashSet<>();
		return this.LV.get(Seq);
	}
}







