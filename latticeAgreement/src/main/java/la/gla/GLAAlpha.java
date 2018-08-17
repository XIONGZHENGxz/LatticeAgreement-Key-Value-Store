package la.gla;

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

public class GLAAlpha extends Server implements Runnable {

	public int seq; //next available sequence number 
	public int r; //round number 
	public Set<Op> buffVal; //values need to be learned
	public ConcurrentHashMap<Integer, Set<Op>> LV; //sequence number to learned values mapping
	public Set<Op> acceptVal; //current accept value
	public boolean active; //proposing or not
	public UdpListener l; 
	public Set<Op> decided; //values in decided messages  
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

			while(true) { 
				if(this.buffVal.size() == 0) break;
				if(Util.DEBUG) System.out.println(this.s.me + " start seq: " + this.seq + " buff: " + this.buffVal);
				synchronized (this.acceptVal) {
					if(Util.DEBUG) System.out.println(this.s.me + " accept: " + this.acceptVal);
					for(Op o : this.buffVal) this.acceptVal.add(o);
				}
				Set<Op> val = new HashSet<>();
				this.r = 0;
				this.decided = ConcurrentHashMap.newKeySet();

				for(; this.r < this.s.f + 1;) {
					received = new HashSet<>();
					synchronized (this.acceptVal) {
						val = new HashSet<>(this.acceptVal);
					}
					if(val.size() == 0) break;
					if(Util.DEBUG) System.out.println(this.me + " propose " + val.toString());
					Request req = new Request("proposal", val, this.r, this.seq, this.me);
					this.tally = 1;
					this.broadCast(req);
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
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

					if(Util.DEBUG) System.out.println(this.me + " got n - f acks");

					/* get any decide message, take join of all decided values */
					if(this.decided.size() > 0 && this.r < this.s.f) {
						if(Util.DEBUG) System.out.println(this.me +" got decide messages");
						val.removeAll(this.decided);
						if(val.size() > 0) {
							Request serverVal = new Request("serverVal", val);
							this.broadCast(serverVal);
						}
						val = new HashSet<>();
						for(Op op : this.decided) {
							val.add(op);
						}
						break;
					} else if(this.tally > this.n / 2) 
						break;
					else { 
						if(Util.DEBUG) System.out.println(this.me + " reject by majority...");
					}
					this.r ++;
				}

				if(val.size() == 0) continue;

				LV.put(this.seq, val);
				if(Util.DEBUG) System.out.println(this.me + " complete seq " + this.seq + " " + val);
				this.seq ++;
				synchronized (this.acceptVal) {
					if(this.seq > 1)
						this.acceptVal.removeAll(this.LV.get(this.seq - 2));
					this.buffVal.removeAll(val);
					this.learntValues.addAll(val);
					while(this.LV.containsKey(this.seq)) {
						this.acceptVal.removeAll(this.LV.get(this.seq - 1));
						this.seq ++;
					}
				}

				/* broadcast learnt */
				for(int i = 0; i < this.n; i ++) {
					if(i == this.s.me || this.maxSeq[i] >= this.seq - 1) continue;
					LearntRequest lr = new LearntRequest("learnt", this.seq - 1, this.seq - 2, this.s.me);
					lr.add(this.seq - 1, val);
					this.sendUdp(lr, i);
				}


				this.s.apply(this.seq - 1);

				try {
					this.s.lock.lock();
					this.s.learnt.signalAll();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					this.s.lock.unlock();
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

	public void receiveClient(Op op) {
		this.receive(op);
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

	public void receive(Op op) {
		if(this.learntValues.contains(op)) return;
		//the agree procedure may break while loop, but not check this.active yet.
		this.buffVal.add(op);
		if(!this.active) {
			Thread t = new Thread(this);
			t.start();
		}
	}

	public void sendUdp(Request req, int peer) {
		Messager.sendPacket(req, this.s.peers.get(peer), this.ports.get(peer));
	}

	public Response handleRequest(Object obj) {
		Request req = (Request) obj;
		if(Util.DEBUG) System.out.println("get request "+req);

		if(req.type.equals("proposal")) {
			int t = this.maxSeq[req.me];
			if(t > req.seq) return null;
			else if(t < req.seq) {
				this.maxSeq[req.me] = req.seq;
			}

			if(this.LV.containsKey(req.seq)) {
				Request resp = new Request("decided", this.LV.get(req.seq), req.round, req.seq, this.me);
				this.sendUdp(resp, req.me);
				return null;
			}

			while(this.seq < req.seq) {
				if(this.maxSeq[req.me] > req.seq) return null;
				//System.out.println(this.me + " waiting for seq "+ this.seq +" "  +req.seq + " active? "+ this.active);
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
			LearntRequest lr = new LearntRequest("learnt", m, m - 1, this.me);
			int i = m;
			while(i < this.seq) { 
				lr.add(i, this.LV.get(i));
				i ++;
			}
			this.sendUdp(lr, req.me);
		} else if(req.type.equals("learnt")) {
			LearntRequest lr = (LearntRequest) req;
			if(Util.DEBUG) System.out.println(this.me + " received learnt. mySeq "+this.seq + " hisSeq "+ lr.min + " "+lr.max + " active "+ this.active);

			/* if small than current seq, ignore */ 
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







