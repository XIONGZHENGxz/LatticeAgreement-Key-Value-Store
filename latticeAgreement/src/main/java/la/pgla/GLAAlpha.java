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
		Arrays.fill(learntSeq, -1);
		for(int i = 0; i < n; i++) this.maxSeq.add(new int[]{-1, -1});

		this.lock = new ReentrantLock();

		decided = new ConcurrentHashMap<>();
		buffVal = ConcurrentHashMap.newKeySet();
		LV = new ConcurrentHashMap<>();
		LV.put(-1, new HashSet<Op>());
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
			this.active = true;

			while(true) { 
				if(this.buffVal.size() == 0) break;
				Set<Op> val = new HashSet<>();
				synchronized (this.acceptVal) {
					this.catchUp();
					for(Op o : this.buffVal) {
						this.acceptVal.add(o);
					}
					val.addAll(this.acceptVal);
				}


				if(Util.DEBUG) System.out.println(this.s.me + " start seq: " + this.seq + " buff: " + this.buffVal);
				this.r = 0;

				for(; this.r < this.s.f + 1;) {
					received = new HashSet<>();
					if(val.size() == 0) break;
					if(Util.DEBUG) System.out.println(this.me + " propose " + val.toString());
					Request req = new Request("proposal", val, this.LV.get(this.seq - 1), this.r, this.seq, this.me);
				//	else req = new Request("proposal", val, null, this.r, this.seq, this.me);
					this.tally = 1;
					this.broadCast(req);
					int loop = 0;
					int want = 0;
					if(this.r == 0) want = this.n - this.s.f - 1;
					else want = n / 2;

					while(this.received.size() < want && !this.decided.containsKey(this.seq)) {
						if(Util.DEBUG) System.out.println(this.me + " waiting for ack received :"+ " "+ this.received + " active +" +this.active + " seq "+ this.seq + " round "+ this.r);
						loop ++;

						if(loop % 10 == 0) this.broadCast(req, this.received);
						try {
							Thread.sleep(6);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

					if(Util.DEBUG) System.out.println(this.me + " got n - f acks");

					/* get any decide message, take join of all decided values */
					if(this.decided.containsKey(this.seq) && this.r < this.s.f) {
						if(Util.DEBUG) System.out.println(this.me +" got decide messages");
						val = this.decided.get(this.seq);
						break;
					} else if(this.tally > this.n / 2) 
						break;
					else { 
						if(Util.DEBUG) System.out.println(this.me + " reject by majority...");
						synchronized (this.acceptVal) {
							val.addAll(this.acceptVal);
						}
					}
					this.r ++;
				}

				if(val.size() == 0) continue;
				/*
				Request learntReq = new Request("decided", val, this.seq, this.me);
				for(int i = 0; i < this.n; i++) {
					if(i == this.me) continue;
					int[] tmp = this.maxSeq.get(i);
					if(tmp[0] >= this.seq) continue;
					this.sendUdp(learntReq, i);
				}
				*/

				LV.put(this.seq ++, val);

				if(Util.DEBUG) System.out.println(this.me + " complete seq " + this.seq + " " + val);

				synchronized (this.acceptVal) {
					this.acceptVal.removeAll(this.LV.get(this.seq - 2));
				}

				this.buffVal.removeAll(val);
				this.learntValues.addAll(val);
				
				this.wake();

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

	public void catchUp() {
		while(this.decided.containsKey(this.seq)) {
			Set<Op> tmpVal = this.decided.get(this.seq);
			this.acceptVal.removeAll(this.LV.get(this.seq - 1));
			this.buffVal.removeAll(tmpVal);
			this.learntValues.addAll(tmpVal);
			this.LV.put(this.seq ++, tmpVal);
		}
	}

	public Response handleRequest(Object obj) {
		Request req = (Request) obj;
		if(Util.DEBUG) System.out.println("get request "+req);

		if(req.type.equals("proposal")) {
			int[] tmp = this.maxSeq.get(req.me);
			if(tmp[0] > req.seq) return null;
			else if(tmp[0] < req.seq) {
				tmp[0] = req.seq;
				tmp[1] = req.round;
			} else {
				if(tmp[1] <= req.round) tmp[1] = req.round;
				else return null;
			}

			if(!this.decided.containsKey(req.seq - 1)) {
				this.decided.put(req.seq - 1, req.learnt);
			}

			while(this.seq < req.seq) {
				if(tmp[0] > req.seq || tmp[1] > req.round) return null;
			//	System.out.println(this.me + " waiting for seq "+ this.seq +" "  +req.seq + " active? "+ this.active);
				if(!this.active) {
					synchronized (this.acceptVal) {
						this.catchUp();
					}
					Request getLearnt = new Request("getLearnt", null, -1, this.seq, this.me);
					this.sendUdp(getLearnt, req.me);
				}
				try {
					Thread.sleep(8);
				} catch(Exception e) {
				}
			}

			if(req.seq < this.seq) {
				req.val.removeAll(this.LV.get(req.seq));
				this.receive(req.val);
				Request resp = new Request("decided", this.LV.get(req.seq), req.round, req.seq, this.me);
				this.sendUdp(resp, req.me);
				return null;
			} 

			synchronized (this.acceptVal) {
				if(req.round < this.s.f && req.val.size() >= this.acceptVal.size() && req.val.containsAll(this.acceptVal)) {
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
			if(!this.decided.containsKey(req.seq))
				this.decided.put(req.seq, req.val);
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







