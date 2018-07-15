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
	public Map<Integer, Set<Op>> LV; //sequence number to learned values mapping
	public Set<Op> acceptVal; //current accept value
	public boolean active; //proposing or not
	public UdpListener l; 
	public Set<Op> decided; //values in decided messages  
	public int dec; //number of decide messages
	public Set<Op> rejected; //values in rejected messages 
	public int tally; //number of accept acks
	public int ack; //number of acks received
	public int n; //# peers
	public Set<Op> learntValues; //history of learned values
	public GlaServer s; 
	public Set<Integer> received; 
	public int minSeq; 
	public int[] learntSeq; //largest learned sequence number for each peer
	public ReentrantLock lock;

	public GLAAlpha (GlaServer s) {
		super(s.me, s.peers, s.port, s.ports);

		this.seq = 0;
		learntValues = new HashSet<>();
		this.lock = new ReentrantLock();
		tally = 0;
		this.s = s;
		this.received = new HashSet<>();
		this.dec = 0;
		this.minSeq = -1;

		this.n = this.s.peers.size(); 

		this.learntSeq = new int[n];
		Arrays.fill(learntSeq, -1);

		this.lock = new ReentrantLock();
		decided = new HashSet<>();
		rejected = new HashSet<>();
		buffVal = new HashSet<>();
		LV = new HashMap<>();
		acceptVal = new HashSet<>();
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

			while(this.buffVal.size() > 0) { 
				if(Util.DEBUG) System.out.println(this.me + " start seq " + this.seq);
				synchronized (this.acceptVal) {
					this.acceptVal.addAll(this.buffVal);
				}
				Set<Op> val = new HashSet<>();

				for(this.r = 0; this.r < this.s.f + 1; this.r ++) {
					decided = new HashSet<>();
					rejected = new HashSet<>();
					received = new HashSet<>();
					val = new HashSet<>(this.acceptVal);
					if(val.size() == 0) break;
					if(Util.DEBUG) System.out.println(this.me + " propose " + val.toString());
					Request req = new Request("proposal", val, this.r, this.seq, this.me);
					this.ack = 0;
					this.tally = 1;
					this.dec = 0;
					this.broadCast(req);
					int loop = 0;
					while(this.ack < this.n - this.s.f - 1 && this.dec == 0) {
						if(Util.DEBUG) System.out.println("waiting for ack");
						loop ++;
						if(loop % 10 == 0) this.broadCast(req, received);
						try {
							Thread.sleep(10);
						} catch (Exception e) {}
					}

					if(Util.DEBUG) System.out.println(this.me + " got n - f acks");

					if(this.decided.size() > 0) {
						if(Util.DEBUG) System.out.println(this.me +" got decide messages");
						Request serverVal = new Request("serverVal", val);
						this.broadCast(serverVal);
						val = this.decided;
						break;
					} else if(this.tally > this.n / 2) 
						break;
					else { 
						if(Util.DEBUG) System.out.println(this.me + " reject by majority...");
						synchronized (this.acceptVal) {
							acceptVal.addAll(rejected);
						}
					}
				}
				synchronized (this.buffVal) {
					this.buffVal.removeAll(val);
				}
				this.broadCast(new Request("learnt", val, -1, this.seq, this.me));

				if(Util.DEBUG) System.out.println(this.me + " complete seq " + this.seq + " " + val);
				LV.put(this.seq ++, val);
				synchronized (this.acceptVal) {
					this.acceptVal.removeAll(this.learntValues);
					learntValues.addAll(val);
					while(LV.containsKey(this.seq)) {
						if(Util.DEBUG) System.out.println(" containsKey...." +this.seq);
						this.acceptVal.removeAll(this.LV.get(this.seq - 1));
						learntValues.addAll(LV.get(seq));
						this.seq ++;
					}
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
		synchronized (this.buffVal) {
			for (Op op: val) {
				if(this.learntValues.contains(op)) continue;
				this.buffVal.add(op);
			}
		}
		if(!this.active) {
			Thread t = new Thread(this);
			t.start();
		}
	}

	public void receive(Op op) {
		if(this.learntValues.contains(op)) return;
		//the agree procedure may break while loop, but not check this.active yet.
		synchronized (this.buffVal) {
			this.buffVal.add(op);
		}
		if(!this.active) {
			Thread t = new Thread(this);
			t.start();
		}
	}

	public void sendUdp(Request req, int peer) {
		Messager.sendPacket(req, this.s.peers.get(peer), this.ports.get(peer));
	}

	public Response handleRequest(Request req) {
		if(req.type.equals("proposal")) {
			if(Util.DEBUG) System.out.println(this.me + " received proposal "+ req.val + " from "+ req.me + " for seq " + req.seq + " for round" + req.round); 

			while(this.seq < req.seq) {
				if(Util.DEBUG) System.out.println(this.me + " waiting for seq "+ this.seq +" "  +req.seq + " active? "+ this.active);
				if(!this.active) {
					Request getLearnt = new Request("getLearnt", null, -1, this.seq, this.me);
					this.sendUdp(getLearnt, req.me);
				}

				try {
					Thread.sleep(10);
				} catch(Exception e) {}
			}

			if(req.seq < this.seq) {
				Request resp = new Request("decided", this.LV.get(req.seq), req.round, req.seq, this.me);
				this.sendUdp(resp, req.me);
				return null;
			} 

			synchronized (this.acceptVal) {
				Set<Op> tmp = this.acceptVal;
				if(req.val.containsAll(tmp)) {
					Request resp = new Request("accept", null, req.round, req.seq, this.me);
					this.sendUdp(resp, req.me);
					this.acceptVal = req.val;
				} else {
					Request resp = new Request("reject", tmp, req.round, req.seq, this.me);
					this.sendUdp(resp, req.me);
					this.acceptVal.addAll(req.val);
				}
			}
		} else if(req.type.equals("decided")) {
			if(req.seq == this.seq && req.round == this.r) {
				this.dec ++;
				if(req.val != null)
				this.decided.addAll(req.val);
			} 
		} else if(req.type.equals("reject")) {
			if(req.seq == this.seq && req.round == this.r) {
				this.ack ++;
				this.rejected.addAll(req.val);
				this.received.add(req.me);
			}
		} else if(req.type.equals("accept")) {
			if(req.seq == this.seq && req.round == this.r) {
				this.ack ++;
				this.tally ++;
				this.received.add(req.me);
			}
		} else if(req.type.equals("serverVal")) {
			this.receive(req.val);
		} else if(req.type.equals("getLearnt")) {
			if(req.seq < this.seq)
				this.sendUdp(new Request("learnt", this.LV.get(req.seq), -1, req.seq, this.me), req.me);
		} else if(req.type.equals("learnt")) {
			try {
				lock.lock();
				if(Util.DEBUG) System.out.println(this.me + " received learnt. mySeq "+this.seq + " hisSeq "+ req.seq + " active "+ this.active);
				this.learntSeq[req.me] = req.seq;
				if(this.seq > req.seq || this.LV.containsKey(req.seq)) return null;
				else if(this.seq < req.seq) {
					this.LV.put(req.seq, req.val);
					synchronized (this.buffVal) {
						this.buffVal.removeAll(req.val);
					}
				} else if(!this.active) { 
					this.LV.put(this.seq ++, new HashSet<Op>(req.val));
					synchronized (this.acceptVal) {
						this.acceptVal.removeAll(this.learntValues);
						this.learntValues.addAll(req.val);
						while(this.LV.containsKey(this.seq)) {
							this.learntValues.addAll(LV.get(seq));
							this.acceptVal.removeAll(this.LV.get(this.seq - 1));
							this.seq ++;
						}
					}
					this.s.apply(this.seq - 1);
				}
			} finally {
				lock.unlock();
			}
			if(req.me == this.minSeq || this.minSeq == -1) {
				this.minLearntSeq();
			}
		} else {
			System.out.println("Invalid request!!!");
		}
		return null;
	}

	public Set<Op> learntVal(int Seq) {
		if(Seq < 0) return new HashSet<>();
		if(Util.DEBUG) System.out.println("learnt? " + (this.LV.get(Seq) == null) + " curr seq "+ this.seq +" invoke seq " + Seq);
		return this.LV.get(Seq);
	}
}







