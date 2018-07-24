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
	public volatile boolean active; //proposing or not
	public UdpListener l; 
	public Set<Op> decided; //values in decided messages  
	public volatile int dec; //number of decide messages
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
	public List<int[]> maxSeq; //store max seq seen for each peer
	public int max_seq;

	public GLAAlpha (GlaServer s) {
		super(s.me, s.peers, s.port, s.ports);

		this.seq = 0;
		this.max_seq = 0;
		learntValues = new HashSet<>();
		this.lock = new ReentrantLock();
		tally = 0;
		this.s = s;
		this.received = new HashSet<>();
		this.dec = 0;
		this.minSeq = -1;

		this.n = this.s.peers.size(); 

		this.learntSeq = new int[n];
		this.maxSeq = new ArrayList<>();
		Arrays.fill(learntSeq, -1);

		for(int i = 0; i < n; i++) {
			this.maxSeq.add(new int[]{-1, -1});
		}

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
			synchronized (this) {
				this.active = true;
			}

			while(true) { 
				if(Util.DEBUG) System.out.println(this.me + " start seq " + this.seq + " active "+ this.active);
				synchronized (this.acceptVal) {
					synchronized (this.buffVal) {
						this.acceptVal.addAll(this.buffVal);
					}
				}
				Set<Op> val = new HashSet<>();
				this.r = 0;

				for(; this.r < this.s.f + 1; this.r ++) {
					decided = new HashSet<>();
					rejected = new HashSet<>();
					received = new HashSet<>();
					synchronized (this.acceptVal) {
						val = new HashSet<>(this.acceptVal);
					}
					if(val.size() == 0) break;
					if(Util.DEBUG) System.out.println(this.me + " propose " + val.toString());
					Request req = new Request("proposal", val, this.r, this.seq, this.me);
					this.ack = 0;
					this.tally = 1;
					this.dec = 0;
					this.broadCast(req);
					int loop = 0;
					int want = 0;
					if(this.r == 0) want = this.n - this.s.f - 1;
					else want = this.n / 2;
					
					while(this.ack < want && this.dec == 0) {
						if(Util.DEBUG) System.out.println("waiting for ack received :"+ this.received + " active +" +this.active);
						loop ++;

						if(loop % 5 == 0) this.broadCast(req, this.received);
						try {
							Thread.sleep(10);
						} catch (Exception e) {}
					}

					if(Util.DEBUG) System.out.println(this.me + " got n - f acks");

					if(this.dec > 0) {
						if(Util.DEBUG) System.out.println(this.me +" got decide messages");
						synchronized (this.decided) {
							val.removeAll(this.decided);
							Request serverVal = new Request("serverVal", val);
							this.broadCast(serverVal);
							val = new HashSet<Op>(this.decided);
						}
						break;
					} else if(this.tally > this.n / 2) 
						break;
					else { 
						if(Util.DEBUG) System.out.println(this.me + " reject by majority...");
						synchronized (this.acceptVal) {
							synchronized (this.rejected) {
								this.acceptVal.addAll(rejected);
							}
						}
					}
				}
				synchronized (this.buffVal) {
					this.buffVal.removeAll(val);
				}
				this.broadCast(new Request("learnt", val, -1, this.seq, this.me));

				if(Util.DEBUG) System.out.println(this.me + " complete seq " + this.seq + " " + val);
				LV.put(this.seq++, val);
				synchronized (this.acceptVal) {
					synchronized (this.learntValues) {
						this.acceptVal.removeAll(this.learntValues);
						learntValues.addAll(val);
						while(LV.containsKey(this.seq)) {
							this.acceptVal.removeAll(this.LV.get(this.seq - 1));
							learntValues.addAll(LV.get(seq));
							this.seq ++;
						}
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
				synchronized (this.buffVal) {
					if(this.buffVal.size() == 0) 
						break;
				}
			}
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
			synchronized(this.buffVal) {
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

	public Response handleRequest(Object obj) {
		Request req = (Request) obj;
		if(Util.DEBUG) System.out.println("get request "+req);
		if(Util.DEBUG) System.out.println("curr seq and round "+this.seq + " "+ this.r);

		if(req.type.equals("proposal")) {
			if(Util.DEBUG) System.out.println(this.me + " received proposal "+ req.val + " from "+ req.me + " for seq " + req.seq + " for round" + req.round); 
			int[] tmp = this.maxSeq.get(req.me);
			if(tmp[0] > req.seq) return null;
			else if(tmp[0] < req.seq) {
				tmp[0] = req.seq;
				tmp[1] = req.round;
			} else {
				if(tmp[1] <= req.round) {
					tmp[1] = req.round;
				} else return null;
			}

			if(this.LV.containsKey(req.seq)) {
				Request resp = new Request("decided", this.LV.get(req.seq), req.round, req.seq, this.me);
				this.sendUdp(resp, req.me);
				return null;
			}

			while(this.seq < req.seq) {
				if(tmp[0] > req.seq || tmp[1] > req.round) return null;
				//System.out.println(this.me + " waiting for seq "+ this.seq +" "  +req.seq + " active? "+ this.active);
				if(!this.active) {
					Request getLearnt = new Request("getLearnt", null, -1, this.seq, this.me);
					this.sendUdp(getLearnt, req.me);
				}
				try {
					Thread.sleep(10);
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
					Request resp = new Request("reject", new HashSet<Op> (this.acceptVal), req.round, req.seq, this.me);
					this.sendUdp(resp, req.me);
					this.acceptVal.addAll(req.val);
				}
			}
		} else if(req.type.equals("decided")) {
			if(req.seq == this.seq) {
				this.dec ++;
				synchronized(this.decided) {
					if(req.val != null)
						this.decided.addAll(req.val);
				}
			} 
		} else if(req.type.equals("reject")) {
			if(req.seq == this.seq && req.round == this.r) {
				this.ack ++;
				synchronized (this.rejected) {
					this.rejected.addAll(req.val);
				}
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
			if(Util.DEBUG) System.out.println(this.me + " received learnt. mySeq "+this.seq + " hisSeq "+ req.seq + " active "+ this.active);
			if(req.seq < this.seq || this.LV.containsKey(req.seq)) return null;
			this.learntSeq[req.me] = req.seq;
			if(this.seq < req.seq) {
				this.LV.put(req.seq, req.val);
				synchronized (this.buffVal) {
					this.buffVal.removeAll(req.val);
				}
			} else {
				synchronized (this) {
					if(!this.active) { 
						synchronized (this.acceptVal) {
							synchronized (this.learntValues) {
								this.LV.put(this.seq ++, new HashSet<Op>(req.val));
								this.acceptVal.removeAll(this.learntValues);
								this.learntValues.addAll(req.val);
								while(this.LV.containsKey(this.seq)) {
									this.learntValues.addAll(LV.get(seq));
									this.acceptVal.removeAll(this.LV.get(this.seq - 1));
									this.seq ++;
								}
							}
						}
					} else { 
						this.dec ++;
						synchronized (this.decided) {
							if(req.val != null) this.decided.addAll(req.val);
						}
					}
				}
				this.s.apply(this.seq - 1);
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
		if(Seq < 0 || !this.LV.containsKey(Seq)) return new HashSet<>();
		if(Util.DEBUG) System.out.println("learnt? " + (this.LV.get(Seq) == null) + " curr seq "+ this.seq +" invoke seq " + Seq);
		return this.LV.get(Seq);
	}
}







