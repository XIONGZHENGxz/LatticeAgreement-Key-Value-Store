package la.mgla;

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

public class Proposer implements Runnable {

	public MglaServer s;

	public volatile int seq; //active sequence number

	public Set<Op> buffVal; //values need to be learned

	public volatile boolean active; //proposing or not

	public int ack; //number of accept acks
	public int n; //# peers

	public Set<Op> learntValues; //history of learned values

	public Set<Integer> received; 

	public ReentrantLock lock;

	public Set<Op> propVal; //current proposing val

	public Proposer (MglaServer s) {
		this.s = s;
		this.seq = 0;
		this.propVal = new HashSet<>();
		this.learntValues = new HashSet<>();
		this.lock = new ReentrantLock();
		this.received = ConcurrentHashMap.newKeySet();
		this.n = this.s.peers.size(); 
		this.buffVal = new HashSet<>();
		this.active = false;
	}

	public void run() {
		try {
			lock.lock();
			this.active = true;

			while(true) { 
				synchronized (this.propVal) {
					synchronized (this.buffVal) {
						this.propVal.addAll(this.buffVal);
					}
				}

				/* keep proposing until got majority of accept */
				while(true) {
					this.seq ++;
					Set<Op> tmp = null;
					synchronized(this.propVal) {
						this.propVal.removeAll(this.learntValues);
						if(this.propVal.size() == 0) break;
						tmp = new HashSet<>(this.propVal);
					}
					if(Util.DEBUG) System.out.println("propose "+ tmp);
					Request req = new Request("proposal", tmp, this.seq, this.s.me);
					this.ack = 0;
					this.received = ConcurrentHashMap.newKeySet();
					this.broadCast(req);
					int loop = 0;
					while(this.received.size() < (this.n + 1) / 2) {
						if(Util.DEBUG) System.out.println("waiting for ack..." + this.received);
						loop ++;
						if(loop % 8 == 0) this.broadCast(req, received);
						try {
							Thread.sleep(6);
						} catch (Exception e) {}
					}

					if(Util.DEBUG) System.out.println(this.s.me + " got majority acks " );

					if(this.ack > this.n / 2) {
						synchronized (this.learntValues) {
							this.learntValues.addAll(tmp);
						}
						synchronized (this.propVal) {
							this.propVal.removeAll(tmp);
						}
						synchronized (this.buffVal) {
							this.buffVal.removeAll(tmp);
						}
						Request learntReq = new Request("learnt", tmp, this.seq, this.s.me);
						this.broadCast(learntReq);
						break;
					}  
					if(Util.DEBUG) System.out.println(this.s.me + " need to refine...");
					synchronized (this.learntValues) {
						tmp.removeAll(this.learntValues);
					}
					Request serverVal = new Request("serverVal", tmp);
					this.broadCast(serverVal);
				}

				try {
					this.s.lock.lock();
					this.s.learnt.signalAll();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					this.s.lock.unlock();
				}

				synchronized (this.buffVal) {
					synchronized (this.propVal) {
						if(this.buffVal.size() == 0 || this.propVal.size() == 0) { 
							this.active = false;
							break;
						}
					}
				}
			}
		} finally {
			lock.unlock();
		}
	}

	public void sendUdp(Request req, int peer) {
		Messager.sendPacket(req, this.s.peers.get(peer), this.s.ports.get(peer));
	}

	public void broadCast(Request req, Set<Integer> ignore) {
		for(int i = 0; i < this.n; i++) {
			if(i == this.s.me || ignore.contains(i)) continue;
			this.sendUdp(req, i);
		}
	}

	public void broadCast(Request req) {
		for(int i = 0; i < this.n; i++) {
			if(i == this.s.me) this.s.acceptor.handleProposal(req);
			else this.sendUdp(req, i);
		}
	}

	public void receiveClient(Op op) {
		this.receive(op);
		//Request req = new Request("serverVal", op);
		//this.broadCast(req, new HashSet<Integer>());
	}

	public void receive(Set<Op> val) {
		synchronized (this.learntValues) {
		for (Op op: val) {
			if(this.learntValues.contains(op)) continue;
			synchronized(this.buffVal) {
				this.buffVal.add(op);
			}
		}
		}
		if(!this.active) {
			Thread t = new Thread(this);
			t.start();
		}
	}

	public void receive(Op op) {
		synchronized (this.learntValues) {
			if(this.learntValues.contains(op)) return;
		}
		//the agree procedure may break while loop, but not check this.active yet.
		synchronized (this.buffVal) {
			this.buffVal.add(op);
		}
		if(!this.active) {
			Thread t = new Thread(this);
			t.start();
		}
	}

	public void handleRequest(Request req) {
			if(req.type.equals("reject")) {
				if(req.seq == this.seq && !this.received.contains(req.me)) {
					synchronized (this.propVal) {
						this.propVal.addAll(req.val);
					}
					this.received.add(req.me);
				}
			} else if(req.type.equals("accept") && !this.received.contains(req.me)) {
				if(req.seq == this.seq) {
					synchronized(this) {
						this.ack ++;
					}
					this.received.add(req.me);
				}
			} else if(req.type.equals("learnt")){ 
				synchronized (this.learntValues) {
					this.learntValues.addAll(req.val);
				}
				synchronized (this.propVal) {
					this.propVal.removeAll(req.val);
				} 
				synchronized (this.buffVal) {
					this.buffVal.removeAll(req.val);
				}
				try {
					this.s.lock.lock();
					this.s.learnt.signalAll();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					this.s.lock.unlock();
				}
			} else if(req.type.equals("serverVal")) {
				this.receive(req.val);
			}
	}

}
