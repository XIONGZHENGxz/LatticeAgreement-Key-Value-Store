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

	public int seq; //active sequence number

	public Set<Op> buffVal; //values need to be learned

	public boolean active; //proposing or not

	public int ack; //number of accept acks
	public int count; //number of acks received 
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
		this.received = new HashSet<>();
		this.n = this.s.peers.size(); 
		this.buffVal = new HashSet<>();
		this.active = false;
	}

	public void run() {
		try {
			lock.lock();
			this.active = true;

			while(this.buffVal.size() > 0 || this.propVal.size() > 0) { 
				synchronized (this.propVal) {
					this.propVal.addAll(this.buffVal);
				}

				/* keep proposing until got majority of accept */
				while(true) {
					this.seq ++;
					if(Util.DEBUG) System.out.println(this.s.me + " start seq " + this.seq);
					Set<Op> tmp = null;
					synchronized(this.propVal) {
						tmp = new HashSet<>(this.propVal);
					}
					Request req = new Request("proposal", tmp, this.seq, this.s.me);
					this.ack = 0;
					this.count = 0;
					this.received = new HashSet<>();
					this.broadCast(req);
					int loop = 0;
					while(this.count < this.n / 2) {
						if(Util.DEBUG) System.out.println("waiting for ack " + this.count + " received "+ this.received);
						loop ++;
						if(loop % 10 == 0) this.broadCast(req, received);
						try {
							Thread.sleep(10);
						} catch (Exception e) {}
					}

					if(Util.DEBUG) System.out.println(this.s.me + " got majority acks");

					if(this.ack > this.n / 2) {
						this.learntValues.addAll(tmp);
						this.propVal.removeAll(tmp);
						synchronized (this.buffVal) {
							this.buffVal.removeAll(tmp);
						}
						Request learntReq = new Request("learnt", tmp);
						this.broadCast(learntReq);
						break;
					}  
					if(Util.DEBUG) System.out.println(this.s.me + " need to refine...");
				}

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
			if(i == this.s.me) this.s.handleRequest(req);
			else this.sendUdp(req, i);
		}
	}

	public void receiveClient(Op op) {
		this.receive(op);
		Request req = new Request("serverVal", op);
		this.broadCast(req);
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

	public void handleRequest(Request req) {
		synchronized (this.received) {
			if(req.type.equals("reject")) {
				if(req.seq == this.seq && !this.received.contains(req.me)) {
					this.count ++;
					synchronized (this.propVal) {
						this.propVal.addAll(req.val);
					}
					this.received.add(req.me);
				}
			} else if(req.type.equals("accept") && !this.received.contains(req.me)) {
				if(req.seq == this.seq) {
					this.ack ++;
					this.count ++;
					this.received.add(req.me);
				}
			} else if(req.type.equals("learnt")){ 
				this.learntValues.addAll(req.val);
				synchronized (this.propVal) {
					this.propVal.removeAll(req.val);
				}
			} else if(req.type.equals("serverVal")) {
				this.receive(req.op);
			}
		}
	}

}
