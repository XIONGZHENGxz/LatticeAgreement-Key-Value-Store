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
import java.util.concurrent.TimeUnit;

import la.common.UdpListener;
import la.common.Op;
import la.common.Messager;
import la.common.Util;
import la.common.Server;

import la.common.Response;
import la.common.Server;

public class GLAAlpha extends Server implements Runnable {

	public volatile int seq; //next available sequence number 
	public volatile int r; //round number 
	public Set<Op> writeBuffVal; //values need to be learned
	public Set<Op> readBuffVal; 
	public ConcurrentHashMap<Integer, Set<Op>> LV; //sequence number to learned values mapping
	public Set<Op> acceptVal; //current accept value
	public Set<Op> oldAccept; //current accept value
	public boolean active; //proposing or not
	public UdpListener l; 
	public volatile int tally; //number of accept acks
	public int n; //# peers
	public Set<Op> learntValues; //history of learned values
	public GlaServer s; 
	public final Object lock1 = new Object();
	public final Object lock2 = new Object();
	public final Object lock3 = new Object();
	public Set<Integer> received; 
	public Set<Integer> all; 
	public int minSeq; 
	public int[] learntSeq; //largest learned sequence number for each peer
	public ReentrantLock lock;
	public Map<Integer, Set<Op>> decided;
	public Map<Integer, Set<Op>> learntReads;
	public List<int[]> maxSeq; //store max seq seen for each peer
	public Request[] maxProp;
	public Set<Integer> reqs;
	public Map<Integer, Boolean> changed;
	public Map<Integer, Set<Op>> deltas;
	public int currSize;
	public int learntMaxSeq;
	public Map<Integer, Set<Op>> lastRound;

	public GLAAlpha (GlaServer s) {
		super(s.me, s.peers, s.port, s.ports);

		this.seq = 0;
		learntValues = ConcurrentHashMap.newKeySet();

		this.lock = new ReentrantLock();
		tally = 0;
		this.s = s;
		this.reqs = new HashSet<>();
		this.received = new HashSet<>();
		this.all = new HashSet<>();
		this.minSeq = -1;

		this.n = this.s.peers.size(); 

		this.learntSeq = new int[n];
		this.learntMaxSeq = -1;
		this.maxSeq = new ArrayList<>();
		this.maxProp = new Request[this.n];
		Arrays.fill(learntSeq, -1);
		Arrays.fill(maxProp, new Request("", null, null, -1, -1, -1));
		for(int i = 0; i < n; i++) this.maxSeq.add(new int[]{-1, -1});

		this.lock = new ReentrantLock();

		decided = new ConcurrentHashMap<>();
		learntReads = new ConcurrentHashMap<>();
		writeBuffVal = ConcurrentHashMap.newKeySet();
		readBuffVal = ConcurrentHashMap.newKeySet();
		LV = new ConcurrentHashMap<>();
		LV.put(-1, new HashSet<Op>());
		LV.put(0, new HashSet<Op>());
		learntReads.put(-1, new HashSet<Op>());
		learntReads.put(0, new HashSet<Op>());
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
		Set<Op> writes = new HashSet<>();
		Set<Op> reads = new HashSet<>();
		//Set<Op> newVals = new HashSet<>();
		if(this.LV.containsKey(this.seq - 1))
		this.acceptVal.removeAll(this.LV.get(this.seq - 1));
		//this.oldAccept = ConcurrentHashMap.newKeySet();
		for(Op o : this.acceptVal) {
			//	oldAccept.add(o);
			writes.add(o);
		}
		/* add writes to propose, accept only has writes */	
		for(Op o : this.writeBuffVal) {
			if(writes.size() > Util.threshold / this.n) break;
			this.acceptVal.add(o);
			writes.add(o);
			//	newVals.add(o);
		}
		/* reads */
		for(Op o : this.readBuffVal) {
			reads.add(o);
		}
		this.seq ++;
		//System.out.println(this.s.me + " start seq: " + this.seq + " val size: " + writes.size());
		this.handleAllProp();

		boolean writesWaked = false;
		synchronized(lock2) {
			this.all = new HashSet<>();
		}
		this.received = new HashSet<>();

		for(this.r = 0; this.r < this.s.f + 1; this.r ++) {
			received = new HashSet<>();
			if(Util.DEBUG) System.out.println(this.me + " propose " + writes.toString());
			Request req = null;
			if(this.r == 0) 
				req = new Request("proposal", writes, reads, this.LV.get(this.seq - 1), this.learntReads.get(this.seq - 1), this.r, this.seq, this.me);
			else req = new Request("proposal", writes, reads, this.r, this.seq, this.me);
			this.tally = 1;
			this.broadCast(req);
			int loop = 0;
			int want = 0;
			if(this.r == 0) want = this.n - this.s.f - 1;
			else want = n / 2;
			while(this.received.size() < want && !this.decided.containsKey(this.seq)) {
				if(this.s.fail) return;
				//while(this.received.size() < want) {
				//System.out.println(this.me + " waiting for ack received :"+ " "+ this.received + " active +" +this.active + " seq "+ this.seq + " round "+ this.r);
				loop ++;

				if(loop % 4 == 0) this.broadCast(req, this.received);
				try {
					Thread.sleep(Util.gap);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			if(Util.DEBUG) System.out.println(this.me + " got n - f acks");

			/* wake writes */
			if(!writesWaked && this.received.size() >= want) {
				this.writeBuffVal.removeAll(writes);
				this.wakeWrites();
				writesWaked = true;
			}

			synchronized (lock2) {
				this.all.addAll(this.received);
				if(this.all.size() == this.n - 1) break;
			}

			/* get any decide message, take join of all decided values */
			if (this.r < this.s.f && this.tally > this.n / 2) {
				break;
			} else if(this.r < this.s.f && this.decided.containsKey(this.seq)) {
				if(Util.DEBUG) System.out.println(this.me +" got decide messages");
				writes = this.decided.get(this.seq);
				reads = this.learntReads.get(this.seq);
				break;
			} else { 
				if(Util.DEBUG) System.out.println(this.me + " reject by majority...");
				for(Op o : this.acceptVal) writes.add(o);
			}
			}
			if(this.s.fail) return;

			this.decided.put(this.seq, writes);
			if(!writesWaked) {
				this.writeBuffVal.removeAll(writes);
				this.wakeWrites();
			}
			if(reads != null) {
				this.learntReads.put(this.seq, reads);
				this.readBuffVal.removeAll(reads);
			} else {
				this.learntReads.put(this.seq, new HashSet<Op>());
			}
			this.LV.put(this.seq , writes);
			this.learntMaxSeq = this.seq;

			//System.out.println(this.me + " complete seq " + this.seq);
			this.acceptVal.removeAll(this.LV.get(this.seq - 1));
			/*
			   synchronized (this.acceptVal) {
			   this.acceptVal = ConcurrentHashMap.newKeySet();
			   }
			   this.acceptVal.addAll(newVals);
			 */
			//this.acceptVal.removeAll(this.oldAccept);
			//	if(receivedAll) 
			//		this.acceptVal.removeAll(this.LV.get(this.seq));
			this.wakeReads();
			//this.sendLearnt(this.seq, writes, reads);
		//	this.s.apply(this.seq);
		}

		public void sleep(int t) {
			try {
				Thread.sleep(t);
			} catch (Exception e) {}
		}

		public void run() {
			try {
				lock.lock();
				synchronized(lock1) {
					this.active = true;
				}
				while(!this.s.fail) { 
					this.seq = this.catchUp();
					this.start();
					if(this.writeBuffVal.size() == 0 && this.readBuffVal.size() == 0) break;
				}
			} finally {
				this.active = false;
				lock.unlock();
			}
		}

		public void sendLearnt(int seq, Set<Op> writes, Set<Op> reads) {
			Request learntReq = new Request("decided", writes,  reads, -1, seq, this.me);
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

		public void wakeWrites() {
			try {
				this.s.wlock.lock();
				this.s.wcond.signalAll();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				this.s.wlock.unlock();
			}
		}
	
		public void wakeReads() {
					try {
						this.s.rlock.lock();
						this.s.rcond.signalAll();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						this.s.rlock.unlock();
					}
		} 
/*
		public void wakeReads() {
			try {	
				if(this.s.elock.tryLock(Util.sigTimeout, TimeUnit.MILLISECONDS)) {
					try {
						this.s.econd.signalAll();
						System.out.println(this.me +" aignalled...");
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						this.s.elock.unlock();
					}
				}
			} catch (Exception e) {
			} 
		}
*/
		public void broadCast(Request req) {
			for(int i = 0; i < this.n; i++) {
				if(i == this.me) continue;
				this.sendUdp(req, i);
			}
		}

		public void receiveWrite(Op op) {
			this.writeBuffVal.add(op);
			synchronized(lock1) {
				if(!this.active) {
					Thread t = new Thread(this);
					//this.active = true;
					t.start();
				}
			}
		}

		public void receiveRead(Op op) {
			this.readBuffVal.add(op);
			synchronized(lock1) {
				if(!this.active) {
					Thread t = new Thread(this);
					//this.active = true;
					t.start();
				}
			}
		}

		public void receiveServer(Set<Op> writes, Set<Op> reads) {
			this.writeBuffVal.addAll(writes);
			this.readBuffVal.addAll(reads);
			if(this.writeBuffVal.size() > 0 || this.readBuffVal.size() > 0) {
				synchronized(lock1) {
					if(!this.active) {
						Thread t = new Thread(this);
						//this.active = true;
						t.start();
					}
				}
			}
		}

		public void sendUdp(Request req, int peer) {
			Messager.sendPacket(req, this.s.peers.get(peer), this.ports.get(peer));
		}

		public void catchUp(int s) {
			while(this.seq < s) {
				this.seq ++;
				if(this.decided.containsKey(this.seq)) {
					this.LV.put(this.seq, this.decided.get(this.seq));
					this.acceptVal.removeAll(this.LV.get(this.seq - 1));
				} 
			}
			this.handleAllProp();
		}

		public int catchUp() {
			int currSeq = this.seq + 1;
			while(this.decided.containsKey(currSeq)) {
				Set<Op> tmpVal = this.decided.get(currSeq);
				if(this.LV.containsKey(currSeq - 1))
				this.acceptVal.removeAll(this.LV.get(currSeq - 1));
				this.writeBuffVal.removeAll(tmpVal);
				if(this.learntReads.containsKey(currSeq))
					this.readBuffVal.removeAll(this.learntReads.get(currSeq));
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

		/* handle proposal for current sequence */
		public void handleProp(Request req) {
			//System.out.println(this.me + " handle proposal....");
			Set<Op> tmpAcc = new HashSet<>();  
			Set<Integer> rev;  
			for (Op o : this.acceptVal) {
				tmpAcc.add(o);
			}
			synchronized (lock2) {
				rev = new HashSet<>(this.all);
			}
			tmpAcc.removeAll(req.writes);
			Request resp = null;
			if(req.round >= this.s.f || tmpAcc.size() > 0) {
				resp = new Request("reject", rev, tmpAcc, null, req.round, req.seq, this.me);
			} else {
				resp = new Request("accept", null, null, req.round, req.seq, this.me);
			}
			this.acceptVal.addAll(req.writes);
			synchronized (lock2) {
				this.all.add(req.seq);
			}
			this.sendUdp(resp, req.me);
		}

		public Response handleRequest(Object obj) {
			Request req = (Request) obj;
			//System.out.println("get request "+req);

			if(req.type.equals("proposal")) {
				System.out.println(this.me + " proposal...."+req.seq + " " + this.seq + " " + this.active);
				if(req.seq < this.seq) {
					req.writes.removeAll(this.LV.get(req.seq));
					if(this.learntReads.containsKey(req.seq)) 
						req.reads.removeAll(this.learntReads.get(req.seq));
					this.receiveServer(req.writes, req.reads);
					Request resp = new Request("decided", this.LV.get(req.seq), this.learntReads.get(req.seq), req.round, req.seq, this.me);
					this.sendUdp(resp, req.me);
				} 
				else if(req.seq == this.seq) {
					this.handleProp(req);
				} 
				else {
					Request tmpProp = this.maxProp[req.me];
					if(tmpProp.seq < req.seq ||
							(tmpProp.seq == req.seq && tmpProp.round <= req.round)) 
						this.maxProp[req.me] = req;
					if(req.round == 0) {
						if(!this.decided.containsKey(req.seq - 1)) { 
							this.decided.put(req.seq - 1, req.learntWrites);
						}

						if(!this.learntReads.containsKey(req.seq - 1))  
							this.learntReads.put(req.seq - 1, req.learntReads);
					}
					synchronized(lock1) {
						if(!this.active) {
							Thread t = new Thread(this);
							//this.active = true;
							t.start();
						}
					}
				}
				return null;
			} else if(req.type.equals("decided")) {
				if(!this.decided.containsKey(req.seq)) {
					this.decided.put(req.seq, req.writes);
					this.learntReads.put(req.seq, req.reads);
				} 	
				if(req.seq == this.seq && req.round == this.r) 
					this.received.add(req.me);
			} else if(req.type.equals("reject")) {
				if(req.seq == this.seq && req.round == this.r) {
					this.acceptVal.addAll(req.writes);
					this.received.add(req.me);
					synchronized (lock2) {
						this.all.addAll(req.received);
					}
				}
			} else if(req.type.equals("accept")) {
				if(req.seq == this.seq && req.round == this.r) { 
					this.tally ++;
					this.received.add(req.me);
				}
			} else if(req.type.equals("getLearnt")) {
				int[] tmp = this.maxSeq.get(req.me);
				if(req.seq >= tmp[0] && this.LV.containsKey(req.seq)) {
					Request lr = new Request("decided", this.LV.get(req.seq),null, -1, req.seq, this.me);
					this.sendUdp(lr, req.me);
				}
			}
			return null;
		}

		public Set<Op> learntWrites(int Seq) {
			return this.LV.get(Seq);
		}

		public Set<Op> learntReads(int Seq) {
			return this.learntReads.get(Seq);
		}
	}







