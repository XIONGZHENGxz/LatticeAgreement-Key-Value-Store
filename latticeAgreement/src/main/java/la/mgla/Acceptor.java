package la.mgla;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import la.common.*;

public class Acceptor {

	/* associated server */
	public MglaServer s;

	/* accepted value */
	public Set<Op> acceptVal; 

	/* delta accepted values for each proposer, record newly accepted values since last proposal */
	public Map<Integer, Set<Op>> accDeltas; 

	public List<String> peers;

	public List<Integer> ports;

	public ReentrantLock lock;

	public Acceptor (MglaServer s) {
		this.s = s;
		this.accDeltas = new HashMap<>();
		this.peers = s.peers;
		this.ports = s.ports;
		this.lock = new ReentrantLock();
		this.acceptVal = new HashSet<>();
	}

	public void updateDeltas(Set<Op> val, int except) {
		for(int pid : this.accDeltas.keySet()) {
			this.accDeltas.get(pid).addAll(val);
		}
		this.accDeltas.put(except, new HashSet<Op>());
	}

	public void handleProposal(Request req) {
		try {
			lock.lock();
			Set<Op> prop = req.val;
			int propId = req.me;
			this.acceptVal.addAll(prop);
			if(!this.accDeltas.containsKey(propId)) {
				this.updateDeltas(prop, propId);	
				Request resp = new Request("reject", this.acceptVal, req.seq, this.s.me);
				Messager.sendPacket(resp, this.peers.get(req.me), this.ports.get(req.me));
				return;
			}
			if(prop.containsAll(this.accDeltas.get(propId))) {
				prop.removeAll(this.accDeltas.get(propId));
				this.updateDeltas(prop, propId);
				Request resp = new Request("accept", null, req.seq, this.s.me);
				Messager.sendPacket(resp, this.peers.get(req.me), this.ports.get(req.me));
			} else {
				Request resp = new Request("reject", this.accDeltas.get(propId), req.seq, this.s.me);
				Messager.sendPacket(resp, this.peers.get(req.me), this.ports.get(req.me));
				this.updateDeltas(prop, propId);
			}
		} finally {
			lock.unlock();
		}
	}
}
