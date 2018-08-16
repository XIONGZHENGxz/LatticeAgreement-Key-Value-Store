package la.crdt;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.io.Serializable;

import la.common.*;

public class LWWMap implements Serializable {
	public ReentrantLock lock;
	private final static long serialVersionUID=11L;
	public int uid;
	public HashMap<String, Entry> A;
	public HashMap<String, TimeStamp> R;

	public LWWMap(int uid) {
		this.uid = uid;
		this.A = new HashMap<String, Entry>();
		lock = new ReentrantLock();
		this.R = new HashMap<String, TimeStamp>();
	}

	public LWWMap(HashMap<String, Entry> a, HashMap<String, TimeStamp> r) {
		this.A = a;
		this.R = r;
	}

	public String get(String key) {
		
		if(!A.containsKey(key)) return null;
		else {
			if(Util.DEBUG) System.out.println("R ts..."+R.get(key));
			if(Util.DEBUG) System.out.println("A ts..."+A.get(key).ts);
			if(R.containsKey(key) && Util.compare(R.get(key), A.get(key).ts) > 0) return null;
			else return A.get(key).val;
		}
	}

	public void put(String key, String val) {
		Util.clock += 1;
		TimeStamp now = new TimeStamp(this.uid, Util.clock);
		Entry e = new Entry(key, val, now);
		this.A.put(key, e);
	}

	public void remove(String key) {
		Util.clock ++;
		TimeStamp now = new TimeStamp(this.uid, Util.clock);
		R.put(key, now);
	}

	public boolean merge(LWWMap remote) {
		boolean update = false;
		try {
			lock.lock();
			for(String key: remote.A.keySet()) {
				if(!A.containsKey(key)) {
					A.put(key, remote.A.get(key));
					if(Util.DEBUG) System.out.println("A not containsKey "+ key);
					update = true;
				} else if(Util.compare(A.get(key).ts, remote.A.get(key).ts) < 0) {
					A.put(key, remote.A.get(key));
					if(Util.DEBUG) System.out.println("A containsKey "+ key);
					update = true;
				} else {
					if(Util.DEBUG) System.out.println("local "+A.get(key).ts + "; remote" + remote.A.get(key).ts);
				}

			}

			for(String key: remote.R.keySet()) {
				if(!R.containsKey(key)) {
					R.put(key, remote.R.get(key));
					update = true;
					if(Util.DEBUG) System.out.println("merge remove(not containKey) "+key + " from "+remote.uid);
				} else if(Util.compare(R.get(key), remote.R.get(key)) < 0) {
					R.put(key, remote.R.get(key));
					update = true;
					if(Util.DEBUG) System.out.println("merge remove(smaller ts) "+key + " from "+remote.uid);
				} else {
					if(Util.DEBUG) System.out.println("R   local "+ R.get(key) + "; remote" + remote.R.get(key));
				}
			}
		} finally {
			lock.unlock();
		}
		return update;
	}

}











