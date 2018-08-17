package la.common;

import java.io.Serializable;
import java.util.Set;

public class Request implements Serializable{
	private final static long serialVersionUID=11L;

	public String type;
	public Set<Op> val;
	public Set<Op> learnt;
	public Set<Op> reads;
	public Set<Op> writes;
	public int round;
	public int seq;
	public Op op;
	public int me;

	public Request() {}

	public Request(String type) {
		this.type = type;
	}

	public Request(String type, int me) {
		this.type = type;
		this.me = me;
	}

	public Request(String type, Set<Op> val) {
		this.type = type;
		this.val = val;
	}
	
	public Request(String type, Set<Op> val, int s, int me) {
		this.type = type;
		this.val = val;
		this.seq = s;
		this.me = me;
	}

	public Request(String type, Set<Op> val, int r, int s, int me) {
		this.type = type;
		this.val = val;
		round = r;
		seq = s;
		this.me = me;
	}

	public Request(String type, Set<Op> val, Set<Op> learnt, int r, int s, int me) {
		this.type = type;
		this.val = val;
		this.learnt = learnt;
		round = r;
		seq = s;
		this.me = me;
	}

	public Request(String type, Set<Op> val, Set<Op> writes, Set<Op> reads, int r, int s, int me) {
		this.type = type;
		this.val = val;
		this.writes = writes;
		this.reads = reads;
		round = r;
		seq = s;
		this.me = me;
	}


	public Request(String type, Op val) {
		this.type = type;
		this.op = val;
	}

	public Request(String type, Op val, int me) {
		this.type = type;
		this.op = val;
		this.me = me;
	}

	public String toString() {
		return type + " "  +op+ " " + round + " " + seq + " "  + " from " + this.me;
	}
	
}
	
