package la.wgla;

import java.io.Serializable;
import java.util.Set;
import java.util.HashSet;

import la.common.Op;

public class Request implements Serializable{
	private final static long serialVersionUID=11L;
	public String type;
	public Set<Op> writes;
	public Set<Op> reads;
	public Set<Op> learntWrites;
	public Set<Op> learntReads;
	public int round;
	public int seq;
	public int me;
	public Set<Integer> received;

	public Request() {}

	public Request(String type, int me) {
		this.type = type;
		this.me = me;
	}

	public Request(String type, Set<Op> writes) {
		this.type = type;
		this.writes = writes;
	}
	
	public Request(String type, Set<Integer> all, Set<Op> writes, Set<Op> reads, int r, int s, int me) {
		this.type = type;
		this.writes = writes;
		this.received = all;
		this.reads = reads;
		this.seq = s;
		this.me = me;
		this.round = r;
	}

	public Request(String type, Set<Op> writes, Set<Op> reads, int r, int s, int me) {
		this.type = type;
		this.writes = writes;
		this.reads = reads;
		this.seq = s;
		this.me = me;
		this.round = r;
	}

	public Request(String type, Set<Op> writes, Set<Op> reads, Set<Op> learntWrites, Set<Op> learntReads, int r, int s, int me) {
		this.type = type;
		this.writes = writes;
		this.reads = reads;
		round = r;
		this.learntWrites = learntWrites;
		this.learntReads = learntReads;
		seq = s;
		this.me = me;
	}

	public String toString() {
		return type + " "  +writes +" "+ reads+ " " + round + " " + seq + " "  + " from " + this.me;
	}
	
}
	
