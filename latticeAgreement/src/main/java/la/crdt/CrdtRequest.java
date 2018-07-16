package la.crdt;
import java.io.Serializable;

import la.common.Request;
import la.common.Op;

class CrdtRequest implements Serializable {
	private static final long serialVersionUID=11L;
	public String type;
	public Op op;
	public LWWMap map;
	public int logic_time;

	public CrdtRequest(String type, Op op) {
		this.type = type;
		this.op = op;
	}

	public CrdtRequest(String type, LWWMap map) {
		this.type = type;
		this.map = map;
	}

	public CrdtRequest(String type, LWWMap map, int logic_time) {
		this.type = type;
		this.map = map;
		this.logic_time = logic_time;
	}

	public String toString() {
		return op + " "+ logic_time;
	}
	
}
	
