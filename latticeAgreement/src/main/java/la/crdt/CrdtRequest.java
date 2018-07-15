package la.crdt;
import java.io.Serializable;

import la.common.Request;
import la.common.Op;

class CrdtRequest extends Request{
	public LWWMap map;
	public int logic_time;

	public CrdtRequest(String type, Op op, LWWMap map) {
		super(type, op);
		this.map = map;
	}

	public CrdtRequest(String type, Op op, LWWMap map, int logic_time) {
		super(type, op);
		this.map = map;
		this.logic_time = logic_time;
	}

	public String toString() {
		return op + " "+ logic_time;
	}
	
}
	
