package la.crdt;
import java.io.Serializable;

public class TimeStamp implements Serializable{
	private static final long serialVersionUID = 11L;
	public int id;
	public long clock;

	public TimeStamp(int id, long clock) {
		this.id = id;
		this.clock = clock;
	}


	public String toString() {
		return "("+this.id + " " + this.clock+")";
	}

}

