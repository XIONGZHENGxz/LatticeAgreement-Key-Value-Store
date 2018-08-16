package la.crdt;
import java.io.Serializable;

public class Entry implements Serializable{
	private static final long serialVersionUID = 11L;
	public String key;
	public String val;
	public TimeStamp ts;

	public Entry(String key, String value, TimeStamp ts) {
		this.key = key;
		this.val = value;
		this.ts = ts;
	}

}
