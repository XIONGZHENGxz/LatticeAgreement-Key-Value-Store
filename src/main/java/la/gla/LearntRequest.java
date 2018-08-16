package la.gla;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import la.common.Op;
import java.util.HashMap;
import la.common.Request;

public class LearntRequest extends Request implements Serializable {	
	public int min;
	public int max;
	public static final long serialVersionUID=11L;
	public Map<Integer, Set<Op>> values;

	public LearntRequest(String type, int min, int max, int me) {
		super(type, me);
		this.min = min;
		this.max = max;
		values = new HashMap<>();
	}

	public void add(int seq, Set<Op> val) {
		this.values.put(seq, val);
		this.max ++;
	}
}
