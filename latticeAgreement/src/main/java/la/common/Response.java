package la.common;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class Response implements Serializable {
	private final static long serialVersionUID = 11L;	
	public boolean ok;
	public String val;

	public Map<Integer, Set<Op>> lv;

	public Response(boolean ok, String val) {
		this.ok  = ok;
		this.val = val;
	}

	public Response(boolean ok, Map<Integer, Set<Op>> lv) {
		this.ok  = ok;
		this.lv = lv;
	}
}
