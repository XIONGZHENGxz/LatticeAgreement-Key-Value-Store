package la.common;
import java.io.Serializable;
public class Op implements Serializable {
	public static final long serialVersionUID = 11L;
	
	public String type;
	public String key;
	public String val;

	public Op(String type) {
		this.type = type;
		this.key = "";
		this.val = "";
	}

	public Op(String type, String key, String val) {
		this.type = type;
		this.key = key;
		this.val = val;
	}

	public String toString() {
		return this.type + " " + this.key + " " + this.val;
	}

	@Override
	public boolean equals(Object o) {
		if(o == this) return true;
		
		if(!(o instanceof Op)) return false;

		Op that = (Op) o;
		boolean equal = that.type.equals(this.type) && that.key.equals(this.key) && that.val.equals(this.val);
		return equal;
	}

	@Override
	public int hashCode() {
		return type.hashCode() + key.hashCode() + val.hashCode() ;
	}
}
