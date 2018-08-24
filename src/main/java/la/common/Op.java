package la.common;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class Op implements Serializable {
	public static final long serialVersionUID = 11L;
	public Type type;
	public String key;
	public String val;


	public Op(){}

	public Op(Type type) {
		this.type = type;
		this.key = "";
		this.val = "";
	}

	public Op(Type type, String key, String val) {
		this.type = type;
		this.key = key;
		this.val = val;
	}

	public ByteBuffer toBytes() {
		System.out.println(key + " " + val);
		byte[] keyByte = Util.toByteArray(key);
		byte[] valByte = Util.toByteArray(val);
		ByteBuffer buffer = ByteBuffer.allocate(12 + keyByte.length + valByte.length);
		buffer.putInt(type.ordinal());
		buffer.putInt(keyByte.length);
		buffer.put(keyByte);
		buffer.putInt(valByte.length);
		buffer.put(valByte);
		return buffer;
	}

	public String toString() {
		return this.type + " " + this.key + " " + this.val;
	}

	@Override
	public boolean equals(Object o) {
		if(o == this) return true;
		
		if(!(o instanceof Op)) return false;

		Op that = (Op) o;
		boolean equal = that.type == this.type && that.key.equals(this.key) && that.val.equals(this.val);
		return equal;
	}

	@Override
	public int hashCode() {
		return type.ordinal() + key.hashCode() + val.hashCode() ;
	}
}
