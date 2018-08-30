package la.common;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.io.DataInputStream;

public class Op implements Serializable {
	public static final long serialVersionUID = 11L;
	public int id = -1;
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

	public Op(int id, Type type, String key, String val) {
		this.id = id;
		this.type = type;
		this.key = key;
		this.val = val;
	}

	public Op(DataInputStream input) throws Exception{
			this.type = Type.values()[input.readInt()];
			int kLen = input.readInt();
			byte[] keyBytes = new byte[kLen];
			input.read(keyBytes, 0, kLen);
			this.key = new String(keyBytes, "UTF-8");
			int vLen = input.readInt();
			byte[] valBytes = new byte[vLen];
			this.val = new String(valBytes, "UTF-8");
	}

	public ByteBuffer toBytes() {
		byte[] keyByte = Util.toByteArray(key);
		byte[] valByte = Util.toByteArray(val);
		int len = 16 + keyByte.length + valByte.length;
		ByteBuffer buffer = ByteBuffer.allocate(len);
		buffer.putInt(len - 4);
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
