package la.common;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import java.nio.ByteBuffer;
import java.io.DataInputStream;

public class Response implements Serializable {
	private final static long serialVersionUID = 11L;	
	public Result ok;
	public String val;
	
	public Response() {}
	public Response (Result ok, String val) {
		this.ok = ok;
		this.val = val;
	}

	public Response(DataInputStream input) throws Exception{
			//System.out.println("reading input stream...");
			ok = Result.values()[input.readInt()];
			//System.out.println("read type...");
			byte[] tmp = new byte[input.readInt()];
			input.readFully(tmp);
			val = new String(tmp, "UTF-8");
	}

	public ByteBuffer writeToBuffer() {
		byte[] tmp = Util.toByteArray(val);
		ByteBuffer bb = ByteBuffer.allocate(8 + tmp.length);
		bb.putInt(ok.ordinal());
		bb.putInt(tmp.length);
		bb.put(tmp);
		return bb;
	}

	public String toString() {
		return this.ok + " " + this.val;
	}
}
