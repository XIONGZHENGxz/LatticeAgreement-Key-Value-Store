package la.jpaxos;

import java.io.Serializable;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

public class MapCommand implements Serializable {
	public String key;
	public String val;
	
	public MapCommand(String key, String val) {
		this.key = key;
		this.val = val;
	}

	public MapCommand(byte[] value) {
		MapCommand com = null;
		ByteArrayInputStream bis = null;
		ObjectInputStream ois = null;

		try {
			bis = new ByteArrayInputStream(value);
			ois = new ObjectInputStream(bis);

			com = (MapCommand) ois.readObject();
		} catch (Exception e) {
			return;
		} finally {
			try {
			if(bis != null) bis.close();
			if(ois != null) ois.close();
			} catch(Exception e){}
		}

		this.key = com.key;
		this.val = com.val;
	}

	public String getKey() {
		return this.key;
	}

	public String getVal() {
		return this.val;
	}

	public boolean isValid() {
		return key != null;
	}

	public String toString() {
		return this.key + " " + this.val;
	}

}
