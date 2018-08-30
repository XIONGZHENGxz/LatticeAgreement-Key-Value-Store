package la.network;
import java.nio.channels.SocketChannel;

import la.common.Util;
import java.nio.ByteBuffer;

public class Socket {
	public int socketId;
	public SocketChannel socket;
	public ByteBuffer buffer;
	public int want;
	public byte[] res;

	public Socket(int id, SocketChannel socket) {
		socketId = id;
		this.socket = socket;
		this.buffer = ByteBuffer.allocateDirect(10 * Util.requestSize);
		res = null;
		want = 0;
	}
}
