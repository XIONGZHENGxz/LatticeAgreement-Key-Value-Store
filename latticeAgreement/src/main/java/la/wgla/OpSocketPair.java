package la.wgla;

import la.common.Op;
import java.net.Socket;

public class OpSocketPair {
	public Op op;
	public Socket socket;

	public OpSocketPair(Op op, Socket socket) {
		this.op = op;
		this.socket = socket;
	}

}
