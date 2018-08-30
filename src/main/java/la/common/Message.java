package la.common;

import java.nio.channels.SocketChannel;

public class Message {
	public Response resp;
	public SocketChannel channel;

	public Message(Response resp, SocketChannel channel) {
		this.resp =resp;
		this.channel = channel;
	}
}
