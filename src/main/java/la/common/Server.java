package la.common;

import java.util.List;
import java.util.ArrayList;

public class Server {
	
	public int me;
	public volatile boolean fail;
	public List<String> peers; 
	public List<Integer> ports;
	public int port;

	public Server(int id, String config, boolean fail) {
		this.me = id;
		this.peers = new ArrayList<>();
		this.ports = new ArrayList<>();
		List<Integer> tmp = new ArrayList<>();
		Util.readConf(peers, tmp, ports, config);
		this.port = tmp.get(id);
		this.fail = fail;
	}

	public Server(int id, List<String> peers, int port, List<Integer> ports) {
		this.peers = peers;
		this.me = id;
		this.ports = ports;
		this.port = port;
	}

	public void applyWrites() {
	}
	public void applyReads() {
	}

	public Response handleRequest(Object req) {
		return null;
	}

	public Response handleRequest(int socketId, Object req) {
		return null;
	}
}
	
