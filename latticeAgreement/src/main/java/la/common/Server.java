package la.common;

import java.util.List;
import java.util.ArrayList;

public class Server {
	
	public int me;
	public List<String> peers; 
	public List<Integer> ports;
	public int port;

	public Server(int id) {
		this.me = id;
		peers = new ArrayList<>();
		ports = new ArrayList<>();
		List<Integer> tmp = new ArrayList<>();
		Util.readConf(peers, tmp, ports);
		this.port = tmp.get(id);
	}

	public Server(int id, List<String> peers, int port, List<Integer> ports) {
		this.peers = peers;
		this.me = id;
		this.ports = ports;
		this.port = port;
	}

	public Response handleRequest(Request req) {
		return null;
	}
}
	
