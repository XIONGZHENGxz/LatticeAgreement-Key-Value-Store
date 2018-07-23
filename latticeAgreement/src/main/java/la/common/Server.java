package la.common;

import java.util.List;
import java.util.ArrayList;

public class Server {
	
	public int me;
	public List<String> peers; 
	public List<Integer> ports;
	public int port;

	public Server(int id, String config) {
		this.me = id;
		this.peers = new ArrayList<>();
		this.ports = new ArrayList<>();
		List<Integer> tmp = new ArrayList<>();
		Util.readConf(peers, tmp, ports, config);
		this.port = tmp.get(id);
	}

	public Server(int id, List<String> peers, int port, List<Integer> ports) {
		this.peers = peers;
		this.me = id;
		this.ports = ports;
		this.port = port;
	}

	public Response handleRequest(Object req) {
		return null;
	}
}
	
