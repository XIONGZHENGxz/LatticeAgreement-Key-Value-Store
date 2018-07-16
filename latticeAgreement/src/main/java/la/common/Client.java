package la.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import la.common.Util;

public class Client extends Thread{

	public List<String> servers;
	public List<Integer> ports;
	public List<String> ops;

	public Client(List<String> ops, String config) { 
		this.servers = new ArrayList<>();
		this.ports = new ArrayList<>();
		Util.readConf(servers, ports, config);
		this.ops = ops;
	}

	public Response handleRequest(Request req) {
		return null;
	}

	public Response executeOp (Op op) {
		while(true) {
			int s = Util.decideServer(this.servers.size());
			Request req = new Request("client", op);
			Response resp  = (Response) Messager.sendAndWaitReply(req, this.servers.get(s), this.ports.get(s));
			if(resp != null && resp.ok) return resp;
		}
	}
	
	@Override
	public void run() {
		for(String op : this.ops) {
			String[] item = op.split("\\s");
			Op ope = null;
			if(item[0].equals("put")) 
				ope = new Op(item[0], item[1], item[2]);
			else ope = new Op(item[0], item[1], "");
			this.executeOp(ope);
		}
	}
}
