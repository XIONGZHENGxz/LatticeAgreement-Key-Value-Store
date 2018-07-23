package la.mgla;

import org.junit.Test;

import static org.junit.Assert.*;

import java.util.*;

import la.common.*;

public class TestMgla {
	
	public MglaClient init(MglaServer[] servers, int numOp, int val_len, long max, double coef, double ratio) {
		int num = servers.length;
		for(int i = 0; i < num; i++) {
			servers[i] = new MglaServer(i);
			servers[i].init(max);
		}
		
		List<String> ops = Util.ops_generator(numOp, max, val_len, coef, ratio);
		MglaClient client  = new MglaClient(ops);
		return client;
	}

	public void cleanup(MglaServer[] servers) {
		for(int i = 0; i < servers.length; i++) {
			servers[i].close();
		}
		try {
			Thread.sleep(20);
		} catch (Exception e) {}
	}

 	//wait paxos to decide
	public boolean waitDecide(MglaServer[] servers, Op op, int wanted){
		int waitInterval = 100;
		int iter = 50;
		int num = -1;
		while(iter-->0){
			num = numOfDecided(servers, op);
			if(num >= wanted) return true;
			try{
				Thread.sleep(waitInterval);
			} catch(InterruptedException e){
				e.printStackTrace();
			}
		}
		return false;
	}

	public  int numOfDecided(MglaServer[] servers, Op op){
		int count = 0;
		for(int i = 0; i < servers.length; i++) {
			if(servers[i].proposer.learntValues.contains(op)) count ++;
		}
		return count;
	}		

	@Test
	public void test1() {
		MglaServer[] servers = new MglaServer[3];
		int num_ops = 1000;
		MglaClient c = init(servers, num_ops, 3, 1000, -1.0, 0.5);

		for(String opStr : c.ops) {
			String[] item = opStr.split("\\s");
			Op op;
			if(item[0].equals("put")) op = new Op(item[0], item[1], item[2]);
			else op = new Op(item[0], item[1], "");

			Response resp = c.executeOp(op);
			if(item[0].equals("put")) 
				assertTrue(waitDecide(servers, op, 3));
		}
		//cleanup(servers);
	}
			
}
			
