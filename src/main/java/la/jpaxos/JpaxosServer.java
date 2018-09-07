package la.jpaxos;

import lsr.paxos.Paxos;
import lsr.paxos.ProposerImpl;
import lsr.paxos.network.TcpNetwork;
import lsr.paxos.replica.ClientBatchManager;
import lsr.service.SimplifiedService;
import lsr.paxos.replica.Replica;
import lsr.common.Configuration;

import java.lang.Process;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import la.common.*;

class JpaxosServer extends SimplifiedService {

	private HashMap<String, String> store;
	public Paxos paxos;

	public JpaxosServer() {
		this.store = new HashMap<>();
	}

	public JpaxosServer(HashMap<String, String> remote) {
		this.store = remote;
	}

	public void close() {
		if (paxos.isLeader()) {
			TcpNetwork net = (TcpNetwork) paxos.getNetwork();
			ProposerImpl proposer = (ProposerImpl) paxos.getProposer();
			ClientBatchManager cliBatch = proposer.getClientBatchManager();
			cliBatch.cleanStop();
			// Wait for all messages to be sent
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			net.closeAll();
			System.exit(1);
		}	
	}

	@Override 
		protected byte[] execute(byte[] value) {
			MapCommand command = new MapCommand(value);

			if(!command.isValid()) {
				this.close();	
			}
			if(Util.DEBUG) System.out.println("executing command "+ command);

			String oldVal = this.store.get(command.getKey());

			if(oldVal == null) oldVal = "";

			if(command.getVal() != null) store.put(command.getKey(), command.getVal());

			ByteArrayOutputStream bos = null;
			DataOutputStream dos = null;
			try {
				bos = new ByteArrayOutputStream();

				dos = new DataOutputStream(bos);

				dos.writeUTF(oldVal);
			} catch (Exception e) {
				e.printStackTrace();
			}

			return bos.toByteArray();
		}


	@Override 
		protected byte[] makeSnapshot() {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();

			try {
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(this.store);
			} catch (Exception e) {
				e.printStackTrace();
			}

			return bos.toByteArray();
		}

	@Override 
		protected void updateToSnapshot(byte[] snapshot) {
			ByteArrayInputStream bis = new ByteArrayInputStream(snapshot);

			ObjectInputStream ois = null;
			try {
				ois = new ObjectInputStream(bis);
				this.store = (HashMap<String, String>) ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	public static void main(String...args) throws IOException {
		int id = Integer.parseInt(args[0]);

		HashMap<String, String> map = Util.initMap(Integer.parseInt(args[1]));

		JpaxosServer js = new JpaxosServer(map);
		Replica replica = new Replica(new Configuration(args[2]), id, js);

		replica.start();
		js.paxos = replica.getPaxos();
	}
}
