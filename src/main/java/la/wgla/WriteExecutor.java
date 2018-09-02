package la.wgla;

import la.common.Message;
import la.common.Op;
import la.common.Response;
import la.common.Result;
import la.common.Messager;
import java.util.Queue;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;


public class WriteExecutor extends Thread {
	public GlaServer server;
	public Queue<Op> outQueue;
	public ReentrantLock lock;
	public Condition cond;

	public WriteExecutor (GlaServer s) {
		this.server = s;
		outQueue = new LinkedList<>();
		lock = new ReentrantLock();
		cond = lock.newCondition();
	}
	
	public void wake() {
		try {
			lock.lock();
			cond.signalAll();
		}catch (Exception e) {}
		finally {
			lock.unlock();
		}
	}

	public void add(Op write) {
		if(outQueue.isEmpty()) {
			outQueue.offer(write);
			this.wake();
		} else outQueue.offer(write);
	}
			
	public void run() {
		try {
			lock.lock();
			while(true) {
				while(outQueue.isEmpty()) {
					cond.await();
				}
				Op op  = outQueue.poll();
				while(op != null) {
					Messager.sendMsg(new Response(Result.TRUE, ""), server.socketAcceptor.socketMap.get(op.id));
					op = outQueue.poll();
				}
			}
		} catch (Exception e) { 
			e.printStackTrace();
		}
		finally {
			lock.unlock();
		}
	}
}
