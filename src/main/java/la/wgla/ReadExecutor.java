package la.wgla;

import la.common.Message;
import la.common.Messager;
import java.util.Queue;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;


public class ReadExecutor extends Thread {
	public GlaServer server;
	public volatile Queue<Message> outQueue;
	public ReentrantLock lock;
	public Condition cond;

	public ReadExecutor (GlaServer s) {
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

	public void add(Message msg) {
		if(outQueue.isEmpty()) {
			outQueue.offer(msg);
			this.wake();
		} else outQueue.offer(msg);
	}
			
	public void run() {
		try {
			lock.lock();
			while(true) {
				while(outQueue.isEmpty()) {
					cond.await();
				}
				Message msg = outQueue.poll();
				while(msg != null) {
					Messager.sendMsg(msg.resp, msg.channel);
					msg = outQueue.poll();
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
