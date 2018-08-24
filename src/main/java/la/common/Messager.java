package la.common;
import java.io.*;

import java.net.Socket;
import java.net.InetAddress;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;

import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import java.nio.ByteBuffer;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class Messager {
	public boolean waitReply;
	public Object msg;	
	public Object resp;
	public int port;
	public String host;

	public Messager(boolean wait, Object msg, String host, int port) {
		waitReply = wait;
		this.msg = msg;
		this.host = host;
		this.port = port;
		this.resp = null;
	}

	//send msg via datagram socket
	public synchronized static void sendPacket(Object msg, String host, int port) {
		DatagramSocket socket = null; 
		try {
			socket = new DatagramSocket();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(msg);
			byte[] data = baos.toByteArray();
			DatagramPacket packet  = new DatagramPacket(data, data.length, InetAddress.getByName(host), port);
			socket.send(packet);
			System.out.println("sent ");
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(socket != null) {
				try {
					socket.close();
				} catch (Exception e) {}
			}
		}
	}


	//send msg
	public static boolean sendMsg(Object msg, String host, int port) {
		ObjectOutputStream out;
		Socket socket = new Socket();
		try {
			InetSocketAddress addr = new InetSocketAddress(host,port);
			socket.connect(addr, Util.TIMEOUT);
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(msg);
			out.flush();
		} catch(Exception e){
			return false;
		} finally {
			if(socket!=null) {
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return true;
	}
	/*	
		public void run() {
		if(waitReply) {
		while(true) {
		this.resp = sendAndWaitReply(msg, this.host, this.port);
		if(this.resp != null) break;
		try{
		Thread.sleep(10);
		} catch(Exception e) {
		e.printStackTrace();
		}
		}
		}
		}
	 */
	//send and wait for response
	public String sendAndWaitReply(String msg,String host,int port) {
		try {
			InetSocketAddress addr = new InetSocketAddress(host,port);
			Socket socket = new Socket();
			socket.connect(addr, Util.TIMEOUT);
			PrintStream ps = new PrintStream(socket.getOutputStream());
			ps.println(msg);
			BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String resp = null;
			long startTime = System.currentTimeMillis();
			long currTime = System.currentTimeMillis();
			while(resp == null && currTime-startTime < Util.TIMEOUT) {
				resp = br.readLine();
				currTime = System.currentTimeMillis();
			}
			return resp;
		} catch (IOException e) {
			return null;
		}
	}

	public Object sendAndWait(String msg, String host, int port) {
		try {
			InetSocketAddress addr = new InetSocketAddress(host,port);
			Socket socket = new Socket();
			socket.connect(addr, Util.TIMEOUT);
			PrintStream ps = new PrintStream(socket.getOutputStream());
			ps.println(msg);
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			Object resp = null;
			long startTime = Util.getCurrTime();
			long currTime = Util.getCurrTime();
			while(resp == null && currTime - startTime < Util.TIMEOUT) {
				resp = in.readObject();
				currTime = Util.getCurrTime();
			}
			return resp;
		} catch (IOException e) {
			return null;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Object sendAndWaitReply(Object msg, String host, int port) {
		try {
			InetSocketAddress addr = new InetSocketAddress(host,port);
			Socket socket = new Socket();
			socket.connect(addr, Util.TIMEOUT);
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(msg);
			out.flush();
			socket.setSoTimeout(Util.TIMEOUT);
			InputStream is = socket.getInputStream();
			ObjectInputStream in = new ObjectInputStream(is);
			Object resp = null;
			resp = in.readObject();
			return resp;
		} catch (Exception e) {
			return null;
		}
	}

	public  static  void sendMsg(Response msg, SelectionKey key) {
		SocketChannel socket = (SocketChannel) key.channel();
		ByteBuffer bb = msg.writeToBuffer();
		//System.out.println("bytebuffer... " + bb);
		bb.flip();
		try {
			int bytesWrite = socket.write(bb);
			//System.out.println("write success....");
			if(bytesWrite < 1) System.out.println("write failed...");
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public  static  void sendMsg(Object msg, Socket socket) {
		ObjectOutputStream out;
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(msg);
			out.flush();
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public static Op getMsg(SelectionKey key) {
		SocketChannel socket = (SocketChannel) key.channel();
		Op op = new Op();
		ByteBuffer buffer = ByteBuffer.allocate(48);
		int byteRead = -1;
		try {
			byteRead = socket.read(buffer);
			//System.out.println("received buffer..." + buffer + " " + byteRead);
			buffer.flip();
			if(byteRead == 0) return null; 
			if(byteRead < 0) {
				try {
					key.attach(null);
					key.cancel();
					key.channel().close();
				} catch (Exception e) {}
				return null;
			}
			op.type = Type.values()[buffer.getInt()];
			byte[] keyBytes = new byte[buffer.getInt()];
			buffer.get(keyBytes, 0, keyBytes.length);
			byte[] valBytes = new byte[buffer.getInt()];
			buffer.get(valBytes, 0, valBytes.length);
			op.key = new String(keyBytes, "UTF-8");
			op.val = new String(valBytes, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return op;
	}

	public  void sendMsg(String msg, String host, int port) {
		try {
			Socket socket = new Socket();
			InetSocketAddress addr = new InetSocketAddress(host,port);
			socket.connect(addr);
			PrintStream ps = new PrintStream(socket.getOutputStream());
			ps.println(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public  void sendMsg(String msg, Socket socket) {
		try {
			PrintStream ps = new PrintStream(socket.getOutputStream());
			ps.println(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public  String getRequest(Socket socket) {
		String resp = "";
		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			resp = reader.readLine();
		} catch(Exception e){
			e.printStackTrace();
		}
		return resp;
	}

	public static Object getMsg(Socket socket) {
		Object resp = null;
		ObjectInputStream inputStream;
		try {
			inputStream = new ObjectInputStream(socket.getInputStream());
			resp = inputStream.readObject();
		} catch(Exception e){
			return resp;
		}
		return resp;
	}

}
