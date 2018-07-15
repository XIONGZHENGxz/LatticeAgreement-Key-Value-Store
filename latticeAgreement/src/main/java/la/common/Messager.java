package la.common;
import java.io.*;

import java.net.Socket;
import java.net.InetAddress;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;

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
	public static void sendPacket(Object msg, String host, int port) {
		try {
			DatagramSocket socket = new DatagramSocket();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(msg);
			byte[] data = baos.toByteArray();
			DatagramPacket packet  = new DatagramPacket(data, data.length, InetAddress.getByName(host), port);
			socket.send(packet);
		} catch(Exception e) {
			e.printStackTrace();
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
			while(resp == null && currTime-startTime < Util.TIMEOUT) {
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
			return null;
		}
	}

	public  static  void sendMsg(Object msg, Socket socket) {
		ObjectOutputStream out;
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(msg);
			out.flush();
		} catch(Exception e){
		}
	}

	//if flag == 0, read object, else read string
	public static Object getMsg(Socket socket) {
		Object resp = null;
		ObjectInputStream inputStream;
		try {
			inputStream = new ObjectInputStream(socket.getInputStream());
			resp = inputStream.readObject();
		} catch(Exception e){
			e.printStackTrace();
		}
		return resp;
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
}
