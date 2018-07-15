package la.common;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * Created by xz on 6/7/17.
 */

public class ClientListener extends Thread{
    private int port;
    private ServerSocket serverSocket;
    private Client server;

    public ClientListener(Client server, int port) {
        this.port = port;
        this.server = server;
    }

    @Override
    public void run() {
        try{
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while(true) {
            try {
                Socket socket = serverSocket.accept();
				Thread t = new Thread(new ClientRequestHandler(this.server, socket));
				t.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class ClientRequestHandler implements Runnable {
	public Client server;
	public Socket s;
	public ClientRequestHandler(Client server, Socket s) {
		this.server = server;
		this.s = s;
	}

	public void run() {
		Request request = (Request) Messager.getMsg(s);
        if(Util.DEBUG) System.out.println("get request: "+request.toString());
		server.handleRequest(request);
	}
}

