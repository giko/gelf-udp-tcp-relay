package org.kluge.gelf;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by giko on 2/16/15.
 */
public class SocketFactory {
    private String address;
    private Integer port;
    
    private Long lastAccess;
    Socket socket;

    public SocketFactory(String address, Integer port) {
        this.address = address;
        this.port = port;
    }

    public Socket getSocket() throws IOException {
        if (socket == null || System.currentTimeMillis() - lastAccess > 1000*60) {
            if (socket != null && !socket.isClosed()){
                socket.close();
            }

            socket = new Socket(address, port);
        }

        lastAccess = System.currentTimeMillis();
        return socket;
    }
    
    public void reset(){
        socket = null;
    }
}
