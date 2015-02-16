package org.kluge.gelf;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by giko on 2/16/15.
 */
public class SocketFactory {
    Socket socket;
    private String address;
    private Integer port;
    private Long lastAccess;

    public SocketFactory(String address, Integer port) {
        this.address = address;
        this.port = port;
    }

    public Socket getSocket() throws IOException {
        if (socket == null || System.currentTimeMillis() - lastAccess > 1000 * 60) {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }

            socket = new Socket(address, port);
            socket.setTcpNoDelay(true);
        } else {
            OutputStream outputStream = socket.getOutputStream();
            for (int i = 0; i < 5; i++) {
                outputStream.write(0);
                outputStream.flush();
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        lastAccess = System.currentTimeMillis();
        return socket;
    }

    public void reset() {
        socket = null;
    }
}
