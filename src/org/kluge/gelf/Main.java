package org.kluge.gelf;

import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.ObjectQueue;
import io.netty.channel.socket.DatagramPacket;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.udp.server.UdpServer;
import rx.Observable;
import rx.functions.Func1;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class Main {
    private static FileObjectQueue<byte[]> messageQueue;

    public static void main(String[] args) throws IOException {
        Socket clientSocket = new Socket("gray", 9003);
        clientSocket.setKeepAlive(true);
        OutputStream outToServer = clientSocket.getOutputStream();

        messageQueue = new FileObjectQueue<>(new File("fallback"),
                new FileObjectQueue.Converter<byte[]>() {
                    @Override public byte[] from(byte[] bytes) throws IOException {
                        return bytes;
                    }

                    @Override public void toStream(byte[] o, OutputStream outputStream) throws IOException {
                        outputStream.write(o);
                        outputStream.close();
                    }
                });
        Thread sender = new Thread(() -> {
            while (messageQueue.size() != 0) {
                try {
                    synchronized (messageQueue) {
                        outToServer.write(messageQueue.peek());
                        outToServer.flush();
                        messageQueue.remove();
                    }
                } catch (IOException e) {
                    System.out.println("Graylog is unavailable");
                }
            }
        });

        messageQueue.setListener(new ObjectQueue.Listener<byte[]>() {
            @Override public void onAdd(ObjectQueue<byte[]> queue, byte[] entry) {
                sender.start();
            }

            @Override public void onRemove(ObjectQueue<byte[]> queue) {

            }
        });
        createServer(9003).startAndWait();
    }

    public static UdpServer<DatagramPacket, DatagramPacket> createServer(Integer port) {
        UdpServer<DatagramPacket, DatagramPacket> server = RxNetty
                .createUdpServer(port, newConnection -> newConnection.getInput()
                        .flatMap(new Func1<DatagramPacket, Observable<Void>>() {
                            @Override
                            public Observable<Void> call(DatagramPacket received) {
                                synchronized (messageQueue) {
                                    messageQueue.add(received.content().array());
                                }
                                return Observable.empty();
                            }
                        }));

        return server;
    }
}
