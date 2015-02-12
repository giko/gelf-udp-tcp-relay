package org.kluge.gelf;

import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.ObjectQueue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.DatagramPacket;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.channel.ConnectionHandler;
import io.reactivex.netty.channel.ObservableConnection;
import io.reactivex.netty.pipeline.PipelineConfigurators;
import io.reactivex.netty.protocol.udp.server.UdpServer;
import rx.Observable;
import rx.functions.Func1;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;

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
    }

    public UdpServer<DatagramPacket, DatagramPacket> createServer(Integer port) {
        UdpServer<DatagramPacket, DatagramPacket> server = RxNetty
                .createUdpServer(port, new ConnectionHandler<DatagramPacket, DatagramPacket>() {
                    @Override
                    public Observable<Void> handle(
                            final ObservableConnection<DatagramPacket, DatagramPacket> newConnection) {
                        return newConnection.getInput().flatMap(new Func1<DatagramPacket, Observable<Void>>() {
                            @Override
                            public Observable<Void> call(DatagramPacket received) {
                                synchronized (messageQueue) {
                                    messageQueue.add(received.content().array());
                                }
                                return Observable.empty();
                            }
                        });
                    }
                });
        
        
        System.out.println("UDP hello server started...");
        return server;
    }
}
