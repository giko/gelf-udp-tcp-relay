package org.kluge.gelf;

import com.squareup.tape.FileObjectQueue;
import com.squareup.tape.ObjectQueue;
import io.netty.channel.socket.DatagramPacket;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.channel.ConnectionHandler;
import io.reactivex.netty.channel.ObservableConnection;
import io.reactivex.netty.protocol.udp.server.UdpServer;
import rx.Observable;
import rx.functions.Func1;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class Main {
    private static FileObjectQueue<byte[]> messageQueue;
    private static SocketFactory socketFactory;

    public static void main(String[] args) throws IOException {
        messageQueue = new FileObjectQueue<>(new File(args[0]),
                new FileObjectQueue.Converter<byte[]>() {
                    @Override public byte[] from(byte[] bytes) throws IOException {
                        return bytes;
                    }

                    @Override public void toStream(byte[] o, OutputStream outputStream) throws IOException {
                        outputStream.write(o);
                        outputStream.close();
                    }
                });
        socketFactory = new SocketFactory(args[1], Integer.valueOf(args[2]));

        final Runnable sender = new Runnable() {
            @Override public void run() {
                while (true) {
                    try {
                        while (messageQueue.size() > 0) {
                            OutputStream outputStream = socketFactory.getSocket().getOutputStream();
                            synchronized (messageQueue) {
                                outputStream.write(messageQueue.peek());
                                outputStream.flush();
                                messageQueue.remove();
                            }
                        }
                        return;
                    } catch (IOException e) {
                        System.out.println("Graylog is unavailable");
                        socketFactory.reset();
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }
        };
        final Thread[] senderThread = { new Thread() };

        messageQueue.setListener(new ObjectQueue.Listener<byte[]>() {
            @Override public void onAdd(ObjectQueue<byte[]> queue, byte[] entry) {
                if (!senderThread[0].isAlive()) {
                    senderThread[0] = new Thread(sender);
                    senderThread[0].start();
                }
            }

            @Override public void onRemove(ObjectQueue<byte[]> queue) {
            }
        });

        createServer(Integer.valueOf(args[3])).startAndWait();
    }

    public static UdpServer<DatagramPacket, DatagramPacket> createServer(Integer port) {
        UdpServer<DatagramPacket, DatagramPacket> server = RxNetty
                .createUdpServer(port, new ConnectionHandler<DatagramPacket, DatagramPacket>() {
                    @Override
                    public Observable<Void> handle(ObservableConnection<DatagramPacket, DatagramPacket> newConnection) {
                        return newConnection.getInput()
                                .flatMap(new Func1<DatagramPacket, Observable<Void>>() {
                                    @Override
                                    public Observable<Void> call(DatagramPacket received) {
                                        byte[] data = new byte[received.content().readableBytes() + 1];
                                        data[data.length - 1] = 0;
                                        received.content().readBytes(data, 0, received.content().readableBytes());
                                        synchronized (messageQueue) {
                                            messageQueue.add(data);
                                        }
                                        return Observable.empty();
                                    }
                                });
                    }
                });

        return server;
    }
}
