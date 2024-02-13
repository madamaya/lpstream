package com.madamaya.l3stream.tests.network;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class NetworkOverheadClient {
    public static void main(String[] args) throws Exception {
        if (args.length != 3 && args.length != 4) {
            throw new IllegalArgumentException();
        }
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);
        final int data_num = Integer.parseInt(args[2]);
        final int sleep_time = (args.length == 4) ? Integer.parseInt(args[3]) : 10;
        try {
            int count = 0;
            while (++count <= data_num) {
                // Socket
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(hostname, port));
                // BufferedWriter
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                // Sleep
                Thread.sleep(sleep_time);
                // Generate data
                String line = String.valueOf(System.currentTimeMillis());
                // Write data
                bw.write(line);
                // Print log
                System.out.print("\r" + count);
                // BW close
                bw.close();
                // Socket close
                socket.close();
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}
