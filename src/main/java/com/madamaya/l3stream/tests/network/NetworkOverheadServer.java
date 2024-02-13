package com.madamaya.l3stream.tests.network;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class NetworkOverheadServer {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException();
        }
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);
        int data_num = Integer.parseInt(args[2]);

        Map<Long, Long> map = new HashMap<>();
        try {
            ServerSocket serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(hostname, port));

            int count = 0;
            while (++count <= data_num) {
                Socket socket = serverSocket.accept();
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                long start = Long.valueOf(br.readLine());
                long end = System.currentTimeMillis();
                long time = end - start;
                // System.out.println(count + ": " + time);
                map.put(time, map.getOrDefault(time, 0L) + 1);

                br.close();
                socket.close();
            }
            System.out.println(map);
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}
