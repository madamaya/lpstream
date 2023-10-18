package com.madamaya.l3stream.cpstore;

import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CpManagerClient extends RichMapFunction<ObjectNode, ObjectNode> implements CheckpointListener {
    ExperimentSettings settings;
    // JedisPool jp;
    // Jedis jedis;
    // int pallarelism;
    // int numOfSOp;
    public CpManagerClient(ExperimentSettings settings) {
        this.settings = settings;
        // this.pallarelism = settings.maxParallelism();
        // this.numOfSOp = settings.sourcesNumber();
    }

    @Override
    public ObjectNode map(ObjectNode jsonNodes) throws Exception {
        return null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sendMessage(settings, getRuntimeContext().getJobId().toHexString());

        /*
        jp = new JedisPool(settings.getRedisIp(), settings.getRedisPort());
        try {
            jedis = jp.getResource();
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
         */
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        sendMessage(settings, Long.toString(l));
        // updateTs(jedis, pallarelism, numOfSOp);
    }

    private void sendMessage(ExperimentSettings settings, String message) throws Exception {
        try(Socket socket = new Socket()) {
            // 接続開始
            socket.connect(new InetSocketAddress(settings.getCpMServerIP(), settings.getCpMServerPort()));

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            bw.write(message);
            bw.close();
        } catch (Exception e) {
            System.err.println(e);
            throw new Exception("CpManager: sendMessage");
        }
    }

    private static void updateTs(Jedis jedis, int pallarelism, int numOfSOp) throws InterruptedException {
        Set<String> keys = jedis.keys("*");
        HashMap<String, Tuple2<Long, Integer>> hm = new HashMap<>();

        // Create Tuple2<String, Tuple2<Integer, Integer>>
        // Tuple2<cpid, Tuple2<minimumTS, the number of cpid tuples>>
        for (String key : keys) {
            if (!key.contains(",")) continue;

            String cpid = key.split(",")[0];
            long ts = Long.parseLong(jedis.get(key));

            Tuple2<Long, Integer> t2;
            if ((t2 = hm.get(cpid)) == null) {
                hm.put(cpid, Tuple2.of(ts, 1));
            } else {
                hm.put(cpid, Tuple2.of(Math.max(t2.f0, ts), t2.f1 + 1));
            }
        }

        for (Map.Entry<String, Tuple2<Long, Integer>> e : hm.entrySet()) {
            if (e.getValue().f1 != (pallarelism * numOfSOp)) {
                // retry(jedis, e.getKey(), pallarelism, numOfSOp);
            }
            jedis.set(e.getKey(), String.valueOf(e.getValue().f0));
        }
    }
}
