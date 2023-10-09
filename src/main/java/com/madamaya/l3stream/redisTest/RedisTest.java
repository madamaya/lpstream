package com.madamaya.l3stream.redisTest;

import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RedisTest {
    public static void main(String[] args) throws Exception {
        JedisPool pool = new JedisPool("localhost", 6379);
        final int N = 10;
        try (Jedis jedis = pool.getResource()) {
            jedis.flushDB();
            for (int cpid = 1; cpid <= N; cpid++) {
                for (int instanceid = 0; instanceid < 4; instanceid++) {
                    for (int k = 0; k < 3; k++) {
                        jedis.set(Integer.toString(cpid) + "," + Integer.toString(instanceid) + "," + k, Integer.toString(1000 * cpid + instanceid * 100 + k * 10));
                    }
                }
            }
            updateTs(jedis, 4, 3);

            int pts = Integer.MAX_VALUE;
            boolean f = true;
            for (int cpid = N; cpid > 0; cpid--) {
                String getResult = jedis.get(Integer.toString(cpid));
                if (getResult == null) {
                    System.out.println("getResult == null (cpid = " + cpid + ")");
                    continue;
                }
                int cts = Integer.parseInt(getResult);
                if (cts > pts) {
                    System.out.println("ERROR: cts > pts (cpid = " + cpid + ")");
                    f = false;
                } else if (cts == pts) {
                    System.out.println("WARNING: cts == pts (cpid = " + cpid + ")");
                }
                pts = cts;
            }
            if (f) {
                System.out.println("✅");
            } else {
                System.out.println("❌");
            }
        }
    }

    private static void updateTs(Jedis jedis, int pallarelism, int numOfSOp) throws InterruptedException {
        Set<String> keys = jedis.keys("*");
        HashMap<String, Tuple2<Integer, Integer>> hm = new HashMap<>();

        // Create Tuple2<String, Tuple2<Integer, Integer>>
        // Tuple2<cpid, Tuple2<minimumTS, the number of cpid tuples>>
        for (String key : keys) {
            if (!key.contains(",")) continue;

            String cpid = key.split(",")[0];
            int ts = Integer.parseInt(jedis.get(key));

            Tuple2<Integer, Integer> t2;
            if ((t2 = hm.get(cpid)) == null) {
                hm.put(cpid, Tuple2.of(ts, 1));
            } else {
                hm.put(cpid, Tuple2.of(Math.min(t2.f0, ts), t2.f1 + 1));
            }
        }

        for (Map.Entry<String, Tuple2<Integer, Integer>> e : hm.entrySet()) {
            if (e.getValue().f1 != (pallarelism * numOfSOp)) {
                retry(jedis, e.getKey(), pallarelism, numOfSOp);
            }
            jedis.set(e.getKey(), String.valueOf(e.getValue().f0));
        }
    }

    private static void retry(Jedis jedis, String cpid, int pallarelism, int numOfSOp) throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            Set<String> keys = jedis.keys(cpid + "*");
            Tuple2<Integer, Integer> t2 = Tuple2.of(Integer.MAX_VALUE, 0);
            for (String key : keys) {
                if (!key.contains(",")) continue;
                t2.f0 = Math.min(t2.f0, Integer.parseInt(jedis.get(key)));
                t2.f1++;
            }

            if (t2.f1 == (pallarelism * numOfSOp)) {
                jedis.set(cpid, String.valueOf(t2.f0));
                return;
            }
            Thread.sleep(10);

        }
        throw new RuntimeException("retry was not completed.");
    }
}
