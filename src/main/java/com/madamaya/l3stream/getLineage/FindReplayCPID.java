package com.madamaya.l3stream.getLineage;

import com.madamaya.l3stream.conf.L3Config;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.regex.Pattern;

public class FindReplayCPID {
    private static int findID(Jedis jedis, int numOfSOp, long timestamp) throws InterruptedException {
        Set<String> keys = jedis.keys("*");
        HashMap<Integer, Tuple2<Long, Integer>> hm = new HashMap<>();
        int cpidMax = Integer.MIN_VALUE;

        // Create Tuple2<Integer, Tuple2<Integer, Integer>>
        // Tuple2<cpid, Tuple2<minimumTS, the number of cpid tuples>>
        for (String key : keys) {
            if (!key.contains(",")) continue;

            int cpid = Integer.parseInt(key.split(",")[0]);
            cpidMax = Math.max(cpidMax, cpid);
            long ts = Long.parseLong(jedis.get(key));

            Tuple2<Long, Integer> t2;
            if ((t2 = hm.get(cpid)) == null) {
                hm.put(cpid, Tuple2.of(ts, 1));
            } else {
                hm.put(cpid, Tuple2.of(Math.max(t2.f0, ts), t2.f1 + 1));
            }
        }

        for (int cpid = cpidMax; cpid > 0; cpid--) {
            Tuple2<Long, Integer> element = hm.get(cpid);
            if (element != null && element.f1 == (L3Config.PARALLELISM * numOfSOp) && element.f0 < timestamp) {
                if (element.f0 < 0) {
                    return 0;
                } else {
                    return cpid;
                }
            }
        }

        return 0;
    }

    public static List<Integer> getValidKeys(Set<String> allKeys) {
        Pattern p = Pattern.compile("^[0-9]+$");
        List<Integer> list = new ArrayList<>();
        for (String key : allKeys) {
            if (p.matcher(key).matches()) {
                list.add(Integer.parseInt(key));
            }
        }
        Collections.sort(list, Collections.reverseOrder());
        return list;
    }

    public static int findID(List<Integer> validKeys, Jedis jedis, long timestamp) {
        for (int idx = 0; idx < validKeys.size(); idx++) {
            long ts = Long.parseLong(jedis.get(String.valueOf(validKeys.get(idx))));
            if (ts < timestamp) {
                return validKeys.get(idx);
            }
        }
        return 0;
    }

    // defalut: redisIP = "localhost", redisPort = 6379
    public static int getReplayID(long outputTs, long maxWindowSize, int numOfSource) {
        JedisPool pool = new JedisPool(L3Config.REDIS_IP, L3Config.REDIS_PORT);
        try (Jedis jedis = pool.getResource()) {
            return findID(jedis, numOfSource, outputTs - maxWindowSize);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
