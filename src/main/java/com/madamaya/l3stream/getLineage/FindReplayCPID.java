package com.madamaya.l3stream.getLineage;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class FindReplayCPID {
    /*
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("-1");
        }
        int outputTs = Integer.parseInt(args[0]);
        System.out.println("outputTs = " + outputTs);

        JedisPool pool = new JedisPool("localhost", 6379);
        final int N = 10;
        try (Jedis jedis = pool.getResource()) {
            // Write sample data
            jedis.flushDB();
            for (int cpid = 1; cpid <= N; cpid++) {
                for (int idx = 1; idx <= 5; idx++) {
                    jedis.set(cpid + "," + idx, String.valueOf(1000 * cpid + idx * 100));
                }
                jedis.set(String.valueOf(cpid), Integer.toString(1000 * cpid));
            }

            // Find replay cpid
            Set<String> allKeys = jedis.keys("*");
            System.out.println("Set<String> allKeys = " + allKeys);

            List<Integer> validKeys = getValidKeys(allKeys);
            System.out.println("List<String> validKeys = " + validKeys);

            int id = findID(validKeys, jedis, outputTs);
            System.out.println("id = " + id);

            System.exit(id);
        }
    }
     */

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

    public static int getReplayID(long outputTs, long maxWindowSize) {
        JedisPool pool = new JedisPool("localhost", 6379);
        try (Jedis jedis = pool.getResource()) {
            return findID(getValidKeys(jedis.keys("*")), jedis, outputTs - maxWindowSize);
        }
    }
}
