package com.madamaya.l3stream.workflows.utils.test;

import com.madamaya.l3stream.workflows.utils.objects.MedianCalc;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class MedianCalcTest {
    public static void main(String[] args) throws Exception {
        Random random = new Random();

        int N = 100;
        boolean flag = true;
        for (int i = 0; i < N; i++) {
            MedianCalc mc = new MedianCalc();
            List<Long> list = new ArrayList<>();
            int dataNum = random.nextInt(10000000) + 1;
            System.out.println("(" + (i+1) + "/" + N + "), dataNum = " + dataNum);
            for (int j = 0; j < dataNum; j++) {
                long data = random.nextLong();
                mc.append(data);
                list.add(data);
            }

            list.sort(Comparator.naturalOrder());

            double med1 = mc.getMedian();
            double med2 = (list.size() % 2 == 1) ? list.get(list.size() / 2) : ((double) list.get((list.size()/2)-1) + list.get(list.size()/2)) / 2;
            boolean isSame = (long) (med1 * 1e10) == (long) (med2 * 1e10);

            flag &= isSame;
            if (!isSame) break;
        }

        if (flag) {
            System.out.println("✅");
        } else {
            System.out.println("❌");
        }
    }
}
