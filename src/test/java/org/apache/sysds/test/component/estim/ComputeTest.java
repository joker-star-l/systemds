package org.apache.sysds.test.component.estim;

import org.junit.Test;

import java.util.Random;
import java.util.SplittableRandom;

public class ComputeTest {

    @Test
    public void test() {
        long start = System.currentTimeMillis();
        int[] a = new int[Integer.MAX_VALUE / 4];
        Random random = new Random();
        SplittableRandom random1 = new SplittableRandom();
        for (int i = 0; i < a.length; i++) {
//            a[i] = random.nextDouble() >= 0.5 ? 1 : 0;
//            a[i] = Math.random() >= 0.5 ? 1 : 0;
//            a[i] = i;
//            a[i] = (int) Math.round(i / 3.0);
            a[i] = random1.nextDouble(0, 1) >= 0.5 ? 1 : 0;
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start) + "ms");
    }

    @Test
    public void test2() {
        int max = Integer.MAX_VALUE;
        long max2 = (long) max * max;
        System.out.println(max2);
    }
}
