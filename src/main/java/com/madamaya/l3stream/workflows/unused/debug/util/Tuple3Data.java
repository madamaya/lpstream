package com.madamaya.l3stream.workflows.unused.debug.util;

import org.apache.flink.api.java.tuple.Tuple3;

public class Tuple3Data<T1, T2, T3> {
    Tuple3<T1, T2, T3> tuple3;
    Long stimulus;

    public Tuple3Data(T1 t1, T2 t2, T3 t3) {
        tuple3 = Tuple3.of(t1, t2, t3);
        stimulus = System.nanoTime();
    }
}
