package com.oltpbenchmark.benchmarks.tpcclike.util;

public class PartSupp {

    private final int partkey;
    private final int suppkey;

    public PartSupp(int partkey, int suppkey) {
        this.partkey = partkey;
        this.suppkey = suppkey;
    }

    public int getPartKey() {
        return partkey;
    }

    public int getSuppKey() {
        return suppkey;
    }

}
