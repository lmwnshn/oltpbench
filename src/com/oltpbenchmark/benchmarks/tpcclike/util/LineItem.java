package com.oltpbenchmark.benchmarks.tpcclike.util;

public class LineItem {

    private final int orderKey;
    private final int lineNumber;

    public LineItem(int orderKey, int lineNumber) {
        this.orderKey = orderKey;
        this.lineNumber = lineNumber;
    }

    public int getOrderKey() {
        return orderKey;
    }

    public int getLineNumber() {
        return lineNumber;
    }
}
