package com.oltpbenchmark.benchmarks.tpcclike.util;

public class Order {

    private final int orderKey;
    private final int custKey;
    private final double totalPrice;

    public Order(int orderKey, int custKey, double totalPrice) {
        this.orderKey = orderKey;
        this.custKey = custKey;
        this.totalPrice = totalPrice;
    }

    public int getOrderKey() {
        return orderKey;
    }

    public int getCustKey() {
        return custKey;
    }

    public double getTotalPrice() {
        return totalPrice;
    }
}
