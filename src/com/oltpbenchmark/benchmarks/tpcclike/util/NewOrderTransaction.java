package com.oltpbenchmark.benchmarks.tpcclike.util;

import java.sql.PreparedStatement;
import java.util.List;

public class NewOrderTransaction extends Transaction {

    private final Order order;
    private final List<LineItem> lineItems;

    public NewOrderTransaction(List<PreparedStatement> preparedStatements,
                               Order order, List<LineItem> lineItems) {
        super(preparedStatements);
        this.order = order;
        this.lineItems = lineItems;
    }

    public Order getOrder() {
        return order;
    }

    public List<LineItem> getLineItems() {
        return lineItems;
    }

}
