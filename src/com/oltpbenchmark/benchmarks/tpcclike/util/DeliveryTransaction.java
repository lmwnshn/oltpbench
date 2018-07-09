package com.oltpbenchmark.benchmarks.tpcclike.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Queue;

public class DeliveryTransaction extends Transaction {

    private int numOrders;

    /**
     * We expect [UPDATE, SELECT, UPDATE]* for preparedStatements
     * @param preparedStatements
     * @param numOrders
     */
    public DeliveryTransaction(List<PreparedStatement> preparedStatements, int numOrders) {
        super(preparedStatements);
        this.numOrders = numOrders;
    }

    @Override
    public void executeTxn(Connection conn) throws SQLException {
        int i = 0;
        for (PreparedStatement ps : preparedStatements) {
            if (i == 1) { // select
                ps.executeQuery();
            } else { // update
                ps.executeBatch();
            }
            i = (i + 1) % 3;
        }
    }

    public int getNumOrders() {
        return numOrders;
    }
}
