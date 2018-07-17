package com.oltpbenchmark.benchmarks.tpcclike.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class DeliveryTransaction extends Transaction {

    /**
     * We expect the pattern of [UPDATE, SELECT, UPDATE]* for prepared statements.
     */
    public DeliveryTransaction(List<PreparedStatement> preparedStatements) {
        super(preparedStatements);
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

}
