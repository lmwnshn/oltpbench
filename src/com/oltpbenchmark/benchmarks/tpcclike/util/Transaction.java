package com.oltpbenchmark.benchmarks.tpcclike.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class Transaction {

    protected List<PreparedStatement> preparedStatements;

    public Transaction(List<PreparedStatement> preparedStatements) {
        this.preparedStatements = preparedStatements;
    }

    public void executeTxn(Connection conn) throws SQLException {
        for (PreparedStatement ps : preparedStatements) {
            ps.executeBatch();
        }
    }

}
