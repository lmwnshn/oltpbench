package com.oltpbenchmark.benchmarks.tpcclike.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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

    // hack, but works for psql driver
    public List<String> getTransactions() {
        List<String> txn = new ArrayList<>();
        for (PreparedStatement ps : preparedStatements) {
            txn.add(String.format("%s;", ps.toString()));
        }
        return txn;
    }

}
