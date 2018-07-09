package com.oltpbenchmark.benchmarks.tpcclike.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PaymentTransaction extends Transaction {

    public PaymentTransaction(List<PreparedStatement> preparedStatements) {
        super(preparedStatements);
    }

}
