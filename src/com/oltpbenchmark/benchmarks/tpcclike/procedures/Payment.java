package com.oltpbenchmark.benchmarks.tpcclike.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcclike.util.PaymentTransaction;
import com.oltpbenchmark.benchmarks.tpcclike.util.TPCCLikeUtil;
import com.oltpbenchmark.benchmarks.tpcclike.util.Transaction;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Payment extends Procedure {

    private SQLStmt updateCustBalStmt = new SQLStmt("UPDATE customer "
            + "SET c_acctbal = c_acctbal - ? "
            + "WHERE c_custkey = ? "
    );

    /**
     * Generates a list of SQL commands that make up a single payment transaction.
     * <p>
     * Payment Specification:
     * <p>
     * A payment conains a random customer key and a random float value.
     *
     * @param conn      connection
     * @param util      instance of utility class
     * @param rand      random number generator
     * @param hyperHack true if we should use the hyper hack
     * @return payment transaction
     * @throws SQLException
     */
    public Transaction generateTransaction(Connection conn, Util util, RandomGenerator rand, boolean hyperHack)
            throws SQLException {
        if (hyperHack) {
            return hyperStatements(conn, util, rand);
        }

        // random customer, random payment
        int c_custkey = util.getRandomCustKey(conn, hyperHack);
        double payment = rand.fixedPoint(2, 1.00, 5000.00);

        PreparedStatement updateCustBalPS = getPreparedStatement(conn, updateCustBalStmt);
        updateCustBalPS.setDouble(1, payment);
        updateCustBalPS.setInt(2, c_custkey);
        updateCustBalPS.addBatch();

        List<PreparedStatement> preparedStatements = new ArrayList<>();
        preparedStatements.add(updateCustBalPS);

        return new PaymentTransaction(preparedStatements);
    }

    /**
     * Avoids mixing PreparedStatement types
     */
    private PaymentTransaction hyperStatements(Connection conn, Util util, RandomGenerator rand)
            throws SQLException {
        int c_custkey = util.getRandomCustKey(conn, true);
        double payment = rand.fixedPoint(2, 1.00, 5000.00);

        String sql = TPCCLikeUtil.replaceParams(updateCustBalStmt.getSQL(),
                new int[]{1},
                new String[]{
                        String.valueOf(payment)
                });

        PreparedStatement updateCustBalPS = conn.prepareStatement(sql);
        updateCustBalPS.setInt(1, c_custkey);
        updateCustBalPS.addBatch();

        List<PreparedStatement> preparedStatements = new ArrayList<>();
        preparedStatements.add(updateCustBalPS);

        return new PaymentTransaction(preparedStatements);
    }

}
