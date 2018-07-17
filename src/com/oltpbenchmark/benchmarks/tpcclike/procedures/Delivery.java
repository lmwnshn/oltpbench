package com.oltpbenchmark.benchmarks.tpcclike.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcclike.util.*;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class Delivery extends Procedure {

    private SQLStmt updateCustBalStmt = new SQLStmt("UPDATE customer "
            + "SET c_acctbal = c_acctbal + ? "
            + "WHERE c_custkey = ? "
    );

    private SQLStmt updateLineItemStmt = new SQLStmt("UPDATE lineitem "
            + "SET l_shipdate = ? "
            + "WHERE l_orderkey = ? AND l_linenumber = ? "
    );

    private SQLStmt getOrderCustPriceStmt = new SQLStmt(
            "SELECT o_custkey, o_totalprice FROM orders WHERE o_orderkey = ?"
    );

    /**
     * Generates a list of SQL commands that make up a single delivery transaction.
     * <p>
     * Delivery Specification:
     * <p>
     * Dequeue "num" orders from the order queue, where num = rand [1, min(|orderQueue|, 10)]
     * For each order, update the corresponding LINEITEM shipdate to given transaction date,
     * and increase the balance of the corresponding CUSTOMER by the order's total price.
     * <p>
     * In theory, the order queue should only store the order key.
     * This order key should be used to look up the order details.
     * We ensure we will look up the order anyway, but we pre-populate what we know to be
     * the result to minimize the time spent in Java during the execution phase.
     *
     * @param conn         connection
     * @param rand         random number generator
     * @param orderQueue   queue of orders
     * @param deliveryDate date of delivery
     * @param hyperHack    true if we should use the hyper hack
     * @return delivery transaction
     * @throws SQLException
     */
    public Transaction generateTransaction(Connection conn, RandomGenerator rand,
                                           Queue<NewOrderTransaction> orderQueue,
                                           Date deliveryDate, boolean hyperHack)
            throws SQLException {

        if (hyperHack) {
            return hyperStatements(conn, rand, orderQueue, deliveryDate);
        }

        // we only deliver up to 10 orders on a given day
        int maxOrders = orderQueue.size();
        int numOrders = maxOrders == 0 ? 0 : rand.number(1, Math.min(maxOrders, 10));

        // these prepared statements will be our transaction
        List<PreparedStatement> prepList = new ArrayList<>();

        for (int i = 0; i < numOrders; i++) {
            NewOrderTransaction orderTxn = orderQueue.remove();
            Order order = orderTxn.getOrder();

            // prepare all the line items
            PreparedStatement lineItemPS = getPreparedStatement(conn, updateLineItemStmt);
            for (LineItem lineItem : orderTxn.getLineItems()) {
                lineItemPS.setDate(1, deliveryDate);
                lineItemPS.setInt(2, lineItem.getOrderKey());
                lineItemPS.setInt(3, lineItem.getLineNumber());
                lineItemPS.addBatch();
            }

            // simulate getting the order details
            PreparedStatement getOrderCustPricePS = getPreparedStatement(conn, getOrderCustPriceStmt);
            getOrderCustPricePS.setInt(1, order.getOrderKey());
            getOrderCustPricePS.addBatch();

            // update the customer
            PreparedStatement updateCustBalPS = getPreparedStatement(conn, updateCustBalStmt);
            updateCustBalPS.setDouble(1, order.getTotalPrice());
            updateCustBalPS.setInt(2, order.getCustKey());
            updateCustBalPS.addBatch();

            // add the prepared statements corresponding to this transaction
            prepList.add(lineItemPS);
            prepList.add(getOrderCustPricePS);
            prepList.add(updateCustBalPS);
        }

        return new DeliveryTransaction(prepList);
    }

    /**
     * Avoids mixing PreparedStatement types
     */
    private DeliveryTransaction hyperStatements(Connection conn, RandomGenerator rand,
                                                Queue<NewOrderTransaction> orderQueue,
                                                Date txnDate)
            throws SQLException {
        int maxOrders = orderQueue.size();
        int numOrders = maxOrders == 0 ? 0 : rand.number(1, Math.min(maxOrders, 10));

        List<PreparedStatement> prepList = new ArrayList<>();

        for (int i = 0; i < numOrders; i++) {
            NewOrderTransaction orderTxn = orderQueue.remove();
            Order order = orderTxn.getOrder();

            PreparedStatement lineItemPS = getPreparedStatement(conn, updateLineItemStmt);
            for (LineItem lineItem : orderTxn.getLineItems()) {
                String sql = TPCCLikeUtil.replaceParams(updateLineItemStmt.getSQL(),
                        new int[]{1},
                        new String[]{
                                String.format("'%s'::date", txnDate)
                        });
                lineItemPS = conn.prepareStatement(sql);
                lineItemPS.setInt(1, lineItem.getOrderKey());
                lineItemPS.setInt(2, lineItem.getLineNumber());
                lineItemPS.addBatch();
            }

            PreparedStatement getOrderCustPricePS = getPreparedStatement(conn, getOrderCustPriceStmt);
            getOrderCustPricePS.setInt(1, order.getOrderKey());
            getOrderCustPricePS.addBatch();

            String sql = TPCCLikeUtil.replaceParams(updateCustBalStmt.getSQL(),
                    new int[]{1},
                    new String[]{
                            String.valueOf(order.getTotalPrice())
                    });

            PreparedStatement updateCustBalPS;
            updateCustBalPS = conn.prepareStatement(sql);
            updateCustBalPS.setInt(1, order.getCustKey());
            updateCustBalPS.addBatch();

            prepList.add(lineItemPS);
            prepList.add(getOrderCustPricePS);
            prepList.add(updateCustBalPS);
        }

        return new DeliveryTransaction(prepList);
    }

}
