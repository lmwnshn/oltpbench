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
     * Specification:
     * <p>
     * 3. delivery transactions:
     * It contains:
     * the date of "today" for this transaction
     * an integer "num" denotes the number of orders that will be shipped "today"
     *
     * @param conn      connection
     * @param rand      random number generator
     * @param txnDate   date of delivery
     * @param maxOrders maximum number of deliverable orders
     * @return delivery transaction
     * @throws SQLException
     */
    public Transaction generateTransaction(Connection conn, RandomGenerator rand,
                                           Queue<NewOrderTransaction> orderQueue,
                                           Date txnDate, int maxOrders)
            throws SQLException {
        int numOrders = maxOrders == 0 ? 0 : rand.number(1, Math.min(maxOrders, 10));

        /*
        3. delivery transactions:
        dequeue "num" orders from Q_neworder
        for each of the "num" orders:
            update the corresponding tuple in LINEITEM table, with "shipdate" updated to "today"
            if there is an index with primary key as the shipdate, add this lineitem to that index
            similarly, if there is any indices maintaining lineitems, update the shipdate of this lineitem in those indices
            increase the balance of the correponding customer by "totalprice" of that order
         */

        List<PreparedStatement> prepList = new ArrayList<>();

        for (int i = 0; i < numOrders; i++) {
            PreparedStatement lineItemPS = getPreparedStatement(conn, updateLineItemStmt);
            PreparedStatement getOrderCustPricePS = getPreparedStatement(conn, getOrderCustPriceStmt);
            PreparedStatement updateCustBalPS = getPreparedStatement(conn, updateCustBalStmt);

            NewOrderTransaction orderTxn = orderQueue.remove();
            for (LineItem lineItem : orderTxn.getLineItems()) {
                lineItemPS.setDate(1, txnDate);
                lineItemPS.setInt(2, lineItem.getOrderKey());
                lineItemPS.setInt(3, lineItem.getLineNumber());
                lineItemPS.addBatch();
            }
            Order order = orderTxn.getOrder();
            // we will simulate getting the order later
            getOrderCustPricePS.setInt(1, order.getOrderKey());
            getOrderCustPricePS.addBatch();
            // before we update the cust accordingly
            updateCustBalPS.setDouble(1, order.getTotalPrice());
            updateCustBalPS.setInt(2, order.getCustKey());
            updateCustBalPS.addBatch();

            prepList.add(lineItemPS);
            prepList.add(getOrderCustPricePS);
            prepList.add(updateCustBalPS);
        }



        return new DeliveryTransaction(prepList, numOrders);
    }

}
