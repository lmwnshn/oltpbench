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

public class NewOrder extends Procedure {

    private SQLStmt insertOrderStmt = new SQLStmt("INSERT INTO orders "
            + "(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, "
            + "o_orderpriority, o_clerk, o_shippriority, o_comment) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    private SQLStmt insertLineItemStmt = new SQLStmt("INSERT INTO lineitem "
            + "(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, "
            + "l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, "
            + "l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );


    /**
     * Generates a list of SQL commands that make up a single new order transaction.
     * Order is at index 0, followed by the line items.
     * <p>
     * Specification:
     * <p>
     * 1. new_order transactions:
     * each new_order transaction contains:
     * an unique order id (since ordered are added sequentially, each new order id is the current number of orders + 1)
     * a random customer key
     * orderdate = today
     * random shippriority (1-5)
     * random orderpriority (1-5)
     * O_ORDERSTATUS set as 'F'
     * A set of x (1<=x<=7) lineitems, the i-th lineitem:
     * uniformly randomly choosen from all partsupps
     * linenumbers are i
     * random quantity < 50
     * L_EXTENDEDPRICE = L_QUANTITY * P_RETAILPRICE, Where P_RETAILPRICE is from the part table with P_PARTKEY = L_PARTKEY.
     * linestatues set to 'F'
     * instructions set to "NONE"
     * shipmode set to "RAIL"
     * return flag set to "R"
     * All the others columns of this lineitem are set to the default value or empty
     * O_TOTALPRICE computed as:
     * sum (L_EXTENDEDPRICE * (1+L_TAX) * (1-L_DISCOUNT)) for all LINEITEM of this order.
     * All the others columns of this order are set to the default value or empty
     *
     * @param conn        connection
     * @param util        instance of utility class
     * @param rand        random number generator
     * @param scaleFactor current scalefactor
     * @param orderkey    orderkey
     * @param orderdate   orderdate
     * @return new order transaction
     * @throws SQLException
     */
    public Transaction generateTransaction(Connection conn, Util util, RandomGenerator rand,
                                           int scaleFactor, int orderkey, Date orderdate)
            throws SQLException {
        // first we add the line items
        PreparedStatement insertLineItemPS = this.getPreparedStatement(conn, insertLineItemStmt);
        List<LineItem> lineItems = new ArrayList<>();

        double o_totalprice = 0.0; // we can compute this as we generate line items
        int numLineItems = rand.number(1, 7);

        for (int l_linenumber = 0; l_linenumber < numLineItems; l_linenumber++) {
            PartSupp partSupp = util.getRandomPartSupp(conn);

            int l_orderkey = orderkey;
            int l_partkey = partSupp.getPartKey();
            int l_suppkey = partSupp.getSuppKey();
            int l_quantity = rand.number(0, 49);

            double p_retailprice = util.getPartRetailPrice(conn, partSupp.getPartKey());
            double l_extendedprice = l_quantity * p_retailprice;
            double l_discount = rand.fixedPoint(2, 0.0, 0.1);
            double l_tax = rand.fixedPoint(2, 0.0, 0.08);

            String l_returnflag = "R";
            String l_linestatus = "F";

            Date l_shipdate = TPCCLikeUtil.addDays(orderdate, rand.number(1, 121));
            Date l_commitdate = TPCCLikeUtil.addDays(orderdate, rand.number(30, 90));
            Date l_receiptdate = TPCCLikeUtil.addDays(l_shipdate, rand.number(1, 30));

            String l_shipinstruct = "NONE";
            String l_shipmode = "RAIL";
            String l_comment = rand.astring(10, 43);

            insertLineItemPS.setInt(1, l_orderkey);
            insertLineItemPS.setInt(2, l_partkey);
            insertLineItemPS.setInt(3, l_suppkey);
            insertLineItemPS.setInt(4, l_linenumber);
            insertLineItemPS.setInt(5, l_quantity);
            insertLineItemPS.setDouble(6, l_extendedprice);
            insertLineItemPS.setDouble(7, l_discount);
            insertLineItemPS.setDouble(8, l_tax);
            insertLineItemPS.setString(9, l_returnflag);
            insertLineItemPS.setString(10, l_linestatus);
            insertLineItemPS.setDate(11, l_shipdate);
            insertLineItemPS.setDate(12, l_commitdate);
            insertLineItemPS.setDate(13, l_receiptdate);
            insertLineItemPS.setString(14, l_shipinstruct);
            insertLineItemPS.setString(15, l_shipmode);
            insertLineItemPS.setString(16, l_comment);
            insertLineItemPS.addBatch();

            o_totalprice += l_extendedprice * (1 + l_tax) * (1 - l_discount);

            LineItem lineItem = new LineItem(l_orderkey, l_linenumber);
            lineItems.add(lineItem);
        }


        // then we load the order
        PreparedStatement insertOrderPS = this.getPreparedStatement(conn, insertOrderStmt);
        int o_orderkey = orderkey;
        int o_custkey = util.getRandomCustKey(conn);
        String o_orderstatus = "F";
        // o_totalprice calculated above
        Date o_orderdate = orderdate;
        int o_orderpriority = rand.number(1, 5);
        int clerkNum = rand.number(1, scaleFactor * 1000);
        String o_clerk = String.format("Clerk#%s", String.format("%09d", clerkNum));
        int o_shippriority = rand.number(1, 5);
        String o_comment = rand.astring(19, 78);

        insertOrderPS.setInt(1, o_orderkey);
        insertOrderPS.setInt(2, o_custkey);
        insertOrderPS.setString(3, o_orderstatus);
        insertOrderPS.setDouble(4, o_totalprice);
        insertOrderPS.setDate(5, o_orderdate);
        insertOrderPS.setInt(6, o_orderpriority);
        insertOrderPS.setString(7, o_clerk);
        insertOrderPS.setInt(8, o_shippriority);
        insertOrderPS.setString(9, o_comment);
        insertOrderPS.addBatch();

        Order order = new Order(o_orderkey, o_custkey, o_totalprice);

        List<PreparedStatement> preparedStatements = new ArrayList<>();
        preparedStatements.add(insertLineItemPS);
        preparedStatements.add(insertOrderPS);

        return new NewOrderTransaction(preparedStatements, order, lineItems);
    }


}
