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
     * NewOrder Specification:
     * <p>
     * Each new order transaction should create a new ORDER with a unique order id (sequential),
     * random o_custkey, o_orderdate set to the txn date argument, random o_shippriority [1,5],
     * random o_orderpriority [1,5], orderstatus set to F,
     * o_totalprice computed as sum(l_extendedprice * (1+l_tax) * (1-l_discount)) for all LINEITEM,
     * and a set of rand [1,7] LINEITEM.
     * <p>
     * The ith LINEITEM is uniformly randomly chosen from all PARTSUPP available.
     * Line numbers are set to i, l_quantity is rand [0,50],
     * l_extendedprice = l_quantity * p_retailprice, where p_retailprice is from the part table,
     * l_linestatus set to F, l_instructions set to 'NONE', l_shipmode set to 'RAIL',
     * l_returnflag set to 'R'.
     * <p>
     * All other columns, where unspecified, were set to the TPC specification or NULL.
     *
     * @param conn        connection
     * @param util        instance of utility class
     * @param rand        random number generator
     * @param scaleFactor current scalefactor
     * @param orderKey    orderkey
     * @param orderDate   orderdate
     * @param hyperHack   true if we should use the hyper hack
     * @return new order transaction
     * @throws SQLException
     */
    public Transaction generateTransaction(Connection conn, Util util, RandomGenerator rand,
                                           int scaleFactor, int orderKey, Date orderDate,
                                           boolean hyperHack)
            throws SQLException {

        if (hyperHack) {
            return hyperStatements(conn, util, rand, scaleFactor, orderKey, orderDate);
        }

        // first, generate all the line items as we need them for order price
        PreparedStatement insertLineItemPS = getPreparedStatement(conn, insertLineItemStmt);
        List<LineItem> lineItems = new ArrayList<>();

        // we can compute this as we generate line items
        double o_totalprice = 0.0;

        // we need rand [1,7] line items
        int numLineItems = rand.number(1, 7);

        for (int l_linenumber = 0; l_linenumber < numLineItems; l_linenumber++) {
            PartSupp partSupp = util.getRandomPartSupp(conn, hyperHack);

            int l_orderkey = orderKey;
            int l_partkey = partSupp.getPartKey();
            int l_suppkey = partSupp.getSuppKey();
            int l_quantity = rand.number(0, 49);

            double p_retailprice = util.getPartRetailPrice(conn, partSupp.getPartKey());
            double l_extendedprice = l_quantity * p_retailprice;
            double l_discount = rand.fixedPoint(2, 0.0, 0.1);
            double l_tax = rand.fixedPoint(2, 0.0, 0.08);

            String l_returnflag = "R";
            String l_linestatus = "F";

            Date l_shipdate = TPCCLikeUtil.addDays(orderDate, rand.number(1, 121));
            Date l_commitdate = TPCCLikeUtil.addDays(orderDate, rand.number(30, 90));
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

        // after generating the line items, generate the order
        int o_orderkey = orderKey;
        int o_custkey = util.getRandomCustKey(conn, hyperHack);
        String o_orderstatus = "F";
        // o_totalprice calculated above
        Date o_orderdate = orderDate;
        int o_orderpriority = rand.number(1, 5);
        int clerkNum = rand.number(1, scaleFactor * 1000);
        String o_clerk = String.format("Clerk#%s", String.format("%09d", clerkNum));
        int o_shippriority = rand.number(1, 5);
        String o_comment = rand.astring(19, 78);

        PreparedStatement insertOrderPS = this.getPreparedStatement(conn, insertOrderStmt);
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

        // make sure we add the order first! line item depends on it
        List<PreparedStatement> preparedStatements = new ArrayList<>();
        preparedStatements.add(insertOrderPS);
        preparedStatements.add(insertLineItemPS);

        return new NewOrderTransaction(preparedStatements, order, lineItems);
    }

    /**
     * Avoids mixing PreparedStatement types
     */
    private NewOrderTransaction hyperStatements(Connection conn, Util util, RandomGenerator rand,
                                                int scaleFactor, int orderKey, Date orderDate)
            throws SQLException {
        PreparedStatement insertLineItemPS = getPreparedStatement(conn, insertLineItemStmt);
        List<LineItem> lineItems = new ArrayList<>();

        double o_totalprice = 0.0;

        int numLineItems = rand.number(1, 7);
        for (int l_linenumber = 0; l_linenumber < numLineItems; l_linenumber++) {
            PartSupp partSupp = util.getRandomPartSupp(conn, true);

            int l_orderkey = orderKey;
            int l_partkey = partSupp.getPartKey();
            int l_suppkey = partSupp.getSuppKey();
            int l_quantity = rand.number(0, 49);

            double p_retailprice = util.getPartRetailPrice(conn, partSupp.getPartKey());
            double l_extendedprice = l_quantity * p_retailprice;
            double l_discount = rand.fixedPoint(2, 0.0, 0.1);
            double l_tax = rand.fixedPoint(2, 0.0, 0.08);

            String l_returnflag = "R";
            String l_linestatus = "F";

            Date l_shipdate = TPCCLikeUtil.addDays(orderDate, rand.number(1, 121));
            Date l_commitdate = TPCCLikeUtil.addDays(orderDate, rand.number(30, 90));
            Date l_receiptdate = TPCCLikeUtil.addDays(l_shipdate, rand.number(1, 30));

            String l_shipinstruct = "NONE";
            String l_shipmode = "RAIL";
            String l_comment = rand.astring(10, 43);

            String sql = TPCCLikeUtil.replaceParams(insertLineItemStmt.getSQL(),
                    new int[]{6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
                    new String[]{
                            String.valueOf(l_extendedprice),
                            String.valueOf(l_discount),
                            String.valueOf(l_tax),
                            String.format("'%s'", l_returnflag),
                            String.format("'%s'", l_linestatus),
                            String.format("'%s'::date", l_shipdate),
                            String.format("'%s'::date", l_commitdate),
                            String.format("'%s'::date", l_receiptdate),
                            String.format("'%s'", l_shipinstruct),
                            String.format("'%s'", l_shipmode),
                            String.format("'%s'", l_comment),
                    });
            insertLineItemPS = conn.prepareStatement(sql);

            insertLineItemPS.setInt(1, l_orderkey);
            insertLineItemPS.setInt(2, l_partkey);
            insertLineItemPS.setInt(3, l_suppkey);
            insertLineItemPS.setInt(4, l_linenumber);
            insertLineItemPS.setInt(5, l_quantity);
            insertLineItemPS.addBatch();

            o_totalprice += l_extendedprice * (1 + l_tax) * (1 - l_discount);

            LineItem lineItem = new LineItem(l_orderkey, l_linenumber);
            lineItems.add(lineItem);
        }

        int o_orderkey = orderKey;
        int o_custkey = util.getRandomCustKey(conn, true);
        String o_orderstatus = "F";
        // o_totalprice calculated above
        Date o_orderdate = orderDate;
        int o_orderpriority = rand.number(1, 5);
        int clerkNum = rand.number(1, scaleFactor * 1000);
        String o_clerk = String.format("Clerk#%s", String.format("%09d", clerkNum));
        int o_shippriority = rand.number(1, 5);
        String o_comment = rand.astring(19, 78);

        String sql = TPCCLikeUtil.replaceParams(insertOrderStmt.getSQL(),
                new int[]{3, 4, 5, 7, 9},
                new String[]{
                        String.format("'%s'", o_orderstatus),
                        String.valueOf(o_totalprice),
                        String.format("'%s'::date", o_orderdate),
                        String.format("'%s'", o_clerk),
                        String.format("'%s'", o_comment),
                });
        PreparedStatement insertOrderPS = conn.prepareStatement(sql);

        insertOrderPS.setInt(1, o_orderkey);
        insertOrderPS.setInt(2, o_custkey);
        insertOrderPS.setInt(3, o_orderpriority);
        insertOrderPS.setInt(4, o_shippriority);
        insertOrderPS.addBatch();

        Order order = new Order(o_orderkey, o_custkey, o_totalprice);

        List<PreparedStatement> preparedStatements = new ArrayList<>();
        preparedStatements.add(insertOrderPS);
        preparedStatements.add(insertLineItemPS);

        return new NewOrderTransaction(preparedStatements, order, lineItems);
    }


}
