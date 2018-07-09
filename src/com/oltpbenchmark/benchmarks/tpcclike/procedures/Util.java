package com.oltpbenchmark.benchmarks.tpcclike.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcclike.util.PartSupp;
import com.oltpbenchmark.benchmarks.tpcclike.util.TPCCLikeUtil;

import java.sql.*;
import java.util.LinkedList;
import java.util.Queue;

public class Util extends Procedure {

    // these SQLStmts use LIMIT to prefetch as RANDOM() can be slow

    private SQLStmt getRandPartSuppStmt = new SQLStmt(
            "SELECT ps_partkey, ps_suppkey FROM partsupp ORDER BY RANDOM() LIMIT ?"
    );

    private SQLStmt getRandCustKeyStmt = new SQLStmt(
            "SELECT c_custkey FROM customer ORDER BY RANDOM() LIMIT ?"
    );

    private SQLStmt getRetailPriceStmt = new SQLStmt(
            "SELECT p_retailprice FROM part WHERE p_partkey = ?"
    );

    private SQLStmt getMaxOrderKeyStmt = new SQLStmt(
            "SELECT max(o_orderkey) FROM orders"
    );

    private int numPrefetch;
    private Queue<PartSupp> partSuppPrefetch;
    private Queue<Integer> custKeyPrefetch;

    public Util(int numPrefetch) {
        this.numPrefetch = numPrefetch;
        this.partSuppPrefetch = new LinkedList<>();
        this.custKeyPrefetch = new LinkedList<>();
    }

    /**
     * Returns a random PartSupp.
     *
     * @param conn connection
     * @return random PartSupp
     * @throws SQLException
     */
    PartSupp getRandomPartSupp(Connection conn) throws SQLException {
        if (!partSuppPrefetch.isEmpty()) {
            return partSuppPrefetch.remove();
        }

        PreparedStatement ps = this.getPreparedStatement(conn, getRandPartSuppStmt);
        ps.setInt(1, numPrefetch);
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            int partkey = rs.getInt(1);
            int suppkey = rs.getInt(2);

            this.partSuppPrefetch.add(new PartSupp(partkey, suppkey));
        }

        assert !this.partSuppPrefetch.isEmpty() : "Couldn't fetch partsupp";
        return this.partSuppPrefetch.remove();
    }

    /**
     * Returns a random customer key.
     *
     * @param conn connection
     * @return random customer key
     * @throws SQLException
     */
    int getRandomCustKey(Connection conn) throws SQLException {
        if (!custKeyPrefetch.isEmpty()) {
            return custKeyPrefetch.remove();
        }

        PreparedStatement ps = this.getPreparedStatement(conn, getRandCustKeyStmt);
        ps.setInt(1, numPrefetch);
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            custKeyPrefetch.add(rs.getInt(1));
        }

        assert !this.custKeyPrefetch.isEmpty() : "Couldn't fetch custkey";
        return this.custKeyPrefetch.remove();
    }

    /**
     * Returns the retail price of a part.
     *
     * @param conn    connection
     * @param partkey key of the part we're interested in
     * @return retail price of part
     * @throws SQLException
     */
    double getPartRetailPrice(Connection conn, int partkey) throws SQLException {
        PreparedStatement ps = this.getPreparedStatement(conn, getRetailPriceStmt, partkey);
        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getDouble(1);
    }

    /**
     * Returns the maximum order key in the database.
     *
     * @param conn connection
     * @return maximum order key in database
     * @throws SQLException
     */
    public int getMaxOrderKey(Connection conn) throws SQLException {
        PreparedStatement ps = this.getPreparedStatement(conn, getMaxOrderKeyStmt);
        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getInt(1);
    }
}
