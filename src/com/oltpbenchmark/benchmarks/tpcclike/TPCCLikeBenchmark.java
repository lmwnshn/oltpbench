package com.oltpbenchmark.benchmarks.tpcclike;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.Delivery;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.NewOrder;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.Payment;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.Util;
import com.oltpbenchmark.benchmarks.tpcclike.util.NewOrderTransaction;
import com.oltpbenchmark.benchmarks.tpcclike.util.TPCCLikeUtil;
import com.oltpbenchmark.benchmarks.tpcclike.util.Transaction;
import com.oltpbenchmark.util.RandomGenerator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.*;

public class TPCCLikeBenchmark extends BenchmarkModule {

    private static final Logger LOG = Logger.getLogger(TPCCLikeBenchmark.class);
    private RandomGenerator randGen;
    private int numTransactions;
    private Queue<Transaction> transactions;

    public TPCCLikeBenchmark(WorkloadConfiguration workConf) {
        super("tpcclike", workConf, true);
        this.rng().setSeed(15721);
        randGen = new RandomGenerator(this.rng().nextInt());
        numTransactions = workConf.getXmlConfig().getInt("num_transactions");
        transactions = new ArrayDeque<>(numTransactions);
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        Connection conn;
        try {
            conn = this.makeConnection();

            // TODO: dialecting should occur before makeWorkersImpl
            // the idea was that we could make use of Procedure's SQL dialects
            // unfortunately, dialecting occurs after makeWorkersImpl right now
            // doesn't matter since we're largely standard SQL
            Util util = new Util(numTransactions);
            NewOrder newOrder = new NewOrder();
            Payment payment = new Payment();
            Delivery delivery = new Delivery();

            ////////////////////////////////////////////////////////
            // GIANT HACK HERE
            // removal condition:
            // 1. stop supporting HyPer, or
            // 2. get a version of HyPer that supports JDBC
            //    without breaking on mixed PreparedStatement types
            ////////////////////////////////////////////////////////

            boolean hyperHack = workConf.getDBName().equals("HYPER");

            // Specification: our date starts at 2000-01-01
            // and increments by a day with probability 1/100 after each txn
            Date orderDate = Date.valueOf("2000-01-01");

            // Specification: our new order keys simply count up from
            // the current maximum order key
            int orderKey = util.getMaxOrderKey(conn) + 1;

            // We need to store information about new orders as we generate them
            // Because they will be used as we deliver them
            Queue<NewOrderTransaction> orderQueue = new LinkedList<>();
            // We also need the scale factor for generating new orders
            int scaleFactor = (int) workConf.getScaleFactor();

            LOG.debug(String.format("Generating %d transactions...", numTransactions));

            // Specification: generate new_order:payment:delivery in 45:43:4 ratio
            for (int i = 0; i < numTransactions; i++) {
                Transaction transaction;
                int txnPicker = randGen.number(1, 92);

                // generate the transaction
                if (1 <= txnPicker && txnPicker <= 45) {                    // new_order
                    transaction = newOrder.generateTransaction(
                            conn, util, randGen, scaleFactor, orderKey, orderDate, hyperHack
                    );
                    orderKey++;
                    orderQueue.add((NewOrderTransaction) transaction);

                    LOG.debug(String.format("Generated NEW_ORDER [%d/%d]", i, numTransactions));
                } else if (46 <= txnPicker && txnPicker <= 88) {            // payment
                    transaction = payment.generateTransaction(
                            conn, util, randGen, hyperHack
                    );

                    LOG.debug(String.format("Generated PAYMENT [%d/%d]", i, numTransactions));
                } else {                                                    // delivery
                    transaction = delivery.generateTransaction(
                            conn, randGen, orderQueue, orderDate, hyperHack
                    );

                    LOG.debug(String.format("Generated DELIVERY [%d/%d]", i, numTransactions));
                }

                transactions.add(transaction);

                // advance date with probability 1/100
                if (randGen.number(1, 100) == 1) {
                    orderDate = TPCCLikeUtil.addDays(orderDate, 1);
                }
            }
        } catch (SQLException ex) {
            LOG.error("Couldn't generate transactions before making worker implementations.");
            throw new RuntimeException(ex);
        }

        LOG.info(String.format("%d transactions generated. We only support 1 worker.", transactions.size()));

        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
        workers.add(new TPCCLikeWorker(this, 0, transactions));

        return workers;
    }

    @Override
    protected Loader<? extends BenchmarkModule> makeLoaderImpl(Connection conn) throws SQLException {
        return new TPCCLikeLoader(this, conn);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return NewOrder.class.getPackage();
    }

}
