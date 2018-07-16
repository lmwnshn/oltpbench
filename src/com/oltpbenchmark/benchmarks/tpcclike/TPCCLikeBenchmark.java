package com.oltpbenchmark.benchmarks.tpcclike;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.*;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.Delivery;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.NewOrder;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.Payment;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.Util;
import com.oltpbenchmark.benchmarks.tpcclike.util.DeliveryTransaction;
import com.oltpbenchmark.benchmarks.tpcclike.util.NewOrderTransaction;
import com.oltpbenchmark.benchmarks.tpcclike.util.TPCCLikeUtil;
import com.oltpbenchmark.benchmarks.tpcclike.util.Transaction;
import com.oltpbenchmark.util.RandomGenerator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

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
            // we need to create instances to make use of Procedure's SQL dialects
            Util util = new Util(numTransactions);
            NewOrder newOrder = new NewOrder();
            Payment payment = new Payment();
            Delivery delivery = new Delivery();

            int scaleFactor = (int) workConf.getScaleFactor();
            int orderkey = util.getMaxOrderKey(conn) + 1;
            Date orderdate = Date.valueOf("2000-01-01");
            Transaction transaction;

            int maxOrders = 0; // needed for Delivery so that we won't dequeue an empty list
            Queue<NewOrderTransaction> orderQueue = new LinkedList<>();

            // generate new_order:payment:delivery in 45:43:4 ratio, which sums to 92
            LOG.debug(String.format("Generating %d transactions...", numTransactions));
            for (int i = 0; i < numTransactions; i++) {
                int txnPicker = randGen.number(1, 92);

                // generate the transaction
                if (1 <= txnPicker && txnPicker <= 45) {
                    // new_order
                    transaction = newOrder.generateTransaction(
                            conn, util, randGen, scaleFactor, orderkey, orderdate
                    );
                    // increment order key
                    orderkey++;
                    maxOrders++;
                    LOG.debug(String.format("Generated NEW_ORDER [%d/%d]", i, numTransactions));
                    orderQueue.add((NewOrderTransaction) transaction);
                } else if (46 <= txnPicker && txnPicker <= 88) {
                    // payment
                    transaction = payment.generateTransaction(conn, util, randGen);
                    LOG.debug(String.format("Generated PAYMENT [%d/%d]", i, numTransactions));
                } else {
                    // delivery
                    transaction = delivery.generateTransaction(conn, randGen, orderQueue, orderdate, maxOrders);
                    maxOrders -= ((DeliveryTransaction) transaction).getNumOrders();
                    LOG.debug(String.format("Generated DELIVERY [%d/%d]", i, numTransactions));
                }

                transactions.add(transaction);

                // advance date with probability 1/100
                if (randGen.number(1, 100) == 1) {
                    orderdate = TPCCLikeUtil.addDays(orderdate, 1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't generate transactions before making workers.");
        }

        LOG.info(String.format("%d transactions generated. We only support 1 worker.", transactions.size()));
        PrintWriter pw = new PrintWriter("txn_generated.sql");
        pw.println("\\c tpch");
        pw.println("\\o out.txt");
        pw.println("\\timing");
        for (Transaction txn : transactions) {
            String type;
            if (txn instanceof NewOrderTransaction) {
                type = "NewOrder";
            } else if (txn instanceof DeliveryTransaction) {
                type = "Delivery";
            } else {
                type = "Payment";
            }
            pw.println(String.format("\\! echo \"%s\"", type));
            pw.println("BEGIN TRANSACTION;");
            for (String s : txn.getTransactions()) {
                pw.println(s);
            }
            pw.println("END TRANSACTION;");
        }

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
