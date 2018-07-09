package com.oltpbenchmark.benchmarks.tpcclike;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.Delivery;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.NewOrder;
import com.oltpbenchmark.benchmarks.tpcclike.procedures.Payment;
import com.oltpbenchmark.benchmarks.tpcclike.util.DeliveryTransaction;
import com.oltpbenchmark.benchmarks.tpcclike.util.NewOrderTransaction;
import com.oltpbenchmark.benchmarks.tpcclike.util.PaymentTransaction;
import com.oltpbenchmark.benchmarks.tpcclike.util.Transaction;
import com.oltpbenchmark.types.TransactionStatus;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class TPCCLikeWorker extends Worker<TPCCLikeBenchmark> {

    private static final Logger LOG = Logger.getLogger(TPCCLikeWorker.class);

    private Queue<Transaction> transactions;

    public TPCCLikeWorker(TPCCLikeBenchmark benchmarkModule, int id, Queue<Transaction> transactions) {
        super(benchmarkModule, id);
        this.transactions = transactions;
    }

    @Override
    protected void initialize() {
        Queue<TransactionType> workload = new LinkedList<>();
        for (Transaction transaction : transactions) {
            if (transaction instanceof NewOrderTransaction) {
                workload.add(transactionTypes.getType(NewOrder.class));
            } else if (transaction instanceof PaymentTransaction) {
                workload.add(transactionTypes.getType(Payment.class));
            } else if (transaction instanceof DeliveryTransaction) {
                workload.add(transactionTypes.getType(Delivery.class));
            }
        }
        this.setWorkload(workload);
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws Procedure.UserAbortException, SQLException {
        if (transactions.isEmpty()) {
            return TransactionStatus.RETRY;
        } else {
            Transaction txn = transactions.poll();
            txn.executeTxn(conn);
            conn.commit();
        }

        return TransactionStatus.SUCCESS;
    }


}
