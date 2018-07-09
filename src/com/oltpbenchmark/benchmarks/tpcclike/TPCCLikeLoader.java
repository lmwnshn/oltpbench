package com.oltpbenchmark.benchmarks.tpcclike;

import com.oltpbenchmark.api.Loader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Dummy class.
 */
public class TPCCLikeLoader extends Loader<TPCCLikeBenchmark> {

    public TPCCLikeLoader(TPCCLikeBenchmark benchmark, Connection conn) {
        super(benchmark, conn);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        // we do not load anything for this benchmark
        List<LoaderThread> loaders = new ArrayList<>();
        return loaders;
    }
}
