package com.vmturbo.history.testutil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;

/**
 * Utilities for testing with bulk loaders.
 */
public class BulkLoaderUtils {

    private BulkLoaderUtils() {}

    /**
     * Create a bulk loader factory.
     * @param historydbIO DB methods
     * @return the new factory
     */
    public static SimpleBulkLoaderFactory getRecordWriterFactory(HistorydbIO historydbIO) {
        return new SimpleBulkLoaderFactory(historydbIO, getConfig(), getRecordWritersThreadPool());
    }

    public static ExecutorService getRecordWritersThreadPool() {
        return Executors.newFixedThreadPool(2);
    }

    public static ImmutableBulkInserterConfig getConfig() {
        return ImmutableBulkInserterConfig.builder()
            .batchSize(1000)
            .maxBatchRetries(2)
            .maxRetryBackoffMsec(5000)
            .maxPendingBatches(2)
            .build();
    }
}
