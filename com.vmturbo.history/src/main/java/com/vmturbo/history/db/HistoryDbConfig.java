package com.vmturbo.history.db;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Spring Configuration for the HistorydbIO class.
 **/
@Configuration
@Import({SQLDatabaseConfig.class})
public class HistoryDbConfig {


    @Value("${authHost}")
    private String authHost;

    @Value("${authRoute:}")
    private String authRoute;

    @Value("${serverHttpPort}")
    private int authPort;

    @Value("${authRetryDelaySecs}")
    private int authRetryDelaySecs;

    /**
     * Size of bulk loader thread pool.
     */
    @Value("${bulk.parallelBatchInserts:8}")
    public int parallelBatchInserts;

    /**
     * Maximum batch size for bulk loaders.
     */
    @Value("${bulk.batchSize:1000}")
    private int batchSize;

    /**
     * Maximum number of times to retry a failed batch insertion in a bulk loader.
     */
    @Value("${bulk.maxBatchRetries:10}")
    private int matchBatchRetries;

    /**
     * Time to wait before final retry of a failed batch; delays prior to other retries ramp up
     * to this in an exponential fashion.
     */
    @Value("${bulk.maxRetryBackoffMsec:60000}") // 1 minute
    private int maxRetryBackoffMsec;

    /**
     * Maximum batches a single bulk loader can have queued or in process in the thread pool before
     * it must block additional batches.
     */
    @Value("${bulk.maxPendingBatches:8}")
    private int maxPendingBatches;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    /**
     * Default config for bulk inserter/loader instances.
     * @return bulk inserter config
     */
    @Bean
    public BulkInserterConfig bulkLoaderConfig() {
        return ImmutableBulkInserterConfig.builder()
            .batchSize(batchSize)
            .maxBatchRetries(matchBatchRetries)
            .maxRetryBackoffMsec(maxRetryBackoffMsec)
            .maxPendingBatches(maxPendingBatches)
            .build();
    }

    @Bean
    public HistorydbIO historyDbIO() {
        final HistorydbIO dbIO
            = new HistorydbIO(dbPasswordUtil(), databaseConfig.getSQLConfigObject());
        HistorydbIO.setSharedInstance(dbIO);
        return dbIO;
    }

    @Bean
    public DBPasswordUtil dbPasswordUtil() {
        return new DBPasswordUtil(authHost, authPort, authRoute, authRetryDelaySecs);
    }
}
