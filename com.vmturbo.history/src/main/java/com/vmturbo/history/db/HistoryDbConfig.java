package com.vmturbo.history.db;

import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.jooq.DSLContext;
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

    @Value("${conPoolMaxActive:25}")
    private int conPoolMaxActive;

    @Value("${conPoolInitialSize:5}")
    private int conPoolInitialSize;

    @Value("${conPoolMinIdle:5}")
    private int conPoolMinIdle;

    @Value("${conPoolMaxIdle:10}")
    private int conPoolMaxIdle;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    /**
     * Provide key parameters, surfaced as spring-configurable values, to be used when creating
     * thie history component conneciton pool.
     *
     * <p>The {@link PoolProperties} structure created here is augmented during pool creation with
     * additional properties required for pool operation.</p>
     *
     * @return A {@link PoolProperties} structure including the configured properties
     */
    @Bean
    PoolProperties connectionPoolProperties() {
        PoolProperties p = new PoolProperties();
        p.setMaxActive(conPoolMaxActive);
        p.setInitialSize(conPoolInitialSize);
        p.setMinIdle(conPoolMinIdle);
        p.setMaxIdle(conPoolMaxIdle);
        return p;
    }

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

    /**
     * Get the shared, initialized instance of {@link HistorydbIO}.
     *
     * @return the instance
     */
    @Bean
    public HistorydbIO historyDbIO() {
        final HistorydbIO dbIO = new HistorydbIO(
                dbPasswordUtil(), databaseConfig.getSQLConfigObject(), connectionPoolProperties());
        HistorydbIO.setSharedInstance(dbIO);
        return dbIO;
    }

    /**
     * Get an instance of {@link DBPasswordUtil} from which we can get needed DB credentials.
     *
     * @return the instance
     */
    @Bean
    public DBPasswordUtil dbPasswordUtil() {
        return new DBPasswordUtil(authHost, authPort, authRoute, authRetryDelaySecs);
    }

    /**
     * Get a {@link DSLContext} configured with a connection factory that will connect to the
     * database as needed.
     *
     * @return the instance
     */
    public DSLContext dsl() {
        return databaseConfig.dsl();
    }
}
