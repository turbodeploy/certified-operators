package com.vmturbo.history.db;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.sql.DataSource;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.DSLContext;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.history.db.bulk.BulkInserter;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.flyway.MigrationCallbackForVersion121;
import com.vmturbo.history.flyway.ResetChecksumsForMyIsamInfectedMigrations;
import com.vmturbo.history.flyway.V1_28_1_And_V1_35_1_Callback;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.sql.utils.dbmonitor.DbMonitorConfig;
import com.vmturbo.sql.utils.flyway.ForgetMigrationCallback;

/**
 * Spring Configuration for the HistorydbIO class.
 **/
@Configuration
@Import({DbMonitorConfig.class})
public class HistoryDbConfig extends SQLDatabaseConfig {
    private static Logger logger = LogManager.getLogger();

    @Value("${historyDbUsername:history}")
    private String historyDbUsername;

    @Value("${historyDbPassword:}")
    private String historyDbPassword;

    @Value("${dbSchemaName:vmtdb}")
    private String dbSchemaName;

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
    @Value("${bulk.maxPendingBatches:10}")
    private int maxPendingBatches;

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
     * Create a shared thread pool for bulk writers used by all the ingesters.
     *
     * @return new thread pool
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService bulkLoaderThreadPool() {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat(BulkInserter.class.getSimpleName() + "-%d")
                .build();
        return Executors.newFixedThreadPool(parallelBatchInserts, factory);
    }

    /**
     * Get the shared, initialized instance of {@link HistorydbIO}.
     *
     * @return the instance
     */
    @Bean
    public HistorydbIO historyDbIO() {
        return new HistorydbIO(dsl(), unpooledDsl());
    }

    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, historyDbUsername, Optional.ofNullable(
                !Strings.isEmpty(historyDbPassword) ? historyDbPassword : null));
    }

    /**
     * Get a {@link DataSource} that will produce connections that are not part of the connection
     * pool. This may be advisable for connections that will be used for potentially long-running
     * operations, to avoid tying up limited pool connections.
     *
     * @return unpooled datasource
     */
    @Bean
    public DataSource unpooledDataSource() {
        return getUnpooledDataSource(dbSchemaName, historyDbUsername, Optional.ofNullable(
                !Strings.isEmpty(historyDbPassword) ? historyDbPassword : null));
    }

    /**
     * Get a {@link DSLContext} that uses unpooled connections to perform database operations.
     * This may be advisable when performing potentially long-running DB operaitions to avoid
     * tying up limited pool connections.
     *
     * @return DSLContext that uses unpooled connections
     */
    @Bean
    public DSLContext unpooledDsl() {
        return new DefaultDSLContext(unpooledDataSource(), sqlDialect);
    }

    @Override
    public FlywayCallback[] flywayCallbacks() {
        return new FlywayCallback[]{
                // V1.27 migrations collided when 7.17 and 7.21 branches were merged
                new ForgetMigrationCallback("1.27"),
                // three migrations were changed in order to remove mention of MyISAM DB engine
                new ResetChecksumsForMyIsamInfectedMigrations(),
                // V1.28.1 and V1.35.1 java migrations needed to change
                // V1.28.1 formerly supplied a checksum but no longer does
                new V1_28_1_And_V1_35_1_Callback(),
                // V1.21 checksum has to change
                new MigrationCallbackForVersion121()
        };
    }

    /** Whether DbMonitor reports should be produced at all. */
    @Value("${dbMonitorEnabled:true}")
    private boolean dbMonitorEnabled;

    public boolean isDbMonitorEnabled() {
        return dbMonitorEnabled;
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }

    @Override
    public String getDbUsername() {
        return historyDbUsername;
    }
}
