package com.vmturbo.history.db;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.sql.DataSource;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.jooq.DSLContext;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.history.db.bulk.BulkInserter;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.dbmonitor.DbMonitorConfig;

/**
 * Configuration class for providing db access, like {@link DSLContext} and {@link DataSource},
 * based on feature flag. Also implements some DB-related beans.
 */
@Configuration
@Import({HistoryDbConfig.class, HistoryDbEndpointConfig.class, DbMonitorConfig.class})
public class DbAccessConfig {

    @Autowired(required = false)
    private HistoryDbConfig historyDbConfig;

    @Autowired(required = false)
    private HistoryDbEndpointConfig historyDbEndpointConfig;

    /**
     * Obtain a {@link DSLContext} instance that can be used to access the database.
     *
     * @return DSLContext
     * @throws SQLException                if a DB initialization operation fails
     * @throws UnsupportedDialectException if the DbEndpoint is based on a bogus SLQDialect
     * @throws InterruptedException        if we're interrupted
     */
    public DSLContext dsl() throws SQLException, UnsupportedDialectException, InterruptedException {
        return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
               ? historyDbEndpointConfig.historyEndpoint().dslContext()
               : historyDbConfig.dsl();
    }

    /**
     * Obtain a {@link DSLContext} instance that can be used to access the database and is not
     * backed by a connection pool. This is useful for potentially long-running operations that
     * shouldn't tie up limited pool connections.
     *
     * @return DSLContext
     * @throws SQLException                if a DB initialization operation fails
     * @throws UnsupportedDialectException if the DbEndpoint is based on a bogus SLQDialect
     * @throws InterruptedException        if we're interrupted
     */
    public DSLContext unpooledDsl()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
               // TODO alter this when unpooledDslContext is supported in endpoints
               ? historyDbEndpointConfig.historyEndpoint().dslContext()
               : historyDbConfig.unpooledDsl();
    }

    /**
     * Obtain a {@link DataSource} instance that can be used to access the database.
     *
     * @return DSLContext
     * @throws SQLException                if a DB initialization operation fails
     * @throws UnsupportedDialectException if the DbEndpoint is based on a bogus SLQDialect
     * @throws InterruptedException        if we're interrupted
     */
    public DataSource dataSource()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
               ? historyDbEndpointConfig.historyEndpoint().datasource()
               : historyDbConfig.dataSource();
    }

    /**
     * Obtain a {@link DataSource} instance that can be used to access the database and is not
     * backed by a connection pool. This is useful for potentially long-running operations that
     * shouldn't tie up limited pool connections..
     *
     * @return DSLContext
     * @throws SQLException                if a DB initialization operation fails
     * @throws UnsupportedDialectException if the DbEndpoint is based on a bogus SLQDialect
     * @throws InterruptedException        if we're interrupted
     */
    public DataSource unpooledDataSource()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
               // TODO alter this when unpooledDatasource is supported in endpoints
               ? historyDbEndpointConfig.historyEndpoint().datasource()
               : historyDbConfig.unpooledDataSource();
    }

    /**
     * Whether DbMonitor reports should be produced at all.
     */
    @Value("${dbMonitorEnabled:true}")
    private boolean dbMonitorEnabled;

    public boolean isDbMonitorEnabled() {
        return dbMonitorEnabled;
    }

    /**
     * Start db monitor if enabled.
     */
    public void startDbMonitor() {
        if (dbMonitorEnabled) {
            if (historyDbConfig != null) {
                historyDbConfig.startDbMonitor();
            } else {
                historyDbEndpointConfig.historyEndpoint().startDbMonitor();
            }
        }
    }

    /**
     * Obtain a {@link HistorydbIO} instance.
     * @return HistorydbIO instance
     */
    @Bean
    public HistorydbIO historyDbIO() {
        try {
            return new HistorydbIO(dsl(), unpooledDsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create HistorydbIO bean", e);
        }
    }

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
     *
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
     * Size of bulk loader thread pool.
     */
    @Value("${bulk.parallelBatchInserts:8}")
    public int parallelBatchInserts;

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
}
