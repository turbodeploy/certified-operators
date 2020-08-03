package com.vmturbo.history.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.history.db.bulk.BulkInserter;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.flyway.ResetChecksumsForMyIsamInfectedMigrations;
import com.vmturbo.history.flyway.V1_28_1_And_V1_35_1_Callback;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.sql.utils.flyway.ForgetMigrationCallback;

/**
 * Spring Configuration for the HistorydbIO class.
 **/
@Configuration
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
        final HistorydbIO dbIO
                = new HistorydbIO(dbPasswordUtil(), getSQLConfigObject(), connectionPoolProperties());
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

    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, historyDbUsername, Optional.ofNullable(
                !Strings.isEmpty(historyDbPassword) ? historyDbPassword : null));
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
                new V1_28_1_And_V1_35_1_Callback()
        };
    }

    @Override
    protected void grantDbPrivileges(@Nonnull final String schemaName, @Nonnull final String dbPassword,
            final Connection rootConnection, final String requestUser) throws SQLException {
        super.grantDbPrivileges(schemaName, dbPassword, rootConnection, requestUser);
        try {
            logger.info("Attempting to grant global PROCESS privilege to {}.", requestUser);
            rootConnection.createStatement().execute(
                    String.format("GRANT PROCESS ON *.* TO %s", requestUser));
            rootConnection.createStatement().executeQuery("FLUSH PRIVILEGES");
        } catch (SQLException e) {
            logger.warn("Failed to grant PROCESS privilege; DbMonitor will only see history threads", e);
        }
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }
}
