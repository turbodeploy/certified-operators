package com.vmturbo.history.db;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.DSLContext;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.history.flyway.MigrationCallbackForVersion121;
import com.vmturbo.history.flyway.ResetChecksumsForMyIsamInfectedMigrations;
import com.vmturbo.history.flyway.V1_28_1_And_V1_35_1_Callback;
import com.vmturbo.sql.utils.ConditionalDbConfig.SQLDatabaseConfigCondition;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.sql.utils.flyway.ForgetMigrationCallback;

/**
 * Spring Configuration for History DB configuration using {@link SQLDatabaseConfig}.
 **/
@Configuration
@Conditional(SQLDatabaseConfigCondition.class)
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
        return new DefaultDSLContext(unpooledDataSource(), sqlDialect, configuration().settings());
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

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }

    @Override
    public String getDbUsername() {
        return historyDbUsername;
    }
}
