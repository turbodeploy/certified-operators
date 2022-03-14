package com.vmturbo.cost.component.db;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration class for providing db access, like {@link DSLContext} and {@link DataSource},
 * based on feature flag.
 */
@Configuration
@Import({CostDBConfig.class, CostDbEndpointConfig.class})
public class DbAccessConfig {

    @Autowired(required = false)
    private CostDBConfig costDBConfig;

    @Autowired(required = false)
    private CostDbEndpointConfig costDbEndpointConfig;

    /** Whether DbMonitor reports should be produced at all. */
    @Value("${dbMonitorEnabled:true}")
    private boolean dbMonitorEnabled;

    /**
     * Start db monitor if enabled.
     */
    public void startDbMonitor() {
        if (dbMonitorEnabled) {
            if (costDBConfig != null) {
                costDBConfig.startDbMonitor();
            } else {
                costDbEndpointConfig.costEndpoint().startDbMonitor();
            }
        }
    }

    /**
     * Get the {@link DSLContext} on a given {@link DbEndpoint} or legacy {@link SQLDatabaseConfig}
     * based on feature flag.
     *
     * @return {@link DSLContext}
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    public DSLContext dsl() throws SQLException, UnsupportedDialectException, InterruptedException {
        return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
                ? costDbEndpointConfig.costEndpoint().dslContext()
                : costDBConfig.dsl();
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
                ? costDbEndpointConfig.costEndpoint().dslContext()
                : costDBConfig.unpooledDsl();
    }

    /**
     * Get the {@link DataSource} on a given {@link DbEndpoint} or legacy {@link SQLDatabaseConfig}
     * based on feature flag.
     *
     * @return {@link DataSource}
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    public DataSource dataSource() throws SQLException, UnsupportedDialectException, InterruptedException {
        return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
                ? costDbEndpointConfig.costEndpoint().datasource()
                : costDBConfig.dataSource();
    }
}