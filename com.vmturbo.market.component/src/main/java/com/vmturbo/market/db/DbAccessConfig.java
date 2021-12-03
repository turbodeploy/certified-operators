package com.vmturbo.market.db;

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
@Import({MarketDBConfig.class, MarketDbEndpointConfig.class})
public class DbAccessConfig {

    @Autowired(required = false)
    private MarketDBConfig marketDBConfig;

    @Autowired(required = false)
    private MarketDbEndpointConfig marketDbEndpointConfig;

    /** Whether DbMonitor reports should be produced at all. */
    @Value("${dbMonitorEnabled:true}")
    private boolean dbMonitorEnabled;

    /**
     * Start db monitor if enabled.
     */
    public void startDbMonitor() {
        if (dbMonitorEnabled) {
            // TODO: OM-76858 implement dbmonitor for postgres
            if (marketDBConfig != null) {
                marketDBConfig.startDbMonitor();
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
                ? marketDbEndpointConfig.marketEndpoint().dslContext()
                : marketDBConfig.dsl();
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
                ? marketDbEndpointConfig.marketEndpoint().datasource()
                : marketDBConfig.dataSource();
    }
}
