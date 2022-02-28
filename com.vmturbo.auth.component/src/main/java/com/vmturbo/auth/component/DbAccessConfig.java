package com.vmturbo.auth.component;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
@Import({AuthDbEndpointConfig.class, AuthDBConfig.class})
public class DbAccessConfig {

    @Autowired(required = false)
    private AuthDBConfig authDBConfig;

    @Autowired(required = false)
    private AuthDbEndpointConfig authDbEndpointConfig;

    /**
     * Whether DbMonitor reports should be produced at all.
     */
    @Value("${dbMonitorEnabled:true}")
    private boolean dbMonitorEnabled;

    private final Logger logger = LogManager.getLogger(AuthComponent.class);

    /**
     * Start db monitor if enabled.
     */
    public void startDbMonitor() {
        if (dbMonitorEnabled) {
            if (authDBConfig != null) {
                authDBConfig.startDbMonitor();
            } else {
                try {
                    authDbEndpointConfig.authDbEndpoint().startDbMonitor();
                } catch (UnsupportedDialectException e) {
                    logger.error("Could not initialize the DBMonitor due to", e);
                }
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
        return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() ? authDbEndpointConfig.authDbEndpoint()
                .dslContext() : authDBConfig.dsl();
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
    public DataSource dataSource()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() ? authDbEndpointConfig.authDbEndpoint()
                .datasource() : authDBConfig.dataSource();
    }
}