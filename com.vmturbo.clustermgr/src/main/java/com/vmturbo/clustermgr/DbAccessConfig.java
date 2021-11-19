package com.vmturbo.clustermgr;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
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
@Import({ClustermgrDBConfig.class, ClustermgrDBConfig2.class})
public class DbAccessConfig {

    @Autowired(required = false)
    private ClustermgrDBConfig clustermgrDBConfig;

    @Autowired(required = false)
    private ClustermgrDBConfig2 clustermgrDBConfig2;

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
                ? clustermgrDBConfig2.clusterMgrEndpoint().dslContext()
                : clustermgrDBConfig.dsl();
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
                ? clustermgrDBConfig2.clusterMgrEndpoint().datasource()
                : clustermgrDBConfig.dataSource();
    }
}
