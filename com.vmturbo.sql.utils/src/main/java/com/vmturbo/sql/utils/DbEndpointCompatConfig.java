package com.vmturbo.sql.utils;

import static com.vmturbo.components.common.BaseVmtComponent.PROP_COMPONENT_TYPE;
import static com.vmturbo.sql.utils.DbEndpointResolver.DEFAULT_MAX_POOL_SIZE;
import static com.vmturbo.sql.utils.DbEndpointResolver.DEFAULT_MIN_POOL_SIZE;
import static com.vmturbo.sql.utils.DbEndpointResolver.DEFAULT_USE_CONNECTION_POOL;

import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.vmturbo.components.common.config.SecretPropertiesReader;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.health.sql.SQLDBHealthMonitor.SQLDBConnectionFactory;
import com.vmturbo.sql.utils.pool.DbConnectionPoolConfig;

/**
 * Config class for component DB configs to inherit from when converting to use {@link DbEndpoint}
 * rather than {@link SQLDBConnectionFactory}.
 *
 * <p>There are a number of config properties that may be present and would affect DB configuration
 * under SQLDatabaseConfig and should have the same impact with DbEndpoint. This class serves
 * as a temporary bridge until we can provide backwoard-compatibility in operator.</p>
 */
@Configuration
public class DbEndpointCompatConfig {

    @Autowired
    private Environment springEnvironment;

    @Value("${" + PROP_COMPONENT_TYPE + ":#{null}}")
    private String componentName;

    /**
     * Get the value of a property for this component's non-root DB user name, if configured. Unlike
     * most DB properties, this property's name depends on the component, according to a mapping
     * structure maintained in {@link SecretPropertiesReader}.
     *
     * @return Value of this component's username property or null if not configured
     */
    public String getDbUserName(String component) {
        String propName = SecretPropertiesReader.getDbUsernamePropertyName(component);
        return propName != null ? springEnvironment.getProperty(propName) : null;
    }

    /**
     * Get the value of a property for this component's non-root DB user name, if configured. Unlike
     * most DB properties, this property's name depends on the component, according to a mapping
     * structure maintained in {@link SecretPropertiesReader}.
     *
     * @return Value of this component's username property or null if not configured
     */
    private String getDbPassword(String component) {
        String propName = SecretPropertiesReader.getDbPasswordPropertyName(component);
        return propName != null ? springEnvironment.getProperty(propName) : null;
    }

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:#{null}}")
    protected String dbSchemaName;

    @Value("${enableSecureDBConnection:false}")
    protected boolean isSecureDBConnectionRequested;

    /**
     * If true, use a connection pool for database connections.
     */
    @Value("${conPoolActive:" + DEFAULT_USE_CONNECTION_POOL + "}")
    protected boolean isConnectionPoolActive;

    /**
     * DB connection pool initial and minimum size. Defaults to 1.
     */
    @Value("${conPoolInitialSize:" + DEFAULT_MIN_POOL_SIZE + "}")
    protected int dbMinPoolSize;

    /**
     * DB connection pool maximum size. Defaults to 10.
     */
    @Value("${conPoolMaxActive:" + DEFAULT_MAX_POOL_SIZE + "}")
    protected int dbMaxPoolSize;

    /**
     * Duration between reports logged by Connection pool monitor in seconds. Zero means reporting
     * is not active.
     */
    @Value("${conPoolMonitorIntervalSec:60}")
    protected int poolMonitorIntervalSec;

    /**
     * DB connection pool frequency to send keep alive messages on idle connections. Defaults to 5.
     */
    @Value("${conPoolKeepAliveIntervalMinutes:"
            + DbConnectionPoolConfig.DEFAULT_KEEPALIVE_TIME_MINUTES + "}")
    protected int dbPoolKeepAliveIntervalMinutes;

    @Value("${dbHost:#{null}}")
    protected String dbHost;

    @Value("${dbPort:#{null}}")
    protected Integer dbPort;

    /**
     * DB root username.
     */
    @Value("${dbRootUsername:#{null}}")
    protected String dbRootUsername;

    /**
     * DB root password.
     */
    @Value("${dbRootPassword:#{null}}")
    protected String dbRootPassword;

    protected DbEndpointBuilder fixEndpointForMultiDb(DbEndpointBuilder builder) {
        return fixEndpointForMultiDb(builder, componentName);
    }

    protected DbEndpointBuilder fixEndpointForMultiDb(DbEndpointBuilder builder, String component) {
        if (dbSchemaName != null) {
            builder = builder.withSchemaName(dbSchemaName)
                    // MySqlFamilyAdapter uses databaseName, not schemaName
                    .withDatabaseName(dbSchemaName);
        }
        // unlike most properties, the dbUsername property includes the component name as a prefix,
        if (getDbUserName(component) != null) {
            builder = builder.withUserName(getDbUserName(component));
        }
        // likewise for dbPassword
        if (getDbPassword(component) != null) {
            builder = builder.withPassword(getDbPassword(component));
        }
        builder = builder.withSecure(isSecureDBConnectionRequested);
        builder = builder.withUseConnectionPool(isConnectionPoolActive);
        builder = builder.withMinPoolSize(dbMinPoolSize);
        builder = builder.withMaxPoolSize(dbMaxPoolSize);
        builder = builder.withPoolMonitorIntervalSec(poolMonitorIntervalSec);
        if (dbHost != null) {
            builder = builder.withHost(dbHost);
        }
        if (dbPort != null) {
            builder = builder.withPort(dbPort);
        }
        DbEndpointConfig preview = builder.preview();
        if (dbRootUsername != null && preview.getRootUserName() == null) {
            builder = builder.withRootUserName(dbRootUsername);
        }
        if (dbRootPassword != null && preview.getRootPassword() == null) {
            builder = builder.withRootPassword(dbRootPassword);
        }
        if (!FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            // if feature flag is disabled, we need to use the same default migration lcoation
            // as is used in SQLDatabaseConfig, which does not match the built-in default in
            // DbEndpoint
            builder = builder.withMigrationLocations("db.migration");
        } else if (!Objects.equals(component, componentName)) {
            // kibitzer needs the host component's migration locations, not its own (non-existent)
            // locations. Only matters with POSTGRES_PRIMARY_DB because legacy locations do not
            // include the component name.
            builder = builder.withMigrationLocations(
                    DbEndpointResolver.getDefaultMigrationLocations(component,
                            builder.getDialect()));
        }
        return builder;
    }
}
