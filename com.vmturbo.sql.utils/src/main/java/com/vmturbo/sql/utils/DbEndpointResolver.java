package com.vmturbo.sql.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.SQLDialect;
import org.springframework.core.env.Environment;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.pool.DbConnectionPoolConfig;

/**
 * This class resolves any {@link DbEndpoint} properties that were not provided using builder
 * methods in the endpoint definition.
 *
 * <p>Builder-specified properties are provided in a {@link DbEndpointConfig} instance, and that
 * config instance is updated with values for any missing fields taken from any of the following
 * sources, in decreasing priority:</p>
 *
 * <ul>
 *     <li>The Spring {@link Environment} as constructed during application context construction</li>
 *     <li>
 *         The property values present in a template endpoint, if this endpoint was constructed
 *         with the {@link DbEndpointBuilder#like(DbEndpoint)}  method. Not all properties are
 *         copied from the template; see {@link DbEndpoint} for details.
 *     </li>
 *     <li>Built-in defaults; see {@link DbEndpoint} for per-property default details.</li>
 * </ul>
 */
public class DbEndpointResolver {
    private static final Logger logger = LogManager.getLogger();

    // names of properties for endpoint configuration

    /** dbHost property. */
    public static final String HOST_PROPERTY = "host";
    /** dbPort property. */
    public static final String PORT_PROPERTY = "port";
    /** dbDatabaseName property. */
    public static final String DATABASE_NAME_PROPERTY = "databaseName";
    /** dbSchemaName property. */
    public static final String SCHEMA_NAME_PROPERTY = "schemaName";
    /** dbUserName property. */
    public static final String USER_NAME_PROPERTY = "userName";
    /** dbPassword property. */
    public static final String PASSWORD_PROPERTY = "password";
    /** dbAccess property. */
    public static final String ACCESS_PROPERTY = "access";
    /** dbRootUserName property. */
    public static final String ROOT_USER_NAME_PROPERTY = "rootUserName";
    /** dbRootPassword property. */
    public static final String ROOT_PASSWORD_PROPERTY = "rootPassword";
    /** dbRootAccessEnabled property. */
    public static final String ROOT_ACCESS_ENABLED_PROPERTY = "rootAccessEnabled";
    /** dbDriverProperties property. */
    public static final String DRIVER_PROPERTIES_PROPERTY = "driverProperties";
    /** dbSecure property. */
    public static final String SECURE_PROPERTY_NAME = "secure";
    /** dbMigrationLocations property. */
    public static final String MIGRATION_LOCATIONS_PROPERTY = "migrationLocations";
    /** dbDestructiveProvisioningEnabled property. */
    public static final String DESTRUCTIVE_PROVISIONING_ENABLED_PROPERTY = "destructiveProvisioningEnabled";
    /** dbEndpointEnabled property. */
    public static final String ENDPOINT_ENABLED_PROPERTY = "endpointEnabled";
    /** dbProvisioningSuffix property. */
    public static final String NAME_SUFFIX_PROPERTY = "nameSuffix";
    /** dbShouldProvisionDatabase property. */
    public static final String SHOULD_PROVISION_DATABASE_PROPERTY = "shouldProvisionDatabase";
    /** dbShouldProvisionUser property. */
    public static final String SHOULD_PROVISION_USER_PROPERTY = "shouldProvisionUser";
    /** dbUseConnectionPool property. */
    public static final String USE_CONNECTION_POOL = "conPoolActive";
    /** DB connection pool initial and minimum size property. */
    public static final String MIN_POOL_SIZE_PROPERTY = "conPoolInitialSize";
    /** DB connection pool maximum size property. */
    public static final String MAX_POOL_SIZE_PROPERTY = "conPoolMaxActive";
    /** DB connection pool keep alive interval (in minutes). */
    public static final String POOL_KEEP_ALIVE_INTERVAL_MINUTES = "conPoolKeepAliveIntervalMinutes";

    /** system property name for retrieving component name for certain property defaults. */
    public static final String COMPONENT_TYPE_PROPERTY = "component_type";

    // default values for some properties

    /** default port for MariaDB and MySql. */
    public static final int DEFAULT_MARIADB_MYSQL_PORT = 3306;
    /** default port for PostgreSQL. */
    public static final int DEFAULT_POSTGRES_PORT = 5432;
    /** default for secure connection. */
    public static final Boolean DEFAULT_SECURE_VALUE = Boolean.FALSE;
    /** default migration location, prepended to component name. */
    public static final String DEFAULT_MIGRATION_LOCATION_PREFIX = "db.migration.";
    /** default for access level. */
    public static final String DEFAULT_ACCESS_VALUE = DbEndpointAccess.READ_ONLY.name();
    /** default value for host name. */
    public static final String DEFAULT_HOST_VALUE = "localhost";
    /** default value for whether to use a connection pool for database connections. */
    public static final boolean DEFAULT_USE_CONNECTION_POOL = true;
    /** default value for connection pool initial and minimum size. */
    public static final int DEFAULT_MIN_POOL_SIZE = 1;
    /** default value for connection pool maximum size. */
    public static final int DEFAULT_MAX_POOL_SIZE = 10;
    /** default value for connection pool keep alive interval (in minutes).
     *
     *  <p>This definition is redundant, but included for consistency with other settings in this
     *  class and so that it is easy to find. </p>*/
    public static final int DEFAULT_POOL_KEEP_ALIVE_INTERVAL_MINUTES =
            DbConnectionPoolConfig.DEFAULT_KEEPALIVE_TIME_MINUTES;

    // absolute min and max values (guardrails) for some properties
    /** default value for connection pool initial and minimum size. */
    public static final int ABSOLUTE_MIN_POOL_SIZE = 1;
    /** default value for connection pool maximum size. */
    public static final int ABSOLUTE_MAX_POOL_SIZE = 500;

    private static final SpelExpressionParser spel = new SpelExpressionParser();

    private final DBPasswordUtil dbPasswordUtil;

    private final DbEndpointConfig config;
    private final DbEndpointConfig template;
    private final String name;
    private final SQLDialect dialect;
    private final UnaryOperator<String> resolver;

    /**
     * Create a new resolve instance.
     *
     * @param config         the endpoint config to be resolved
     * @param resolver       a function that looks up property values from the environment
     * @param dbPasswordUtil a {@link DBPasswordUtil} instance to obtain credential defaults
     */
    public DbEndpointResolver(
            DbEndpointConfig config, UnaryOperator<String> resolver, DBPasswordUtil dbPasswordUtil) {
        this.config = config;
        this.resolver = resolver;
        this.dbPasswordUtil = dbPasswordUtil;
        this.name = Strings.isNullOrEmpty(config.getName()) ? null : config.getName();
        this.dialect = config.getDialect();
        this.template = config.getTemplate() != null ? config.getTemplate().getConfig() : null;
    }

    void resolve() throws UnsupportedDialectException {
        resolveProvisioningSuffix();
        resolveHost();
        resolvePort();
        resolveDatabaseName();
        resolveSchemaName();
        resolveUserName();
        resolvePassword();
        resolveAccess();
        resolveRootUserName();
        resolveRootPassword();
        resolveRootAccessEnabled();
        resolveDriverProperties();
        resolveUseConnectionPool();
        resolveMinPoolSize();
        resolveMaxPoolSize();
        resolvePoolKeepAliveInterval();
        resolveSecure();
        resolveMigrationLocations();
        resolveFlywayCallbacks();
        resolveEndpointEnabled();
        resolveShouldProvisionDatabase();
        resolveShouldProvisionUser();
    }

    /**
     * Resolve the dbHost property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    private void resolveHost() throws UnsupportedDialectException {
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getHost);
        config.setHost(firstNonNull(configuredPropValue(HOST_PROPERTY),
                config.getHost(), fromTemplate,
                // backward compatibility for customers who are still using old operator
                // this should be dropped when we have no at-risk customers
                DEFAULT_HOST_VALUE));
    }

    /**
     * Resolve the dbPort property for this endpoint.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    private void resolvePort() throws UnsupportedDialectException {
        String fromTemplate = getFromTemplate(DbEndpointConfig::getPort);
        String fromDefault = Integer.toString(getDefaultPort(dialect));
        String currentValue = config.getPort() != null ? config.getPort().toString() : null;
        String propValue = firstNonNull(configuredPropValue(PORT_PROPERTY),
                currentValue, fromTemplate, fromDefault);
        config.setPort(Integer.parseInt(propValue));
    }

    /**
     * Resolve the dbDatabaseName property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDatabaseName() throws UnsupportedDialectException {
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getDatabaseName);
        final String value = firstNonNull(configuredPropValue(DATABASE_NAME_PROPERTY),
                config.getDatabaseName(), fromTemplate, getComponentName());
        config.setDatabaseName(addSuffix(value));
    }

    /**
     * Resolve the dbSchemaName for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveSchemaName() throws UnsupportedDialectException {
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getSchemaName);
        final String value = firstNonNull(configuredPropValue(SCHEMA_NAME_PROPERTY),
                config.getSchemaName(), fromTemplate, getComponentName());
        config.setSchemaName(addSuffix(value));
    }

    /**
     * Resolve the dbUserName property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveUserName() throws UnsupportedDialectException {
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getUserName);
        final String value = firstNonNull(configuredPropValue(USER_NAME_PROPERTY),
                config.getUserName(), fromTemplate, getComponentName());
        config.setUserName(addSuffix(value));
    }

    /**
     * Resolve the dbPassword property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolvePassword() throws UnsupportedDialectException {
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getPassword);
        config.setPassword(firstNonNull(configuredPropValue(PASSWORD_PROPERTY),
                config.getPassword(), fromTemplate, dbPasswordUtil.getSqlDbRootPassword()));
    }

    /**
     * Resolve the dbAccess property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveAccess() throws UnsupportedDialectException {
        String currentValue = config.getAccess() != null ? config.getAccess().name() : null;
        String fromTemplate = getFromTemplate(DbEndpointConfig::getAccess);
        config.setAccess(DbEndpointAccess.valueOf(
                firstNonNull(configuredPropValue(ACCESS_PROPERTY),
                        currentValue, fromTemplate, DEFAULT_ACCESS_VALUE)));
    }

    /**
     * Resolve the dbRootUserName property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveRootUserName() throws UnsupportedDialectException {
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getRootUserName);
        config.setRootUserName(firstNonNull(configuredPropValue(ROOT_USER_NAME_PROPERTY),
                config.getRootUserName(), fromTemplate,
                dbPasswordUtil.getSqlDbRootUsername(dialect.toString())));
    }

    /**
     * Resolve the rootAccessEnabled property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveRootAccessEnabled() throws UnsupportedDialectException {
        final String configuredValue = config.isRootAccessEnabled() != null
                ? Boolean.toString(config.isRootAccessEnabled()) : null;
        final String fromTemplate = getFromTemplate(DbEndpointConfig::isRootAccessEnabled);
        config.setRootAccessEnabled(Boolean.parseBoolean(
                firstNonNull(configuredPropValue(ROOT_ACCESS_ENABLED_PROPERTY),
                        configuredValue, fromTemplate, "false")));
    }

    /**
     * Resolve the dbRootPassword for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveRootPassword() throws UnsupportedDialectException {
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getRootPassword);
        config.setRootPassword(firstNonNull(configuredPropValue(ROOT_PASSWORD_PROPERTY),
                config.getRootPassword(), fromTemplate, dbPasswordUtil.getSqlDbRootPassword()));
    }

    /**
     * Resolve the dbDriverProperties property for this endpoint.
     *
     * <p>This is more complicated than most of the other properties because it's a map. It works
     * by first selecting a "base" map, which is either a value already set for this endpoint, or
     * the value set for the template if any, or the default for this endpoint's dialect. Then if
     * there's a configured value, it is merged into the base, overriding the values for any keys
     * that appear in both.</p>
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    public void resolveDriverProperties() throws UnsupportedDialectException {
        Map<String, String> base = config.getDriverProperties();
        if (base == null) {
            base = getFromTemplate(template, DbEndpointConfig::getDriverProperties);
        }
        if (base == null) {
            base = new HashMap<>(getDefaultDriverProperties(dialect));
        }
        final String injectedProperties = configuredPropValue(DRIVER_PROPERTIES_PROPERTY);
        if (injectedProperties != null) {
            @SuppressWarnings("unchecked")
            final Map<? extends String, ? extends String> injectedMap =
                    (Map<? extends String, ? extends String>)spel.parseRaw(injectedProperties)
                            .getValue();
            base.putAll(injectedMap != null ? injectedMap : Collections.emptyMap());
        }
        config.setDriverProperties(base);
    }

    /**
     * Resolve the conPoolActive property for this endpoint.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    private void resolveUseConnectionPool() throws UnsupportedDialectException {
        final String currentValue = config.getUseConnectionPool() != null
                ? config.getUseConnectionPool().toString() : null;
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getUseConnectionPool);
        config.setUseConnectionPool(Boolean.parseBoolean(firstNonNull(
                configuredPropValue(USE_CONNECTION_POOL),
                currentValue, fromTemplate, Boolean.toString(DEFAULT_USE_CONNECTION_POOL))));
    }

    /**
     * Resolve the conPoolInitialSize property for this endpoint.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    private void resolveMinPoolSize() throws UnsupportedDialectException {
        String fromTemplate = getFromTemplate(DbEndpointConfig::getMinPoolSize);
        String fromDefault = Integer.toString(DEFAULT_MIN_POOL_SIZE);
        String currentValue = config.getMinPoolSize() != null ? config.getMinPoolSize().toString() : null;
        String propValue = firstNonNull(configuredPropValue(MIN_POOL_SIZE_PROPERTY),
                currentValue, fromTemplate, fromDefault);
        int minPoolSize = Integer.parseInt(propValue);
        minPoolSize =
                Math.min(ABSOLUTE_MAX_POOL_SIZE, Math.max(ABSOLUTE_MIN_POOL_SIZE, minPoolSize));
        config.setMinPoolSize(minPoolSize);
    }

    /**
     * Resolve the conPoolMaxActive property for this endpoint.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    private void resolveMaxPoolSize() throws UnsupportedDialectException {
        String fromTemplate = getFromTemplate(DbEndpointConfig::getMaxPoolSize);
        String fromDefault = Integer.toString(DEFAULT_MAX_POOL_SIZE);
        String currentValue = config.getMaxPoolSize() != null
                ? config.getMaxPoolSize().toString()
                : null;
        String propValue = firstNonNull(configuredPropValue(MAX_POOL_SIZE_PROPERTY),
                currentValue, fromTemplate, fromDefault);
        int maxPoolSize = Integer.parseInt(propValue);
        // Enforce guardrails on the final maximum pool size.
        maxPoolSize =
                Math.min(ABSOLUTE_MAX_POOL_SIZE, Math.max(ABSOLUTE_MIN_POOL_SIZE, maxPoolSize));
        config.setMaxPoolSize(maxPoolSize);
    }

    /**
     * Resolve the conPoolKeepAliveIntervalMinutes property for this endpoint.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    private void resolvePoolKeepAliveInterval() throws UnsupportedDialectException {
        String fromTemplate = getFromTemplate(DbEndpointConfig::getKeepAliveIntervalMinutes);
        String fromDefault = Integer.toString(DEFAULT_POOL_KEEP_ALIVE_INTERVAL_MINUTES);
        String currentValue = config.getKeepAliveIntervalMinutes() != null
                ? config.getKeepAliveIntervalMinutes().toString()
                : null;
        String propValue = firstNonNull(configuredPropValue(POOL_KEEP_ALIVE_INTERVAL_MINUTES),
                currentValue, fromTemplate, fromDefault);
        int keepAliveIntervalMinutes = Integer.parseInt(propValue);
        config.setKeepAliveIntervalMinutes(keepAliveIntervalMinutes);
    }

    /**
     * Resolve the dbSecure property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveSecure() throws UnsupportedDialectException {
        final String currentValue = config.getSecure() != null ? config.getSecure().toString() : null;
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getSecure);
        config.setSecure(Boolean.parseBoolean(firstNonNull(configuredPropValue(SECURE_PROPERTY_NAME),
                currentValue, fromTemplate, DEFAULT_SECURE_VALUE.toString())));
    }

    /**
     * Resolve the dbMigrationLocations property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveMigrationLocations() throws UnsupportedDialectException {
        String fromTemplate = getFromTemplate(DbEndpointConfig::getMigrationLocations);
        config.setMigrationLocations(firstNonNull(configuredPropValue(MIGRATION_LOCATIONS_PROPERTY),
                config.getMigrationLocations(), fromTemplate,
                DEFAULT_MIGRATION_LOCATION_PREFIX + getComponentName()));
    }

    /**
     * Get the flyway callbacks declared for this endpoint.
     *
     * <p>This property cannot be set via external configuration.</p>
     */
    public void resolveFlywayCallbacks() {
        if (config.getFlywayCallbacks() == null) {
            final FlywayCallback[] fromTemplate =
                    getFromTemplate(template, DbEndpointConfig::getFlywayCallbacks);
            config.setFlywayCallbacks(fromTemplate != null ? fromTemplate : new FlywayCallback[0]);
        }
    }

    /**
     * Resolve the dbEndpointEnabled property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveEndpointEnabled() throws UnsupportedDialectException {
        // enablement is always set with a function, even when a boolean value is provided,
        // to make things less complicated here
        final String currentValue = config.getEndpointEnabledFn() != null
                ? config.getEndpointEnabledFn().apply(resolver).toString() : null;
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getEndpointEnabled);
        config.setEndpointEnabled(Boolean.parseBoolean(firstNonNull(
                configuredPropValue(ENDPOINT_ENABLED_PROPERTY),
                currentValue, fromTemplate, Boolean.TRUE.toString())));
    }


    /**
     * Resolve the dbProvisioningSuffix property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveProvisioningSuffix() throws UnsupportedDialectException {
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getProvisioningSuffix);
        config.setProvisioningSuffix(firstNonNull(configuredPropValue(NAME_SUFFIX_PROPERTY),
                config.getProvisioningSuffix(), fromTemplate, ""));
    }

    /**
     * Resolve the dbShouldProvisionDatabase property.
     *
     * @throws UnsupportedDialectException if endpoint is mis-configured
     */
    public void resolveShouldProvisionDatabase() throws UnsupportedDialectException {
        final String currentValue = config.getShouldProvisionDatabase() != null
                ? config.getShouldProvisionDatabase().toString() : null;
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getShouldProvisionDatabase);
        config.setShouldProvisionDatabase(Boolean.parseBoolean(firstNonNull(
                configuredPropValue(SHOULD_PROVISION_DATABASE_PROPERTY),
                currentValue, fromTemplate, Boolean.toString(false))));
    }

    /**
     * Resolve the dbShouldProvisionUser property.
     *
     * @throws UnsupportedDialectException if endpoint is mis-configured
     */
    public void resolveShouldProvisionUser() throws UnsupportedDialectException {
        final String currentValue = config.getShouldProvisionUser() != null
                ? config.getShouldProvisionUser().toString() : null;
        final String fromTemplate = getFromTemplate(DbEndpointConfig::getShouldProvisionUser);
        config.setShouldProvisionUser(Boolean.parseBoolean(firstNonNull(
                configuredPropValue(SHOULD_PROVISION_USER_PROPERTY),
                currentValue, fromTemplate, Boolean.toString(false))));
    }

    private String configuredPropValue(String propertyName) throws UnsupportedDialectException {
        // top priority choices are the property name prefixed with all the dot-boundary prefixes
        // of the endpoint name, from longest to shortest; after that come dialect-specific
        // prefixes
        return Stream.of(dotPrefixes(name).stream(), dialectPropertyPrefixes().stream())
                .flatMap(Functions.identity())
                // lookup each choice
                .map(prefix -> prefix + "." + propertyName)
                // lookup each choice in resolver
                .map(resolver)
                // skip names that could not be resolved
                .filter(Objects::nonNull)
                // and return first hit, if any
                .findFirst()
                .orElse(null);
    }

    private Collection<String> dotPrefixes(final String name) {
        String prefix = name;
        List<String> result = new ArrayList<>();
        while (prefix != null) {
            result.add(prefix);
            final int lastDot = prefix.lastIndexOf('.');
            prefix = lastDot >= 0 ? prefix.substring(0, lastDot) : null;
        }
        return result;
    }

    private List<String> dialectPropertyPrefixes() throws UnsupportedDialectException {
        return dialectPropertyPrefixes(dialect);
    }

    @VisibleForTesting
    static List<String> dialectPropertyPrefixes(SQLDialect dialect) throws UnsupportedDialectException {
        switch (dialect) {
            case MYSQL:
                return Arrays.asList("dbs.mysqlDefault", "dbs.mariadbDefault");
            case MARIADB:
                return Arrays.asList("dbs.mariadbDefault", "dbs.mysqlDefault");
            case POSTGRES:
                return Arrays.asList("dbs.postgresDefault");
            default:
                throw new UnsupportedDialectException(dialect);
        }
    }

    private String firstNonNull(String... choices) {
        return Stream.of(choices).filter(Objects::nonNull).findFirst().orElse(null);
    }

    private String addSuffix(String value) {
        final String suffix = config.getProvisioningSuffix();
        return !Strings.isNullOrEmpty(suffix) && !value.endsWith(suffix)
                ? value + suffix
                : value;
    }

    private <T> String getFromTemplate(Function<DbEndpointConfig, T> getter) {
        return getFromTemplate(getter, Object::toString);
    }

    private <T> String getFromTemplate(Function<DbEndpointConfig, T> getter, Function<T, String> toString) {
        final T value = getFromTemplate(template, getter);
        return value != null ? toString.apply(value) : null;
    }

    private <T> T getFromTemplate(DbEndpointConfig tConfig, Function<DbEndpointConfig, T> getter) {
        while (tConfig != null) {
            T value = getter.apply(tConfig);
            if (value != null) {
                return value;
            }
            tConfig = tConfig.getTemplate() != null ? tConfig.getTemplate().getConfig() : null;
        }
        return null;
    }


    /**
     * Get the name of this component from configuration.
     *
     * <p>This is used as a default for some properties in an untagged endpoint.</p>
     *
     * @return component name, or null if not available
     */
    private String getComponentName() {
        return resolver.apply(COMPONENT_TYPE_PROPERTY);
    }

    /**
     * Get the default dbPort value based on this endpoint's server type.
     *
     * @param dialect server type
     * @return default port
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    public static int getDefaultPort(SQLDialect dialect) throws UnsupportedDialectException {
        switch (dialect) {
            case MYSQL:
            case MARIADB:
                return DEFAULT_MARIADB_MYSQL_PORT;
            case POSTGRES:
                return DEFAULT_POSTGRES_PORT;
            default:
                throw new UnsupportedDialectException(dialect);
        }
    }

    /**
     * Get the default driver properties for this endpoint, based on the database type.
     *
     * @param dialect database server type
     * @return default driver properties
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    public static Map<String, String> getDefaultDriverProperties(SQLDialect dialect) throws UnsupportedDialectException {
        switch (dialect) {
            case MYSQL:
                return Collections.emptyMap();
            case MARIADB:
                return ImmutableMap.of(
                        "useServerPrepStmts", "true"
                );
            case POSTGRES:
                // set up for secure connection?
                return Collections.emptyMap();
            default:
                throw new UnsupportedDialectException(dialect);
        }
    }
}
