package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import javax.sql.DataSource;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.jetbrains.annotations.NotNull;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.auth.api.db.DBPasswordUtil;

/**
 * This class manages access to a database, including initializing the database on first use,
 * applying migrations during restarts, and providing access in the form of JDBC connections and
 * data sources, and jOOQ DSL contexts.
 *
 * <p>Normally, a component should create db endpoints with the assistance of the {@link
 * SQLDatabaseConfig2} class, which constructs a context for resolving the config objects that apply
 * to each endpoint.</p>
 *
 * <p>A given endpoint may be defined with a tag, in which case config property names are all
 * prefixed with the tag name and an underscore, e.g. "xxx_dbPort" instead of just "dbPort". A
 * component may define (at least through {@link SQLDatabaseConfig2} at most one untagged endpoint,
 * and for that component, config properties are un-prefixed.</p>
 *
 * <p>Each endpoint is defined with a {@link SQLDialect} value that identifies the type of database
 * server accessed by the endpoint. Some config property defaults are based on this value.</p>
 *
 * <p>Relevant properties include:</p>
 * <dl>
 *     <dt>dbHost</dt>
 *     <dd>The host name or IP address of the DB server. No default.</dd>
 *     <dt>dbPort</dt>
 *     <dd>The port on which the server listens for new connections. Default is the standard port
 *     for server type, e.g. 3306 for MySql/MariaDB, 5432 for Postgres, etc.</dd>
 *     <dt>dbDatabaseName</dt>
 *     <dd>The name of the database to be accessed on the server. Defaults to the tag name for
 *     tagged endpoints, or to the underlying component name for untagged endpoints.</dd>
 *     <dt>dbSchemaName</dt>
 *     <dd>The name of the schema to be accessed (not relevant for MySql/MariaDB). Defaults to the
 *     tag name for tagged instances, or to the component name for untagged instances.</dd>
 *     <dt>dbUserName</dt>
 *     <dd>The non-root user name by which this endpoint will access the database. Defaults to the
 *     tag name for tagged instances, or to the component name for untagged instances.</dd>
 *     <dt>dbPassword</dt>
 *     <dd>The password for the non-root user. Defaults to the root DB password obtained from the
 *     auth component.</dd>
 *     <dt>dbRootUserName</dt>
 *     <dd>The name of a DB user with root (super) privileges that will be used to create and set up
 *     this endpoint's database, schema, and non-root user, if needed. Defaults to the user name
 *     provided by the auth component.</dd>
 *     <dt>dbRootPassword</dt>
 *     <dd>The password for the root user. Defaults to the password provided by the auth component.</dd>
 *     <dt>dbDriverProperties</dt>
 *     <dd>Map of name/value pairs for properties to be conveyed as JDBC URL query parameters when
 *     creating a connection to the database. Defaults are based on the {@link SQLDialect}. Values
 *     provided through configuration are merged into the defaults, and should be specified using
 *     the Spring (SPEL) syntax for map literals.</dd>
 *     <dt>dbSecure</dt>
 *     <dd>Boolean indicating whether the database should be accessed with a secure (SSL) connection.
 *     Default is false.</dd>
 *     <dt>dbMigrationLocation</dt>
 *     <dd>Package name (or names, separated by commas) defining the location where Flyway can find
 *     migrations for this database. Defaults to "db.migration".</dd>
 * </dl>
 *
 * <p>Some items that may seem like configuration are, like the `SQLDialect` value, treated as
 * un-injectable and left as part of the endpoint definition in the code. These are intimately
 * tied with a component's programmatic use of the database, and so exposing them to injectable
 * configuration is probably just asking for trouble. This includes:</p>
 *
 * <ul>
 *     <li>Access level (r/w, r/o, etc.)</li>
 *     <li>Enable/disable migrations</li>
 * </ul>
 */
public class DbEndpoint {
    private static final Logger logger = LogManager.getLogger();
    private static final SpelExpressionParser spel = new SpelExpressionParser();

    // names of properties for endpoint configuration (minus tag prefixes)

    /** dbHost property. */
    public static final String DB_HOST_PROPERTY = "dbHost";
    /** dbPort property. */
    public static final String DB_PORT_PROPERTY = "dbPort";
    /** dbDatabaseName property. */
    public static final String DB_DATABASE_NAME_PROPERTY = "dbDatabaseName";
    /** dbSchemaName property. */
    public static final String DB_SCHEMA_NAME_PROPERTY = "dbSchemaName";
    /** dbUserName property. */
    public static final String DB_USER_NAME_PROPERTY = "dbUserName";
    /** dbPassword property. */
    public static final String DB_PASSWORD_PROPERTY = "dbPassword";
    /** dbRootUserName property. */
    public static final String DB_ROOT_USER_NAME_PROPERTY = "dbRootUserName";
    /** dbRootPassword property. */
    public static final String DB_ROOT_PASSWORD_PROPERTY = "dbRootPassword";
    /** dbDriverProperties property. */
    public static final String DRIVER_PROPERTIES_PROPERTY = "dbDriverProperties";
    /** dbSecure property. */
    public static final String SECURE_PROPERTY_NAME = "dbSecure";
    /** dbMigrationLocation property. */
    public static final String DB_MIGRATION_LOCATION_PROPERTY = "dbMigrationLocation";

    // default values for some properties

    /** default port for MariaDB and MySql. */
    public static final int DEFAULT_MARIADB_MYSQL_PORT = 3306;
    /** default port for PostgreSQL. */
    public static final int DEFAULT_POSTGRES_PORT = 5432;
    /** default for secure connection. */
    public static final Boolean DEFAULT_SECURE_VALUE = Boolean.FALSE;
    /** default migration location. */
    public static final String DB_MIGRATION_LOCATION_DEFAULT = "db.migration";

    /** separator between tag name and property name when configuring tagged endpoints. */
    public static final String TAG_PREFIX_SEPARATOR = "_";

    private final String tag;
    private final SQLDialect dialect;
    private final DBPasswordUtil dbPasswordUtil;
    private final AtomicReference<UnaryOperator<String>> resolver;
    private final DbAdapter adapter;

    private boolean initialized = false;
    private FlywayCallback[] flywayCallbacks = new FlywayCallback[0];
    private DbEndpointAccess access = DbEndpointAccess.ALL;
    private boolean isMigrationEnabled = true;
    private DbEndpoint template;
    private final CountDownLatch initLatch1 = new CountDownLatch(1);
    private final CountDownLatch initLatch2 = new CountDownLatch(1);

    /**
     * Internal constructor for a new endpoint instance.
     *
     * <p>Client code should use {@link SQLDatabaseConfig2#primaryDbEndpoint(SQLDialect)} or
     * {@link SQLDatabaseConfig2#secondaryDbEndpoint(String, SQLDialect)} to declare endpoints.</p>
     *
     * @param tag            tag for secondary endpoint, or null for primary
     * @param dialect        server type, identified by {@link SQLDialect}
     * @param dbPasswordUtil a {@link DBPasswordUtil instance} for obtaining credentials from auth
     *                       component
     * @param resolver       method to resolve property names against Spring {@link Environment} and
     *                       {@link Value}-injected config fields
     * @throws UnsupportedDialectException if the server type is not supported
     */
    DbEndpoint(String tag, SQLDialect dialect, final DBPasswordUtil dbPasswordUtil,
            AtomicReference<UnaryOperator<String>> resolver)
            throws UnsupportedDialectException {
        this.tag = tag;
        this.dialect = dialect;
        this.dbPasswordUtil = dbPasswordUtil;
        this.resolver = resolver;
        this.adapter = DbAdapter.of(this);
    }

    /**
     * Use another endpoint as a template from which to obtain some default parameters for this
     * endpoint.
     *
     * <p>Explicit configuration of the property will be used in preference to the template values,
     * but the template values, if any, will be preferred over any built-in defaults.</p>
     *
     * <p>Properties that make use of template values include:</p>
     * <ul>
     *     <li>dbHost</li>
     *     <li>dbPort</li>
     *     <li>dbDatabaseName</li>
     *     <li>dbSchemaName</li>
     *     <li>dbRootUserName</li>
     *     <li>dbRootPassword</li>
     *     <li>dbDriverProperties</li>
     *     <li>dbSecure</li>
     * </ul>
     *
     * @param template endpoint to use as a template
     * @return this endpoint
     */
    public DbEndpoint like(final DbEndpoint template) {
        if (template.getDialect() != dialect) {
            throw new IllegalArgumentException(
                    String.format("Cannot create a %s database that is \"like()\" a %s database",
                            dialect, template.getDialect()));
        }
        this.template = template;
        this.isMigrationEnabled = false;
        return this;
    }

    /**
     * Specify database access level for this endpoint.
     *
     * @param access required access level
     * @return this endpoint
     */
    public DbEndpoint withAccess(DbEndpointAccess access) {
        this.access = access;
        return this;
    }

    public DbEndpointAccess getAccess() {
        return access;
    }

    /**
     * Specify tha this endpoint does not perform migrations.
     *
     * @return this endpoint
     */
    public DbEndpoint noMigration() {
        this.isMigrationEnabled = false;
        return this;
    }

    /**
     * Specify flyway callbacks that should be active during migrations for this endpoint.
     *
     * @param flywayCallbacks flyway callback objects
     * @return this endpoint
     */
    public DbEndpoint withFlywayCallbacks(FlywayCallback... flywayCallbacks) {
        this.flywayCallbacks = flywayCallbacks;
        return this;
    }

    public boolean isMigrationEnabled() {
        return isMigrationEnabled;
    }

    /**
     * Initialize this endpoint.
     *
     * <p>Initialization entails:</p>
     * <li>
     *     <ul>Resolving all endpoint properties</ul>
     *     <ul>Performing any necessary endpoint setup through database operations, e.g. creating
     *     database, schema and/or user; configuring user privileges; running migrations.</ul>
     * </li>
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if an error occurs performing database operations
     * @throws InterruptedException if interrupted
     */
    public void init() throws UnsupportedDialectException, SQLException, InterruptedException {
        // if we depend on another endpoint, make sure it's initialized first.
        if (!initialized) {
            if (template != null) {
                template.init();
            }
            // open up access to property resolvers, but don't allow client connectivity
            // yet
            initLatch1.countDown();
            adapter.init();
            initialized = true;
            // now open up fully
            initLatch2.countDown();
        }
    }

    /**
     * Create a {@link DSLContext} bound to this endpoint.
     *
     * @return dsl context
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException                if there's a problem gaining access
     * @throws InterruptedException if interrupted
     */
    public DSLContext dslContext() throws UnsupportedDialectException, SQLException, InterruptedException {
        try {
            initLatch2.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return DSL.using(getConfiguration());
    }

    /**
     * Get a {@link DataSource} bound to this endpoint.
     *
     * @return dataSource data source
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException                if there's a problem gaining access
     * @throws InterruptedException if interrupted
     */
    public DataSource datasource() throws UnsupportedDialectException, SQLException, InterruptedException {
        initLatch2.await();
        return adapter.getDataSource();
    }

    /**
     * Get a jOOQ {@link Configuration} object configured for and bound to this endpoint.
     *
     * @return the configuration object
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException                if there's a problem creating the configuration
     * @throws InterruptedException if interrupted
     */
    private Configuration getConfiguration() throws UnsupportedDialectException, SQLException, InterruptedException {
        DefaultConfiguration jooqConfiguration = new DefaultConfiguration();
        jooqConfiguration.set(connectionProvider());
        jooqConfiguration.set(new Settings()
                .withRenderNameStyle(RenderNameStyle.LOWER)
                // Set withRenderSchema to false to avoid rendering schema name in Jooq generated SQL
                // statement. For example, with false withRenderSchema, statement
                // "SELECT * FROM vmtdb.entities" will be changed to "SELECT * FROM entities".
                // And dynamically set schema name in the constructed JDBC connection URL to support
                // multi-tenant database.
                .withRenderSchema(false));
        jooqConfiguration.set(new DefaultExecuteListenerProvider(exceptionTranslator()));
        jooqConfiguration.set(dialect);
        return jooqConfiguration;
    }

    /**
     * Get a connection provider bound to this endpoint.
     *
     * @return the connection provider
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws SQLException                if there's a problem gaining access
     * @throws InterruptedException if interrupted
     */
    private DataSourceConnectionProvider connectionProvider()
            throws UnsupportedDialectException, SQLException, InterruptedException {
        init();
        return new DataSourceConnectionProvider(
                new TransactionAwareDataSourceProxy(
                        new LazyConnectionDataSourceProxy(
                                adapter.getDataSource())));
    }

    public SQLDialect getDialect() {
        return dialect;
    }

    /**
     * Resolve the dbHost property for this endpoint.
     *
     * @return the dbHost value
     * @throws InterruptedException if interrupted
     */
    public String getHost() throws InterruptedException {
        final String fromTemplate = template != null ? template.getHost() : null;
        return getTaggedPropertyValue(DB_HOST_PROPERTY, fromTemplate);
    }

    /**
     * Resolve the dbPort property for this endpoint.
     *
     * @return the port number
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws InterruptedException if interrupted
     */
    public int getPort() throws UnsupportedDialectException, InterruptedException {
        final String propValue = getPortAsString();
        return !Strings.isNullOrEmpty(propValue) ? Integer.parseInt(propValue) : getDefaultPort();
    }

    private String getPortAsString() throws UnsupportedDialectException, InterruptedException {
        final String fromTemplate = template != null ? template.getPortAsString() : null;
        final String fromDefault = Integer.toString(getDefaultPort());
        return getTaggedPropertyValue(DB_PORT_PROPERTY, fromTemplate, fromDefault);
    }

    /**
     * Resolve the dbDatabaseName property for this endpoint.
     *
     * @return the database name
     * @throws InterruptedException if interrupted
     */
    public String getDatabaseName() throws InterruptedException {
        final String fromTemplate = template != null ? template.getDatabaseName() : null;
        return getTaggedPropertyValue(DB_DATABASE_NAME_PROPERTY, fromTemplate, tag, getComponentName());
    }

    /**
     * Resolve the dbSchemaName for this endpoint.
     *
     * @return the schema name
     * @throws InterruptedException if interrupted
     */
    public String getSchemaName() throws InterruptedException {
        final String fromTemplate = template != null ? template.getSchemaName() : null;
        return getTaggedPropertyValue(DB_SCHEMA_NAME_PROPERTY, fromTemplate, tag, getComponentName());
    }

    /**
     * Resolve the dbUserName property for this endpoint.
     *
     * @return the user name
     * @throws InterruptedException if interrupted
     */
    public String getUserName() throws InterruptedException {
        return getTaggedPropertyValue(DB_USER_NAME_PROPERTY, tag, getComponentName());
    }

    /**
     * Resolve the dbPassword property for this endpoint.
     *
     * @return the password
     * @throws InterruptedException if interrupted
     */
    public String getPassword() throws InterruptedException {
        return getTaggedPropertyValue(DB_PASSWORD_PROPERTY, dbPasswordUtil.getSqlDbRootPassword());
    }

    /**
     * Resolve the dbRootUserName property for this endpoint.
     *
     * @return the root user name
     * @throws InterruptedException if interrupted
     */
    public String getRootUserName() throws InterruptedException {
        final String fromTemplate = template != null ? template.getRootUserName() : null;
        return getTaggedPropertyValue(DB_ROOT_USER_NAME_PROPERTY, fromTemplate, dbPasswordUtil.getSqlDbRootUsername());
    }

    /**
     * Resolve the dbRootPassword for this endpoint.
     *
     * @return the root password
     * @throws InterruptedException if interrupted
     */
    public String getRootPassword() throws InterruptedException {
        final String fromTemplate = template != null ? template.getRootPassword() : null;
        return getTaggedPropertyValue(DB_ROOT_PASSWORD_PROPERTY, fromTemplate, dbPasswordUtil.getSqlDbRootPassword());
    }


    /**
     * Resolve the dbDriverProperties property for this endpoint.
     *
     * @return the driver properties map
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws InterruptedException if interrupted
     */
    public Map<String, String> getDriverProperties() throws UnsupportedDialectException, InterruptedException {
        final Map<String, String> fromTemplate = template != null ? template.getDriverProperties() : null;
        if (fromTemplate != null) {
            return fromTemplate;
        }
        Map<String, String> props = new HashMap<>(getDefaultDriverProperties());
        final String injectedProperties = getTaggedPropertyValue(DRIVER_PROPERTIES_PROPERTY);
        if (injectedProperties != null) {
            @SuppressWarnings("unchecked")
            final Map<? extends String, ? extends String> injectedMap
                    = (Map<? extends String, ? extends String>)spel.parseRaw(injectedProperties).getValue();
            props.putAll(injectedMap != null ? injectedMap : Collections.emptyMap());
        }
        return props;
    }

    /**
     * Resolve the dbSecure property for this endpoint.
     *
     * @return true if secure connection is required
     * @throws InterruptedException if interrupted
     */
    public boolean isSecureConnectionRequired() throws InterruptedException {
        return Boolean.parseBoolean(
                getTaggedPropertyValue(SECURE_PROPERTY_NAME, DEFAULT_SECURE_VALUE.toString()));
    }

    /**
     * Resolve the dbMigrationLocation property for this endpoint.
     *
     * @return the migration location(s)
     * @throws InterruptedException if interrupted
     */
    public String getMigrationLocation() throws InterruptedException {
        return getTaggedPropertyValue(DB_MIGRATION_LOCATION_PROPERTY, DB_MIGRATION_LOCATION_DEFAULT);
    }

    /**
     * Get the flyway callbacks declared for this endpoint.
     *
     * @return flyway callbacks
     */
    public FlywayCallback[] getFlywayCallbacks() {
        return flywayCallbacks;
    }

    /**
     * Obtain a value for the given property, prefixing the name with this endpoint's tag if it has
     * one.
     *
     * <p>If the property cannot be resolved to a configuration value, the first non-null value
     * among the supplied defaults is returned (if any)</p>
     *
     * @param name          property name
     * @param defaultValues default values
     * @return configured or first non-null default value for property
     * @throws InterruptedException if interrupted
     */
    private String getTaggedPropertyValue(final String name, String... defaultValues) throws InterruptedException {
        // don't resolve any properties until we've begun initialization
        initLatch1.await();
        final String value;
        value = resolver.get().apply(taggedPropertyName(name));
        return Strings.isNullOrEmpty(value)
                ? Arrays.stream(defaultValues).filter(Objects::nonNull).findFirst().orElse(null)
                : value;
    }

    private String taggedPropertyName(String propertyName) {
        return tag != null ? tag + TAG_PREFIX_SEPARATOR + propertyName : propertyName;
    }

    /**
     * Get a connection URL for this endpoint, connecting to the endpoint's configured database.
     *
     * @return connection URL
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws InterruptedException if interrupted
     */
    public String getUrl() throws UnsupportedDialectException, InterruptedException {
        return getUrl(getDatabaseName());
    }

    /**
     * Get a connection URL for this endpoint, connecting to the given database.
     *
     * @param database name of database to connect to.
     * @return connection URL
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws InterruptedException if interrupted
     */
    public String getUrl(String database) throws UnsupportedDialectException, InterruptedException {
        return getUrlBuilder()
                .path(database != null ? "/" + database : "/")
                .build().toUriString();
    }

    @NotNull
    private UriComponentsBuilder getUrlBuilder() throws UnsupportedDialectException, InterruptedException {
        final UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
                .scheme("jdbc:" + getJdbcProtocol())
                .host(getHost())
                .port(getPort());
        getDriverProperties().forEach(builder::queryParam);
        return builder;
    }

    /**
     * Get the name of this component from configuration.
     *
     * <p>This is used as a default for some properties in an untagged endpoint.</p>
     *
     * @return component name, or null if not available
     */
    private String getComponentName() {
        return resolver.get().apply("component_type");
    }

    /**
     * Get the default dbPort value based on this endpoint's server type.
     *
     * @return default port
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    private int getDefaultPort() throws UnsupportedDialectException {
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
     * @return default driver properties
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     * @throws InterruptedException if interrupted
     */
    private Map<String, String> getDefaultDriverProperties() throws UnsupportedDialectException, InterruptedException {
        switch (dialect) {
            case MYSQL:
                return ImmutableMap.of(
                        "trustServerCertificate", Boolean.toString(isSecureConnectionRequired())
                );
            case MARIADB:
                return ImmutableMap.of(
                        "trustServerCertificate", Boolean.toString(isSecureConnectionRequired()),
                        "useServerPrepStmts", "true"
                );
            case POSTGRES:
                // set up for secure connection?
                return Collections.emptyMap();
            default:
                throw new UnsupportedDialectException(dialect);
        }
    }

    /**
     * Get the JDBC protocol string to use in connection URLs for this endpoint, based on the server
     * type.
     *
     * @return protocol string
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    private String getJdbcProtocol() throws UnsupportedDialectException {
        switch (dialect) {
            case MARIADB:
            case MYSQL:
                return dialect.getNameLC();
            case POSTGRES:
                return "postgresql";
            default:
                throw new UnsupportedDialectException(dialect);
        }
    }

    /**
     * Exception to throw when an unsupported {@link SQLDialect} value is used to configure an
     * endpoint.
     */
    public static class UnsupportedDialectException extends Exception {
        /**
         * Create a new instance with a message identifying the unsupported dialect value.
         *
         * @param dialect dialect value
         */
        public UnsupportedDialectException(SQLDialect dialect) {
            this("Unsupported SQLDialect: " + dialect.name());
        }

        /**
         * Create a new instance with the given message.
         *
         * @param s message
         */
        public UnsupportedDialectException(final String s) {
            super(s);
        }
    }

    /**
     * Create a jOOQ exception translator for this endpoint.
     *
     * @return exception translator
     */
    public JooqExceptionTranslator exceptionTranslator() {
        return new JooqExceptionTranslator();
    }

    /**
     * Enum declaring levels of access for database endpoints.
     */
    public enum DbEndpointAccess {

        /** Full access to the configured schema. */
        ALL,
        /** Ability to read and write to all objects in the database, but not to create new objects. */
        READ_WRITE_DATA,
        /**
         * Ability to read all objects in the database, but not to alter any data or create new
         * objects.
         */
        READ_ONLY
    }
}
