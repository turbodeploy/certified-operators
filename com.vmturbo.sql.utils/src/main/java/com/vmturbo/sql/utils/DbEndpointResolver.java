package com.vmturbo.sql.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.SQLDialect;
import org.springframework.core.env.Environment;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

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
 *         with the {@link DbEndpointBuilder#like(Supplier)} method. Not all properties are
 *         copied from the template; see {@link DbEndpoint} for details.
 *     </li>
 *     <li>Built-in defaults; see {@link DbEndpoint} for per-property default details.</li>
 * </ul>
 */
public class DbEndpointResolver {

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
    /** dbAccess property. */
    public static final String DB_ACCESS_PROPERTY = "dbAccess";
    /** dbRootUserName property. */
    public static final String DB_ROOT_USER_NAME_PROPERTY = "dbRootUserName";
    /** dbRootPassword property. */
    public static final String DB_ROOT_PASSWORD_PROPERTY = "dbRootPassword";
    /** dbDriverProperties property. */
    public static final String DRIVER_PROPERTIES_PROPERTY = "dbDriverProperties";
    /** dbSecure property. */
    public static final String SECURE_PROPERTY_NAME = "dbSecure";
    /** dbMigrationLocations property. */
    public static final String DB_MIGRATION_LOCATIONS_PROPERTY = "dbMigrationLocation";
    /** dbDestructiveProvisioningEnabled property. */
    public static final String DB_DESTRUCTIVE_PROVISIONING_ENABLED_PROPERTY = "dbDestructiveProvisioningEnabled";
    /** dbEndpointEnabled property. */
    public static final String DB_ENDPOINT_ENABLED_PROPERTY = "dbEndpointEnabled";

    // default values for some properties

    /** default port for MariaDB and MySql. */
    public static final int DEFAULT_MARIADB_MYSQL_PORT = 3306;
    /** default port for PostgreSQL. */
    public static final int DEFAULT_POSTGRES_PORT = 5432;
    /** default for secure connection. */
    public static final Boolean DEFAULT_DB_SECURE_VALUE = Boolean.FALSE;
    /** default migration location. */
    public static final String DEFAULT_DB_MITRATION_LOCATION_VALUE = "db.migration";
    /** default for access level. */
    public static final String DEFAUT_DB_ACCESS_VALUE = DbEndpointAccess.READ_ONLY.name();

    /** separator between tag name and property name when configuring tagged endpoints. */
    public static final String TAG_PREFIX_SEPARATOR = "_";

    private static final SpelExpressionParser spel = new SpelExpressionParser();

    private DBPasswordUtil dbPasswordUtil;

    private DbEndpointConfig config;
    private DbEndpointConfig template;
    private String tag;
    private SQLDialect dialect;
    private UnaryOperator<String> resolver;

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
        this.tag = config.getTag();
        this.dialect = config.getDialect();
        this.template = getTemplate(config.getTemplateSupplier());
    }

    private DbEndpointConfig getTemplate(Supplier<DbEndpoint> templateSupplier) {
        if (templateSupplier == null) {
            return null;
        }
        Throwable error = null;
        try {
            return templateSupplier.get().getConfig();
        } catch (Exception e) {
            error = e;
        }
        throw new IllegalStateException(
                "DbEndpoint cannot be resolved because its template endpoint is not available", error);
    }

    void resolve() throws UnsupportedDialectException {
        resolveDbHost();
        resolveDbPort();
        resolveDbDatabaseName();
        resolveDbSchemaName();
        resolveDbUserName();
        resolveDbPassword();
        resolveDbAccess();
        resolveDbRootUserName();
        resolveDbRootPassword();
        resolveDbDriverProperties();
        resolveDbSecure();
        resolveDbMigrationLocations();
        resolveDbFlywayCallbacks();
        resolveDbDestructiveProvisioningEnabled();
        resolveDbEndpointEnabled();
    }

    /**
     * Resolve the dbHost property for this endpoint.
     */
    private void resolveDbHost() {
        if (config.getDbHost() == null) {
            final String fromTemplate = template != null ? template.getDbHost() : null;
            config.setDbHost(getTaggedPropertyValue(DB_HOST_PROPERTY, fromTemplate));
        }
    }

    /**
     * Resolve the dbPort property for this endpoint.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    private void resolveDbPort() throws UnsupportedDialectException {
        if (config.getDbPort() == null) {
            String fromTemplate = template != null ? Integer.toString(template.getDbPort()) : null;
            String fromDefault = Integer.toString(getDefaultPort());
            String propValue = getTaggedPropertyValue(DB_PORT_PROPERTY, fromTemplate, fromDefault);
            config.setDbPort(Integer.parseInt(propValue));
        }
    }

    /**
     * Resolve the dbDatabaseName property for this endpoint.
     */
    public void resolveDbDatabaseName() {
        if (config.getDbDatabaseName() == null) {
            final String fromTemplate = template != null ? template.getDbDatabaseName() : null;
            config.setDbDatabaseName(getTaggedPropertyValue(DB_DATABASE_NAME_PROPERTY, fromTemplate, tag, getComponentName()));
        }
    }

    /**
     * Resolve the dbSchemaName for this endpoint.
     */
    public void resolveDbSchemaName() {
        if (config.getDbSchemaName() == null) {
            final String fromTemplate = template != null ? template.getDbSchemaName() : null;
            config.setDbSchemaName(getTaggedPropertyValue(DB_SCHEMA_NAME_PROPERTY, fromTemplate, tag, getComponentName()));
        }
    }

    /**
     * Resolve the dbUserName property for this endpoint.
     */
    public void resolveDbUserName() {
        if (config.getDbUserName() == null) {
            config.setDbUserName(getTaggedPropertyValue(DB_USER_NAME_PROPERTY, tag, getComponentName()));
        }
    }

    /**
     * Resolve the dbPassword property for this endpoint.
     */
    public void resolveDbPassword() {
        if (config.getDbPassword() == null) {
            config.setDbPassword(getTaggedPropertyValue(DB_PASSWORD_PROPERTY, dbPasswordUtil.getSqlDbRootPassword()));
        }
    }

    /**
     * Resolve the dbAccess property for this endpoint.
     */
    public void resolveDbAccess() {
        if (config.getDbAccess() == null) {
            config.setDbAccess(DbEndpointAccess.valueOf(
                    getTaggedPropertyValue(DB_ACCESS_PROPERTY, DEFAUT_DB_ACCESS_VALUE)));
        }
    }

    /**
     * Resolve the dbRootUserName property for this endpoint.
     */
    public void resolveDbRootUserName() {
        if (config.getDbRootUserName() == null) {
            final String fromTemplate = template != null ? template.getDbRootUserName() : null;
            config.setDbRootUserName(getTaggedPropertyValue(DB_ROOT_USER_NAME_PROPERTY, fromTemplate,
                    dbPasswordUtil.getSqlDbRootUsername(dialect.toString())));
        }
    }

    /**
     * Resolve the dbRootPassword for this endpoint.
     */
    public void resolveDbRootPassword() {
        if (config.getDbRootPassword() == null) {
            final String fromTemplate = template != null ? template.getDbRootPassword() : null;
            config.setDbRootPassword(getTaggedPropertyValue(DB_ROOT_PASSWORD_PROPERTY, fromTemplate,
                    dbPasswordUtil.getSqlDbRootPassword()));
        }
    }


    /**
     * Resolve the dbDriverProperties property for this endpoint.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    public void resolveDbDriverProperties() throws UnsupportedDialectException {
        if (config.getDbDriverProperties() == null) {
            final Map<String, String> fromTemplate = template != null ? template.getDbDriverProperties() : null;
            if (fromTemplate != null) {
                config.setDbDriverProperties(fromTemplate);
            } else {
                Map<String, String> props = new HashMap<>(getDefaultDriverProperties());
                final String injectedProperties = getTaggedPropertyValue(DRIVER_PROPERTIES_PROPERTY);
                if (injectedProperties != null) {
                    @SuppressWarnings("unchecked")
                    final Map<? extends String, ? extends String> injectedMap
                            = (Map<? extends String, ? extends String>)spel.parseRaw(injectedProperties).getValue();
                    props.putAll(injectedMap != null ? injectedMap : Collections.emptyMap());
                }
                config.setDbDriverProperties(props);
            }
        }
    }

    /**
     * Resolve the dbSecure property for this endpoint.
     */
    public void resolveDbSecure() {
        if (config.getDbSecure() == null) {
            final String fromTemplate = template != null
                    ? Boolean.toString(template.getDbSecure()) : null;
            config.setDbSecure(Boolean.parseBoolean(
                    getTaggedPropertyValue(SECURE_PROPERTY_NAME,
                            fromTemplate, DEFAULT_DB_SECURE_VALUE.toString())));
        }
    }

    /**
     * Resolve the dbMigrationLocations property for this endpoint.
     */
    public void resolveDbMigrationLocations() {
        if (config.getDbMigrationLocations() == null) {
            String defaultLocations = template != null ? "" : DEFAULT_DB_MITRATION_LOCATION_VALUE;
            config.setDbMigrationLocations(getTaggedPropertyValue(DB_MIGRATION_LOCATIONS_PROPERTY,
                    defaultLocations));
        }
    }

    /**
     * Get the flyway callbacks declared for this endpoint.
     */
    public void resolveDbFlywayCallbacks() {
        if (config.getDbFlywayCallbacks() == null) {
            config.setDbFlywayCallbacks(new FlywayCallback[0]);
        }
    }

    /**
     * Resolve the dbDestructiveProvisioningEnabled property for this endpoint.
     */
    public void resolveDbDestructiveProvisioningEnabled() {
        if (config.getDbDestructiveProvisioningEnabled() == null) {
            config.setDbDestructiveProvisioningEnabled(Boolean.parseBoolean(
                    getTaggedPropertyValue(DB_DESTRUCTIVE_PROVISIONING_ENABLED_PROPERTY,
                            Boolean.FALSE.toString())));
        }
    }

    /**
     * Resolve the dbEndpointEnabled property for this endpoint.
     */
    public void resolveDbEndpointEnabled() {
        if (config.getDbEndpointEnabled() == null) {
            config.setDbEndpointEnabled(Boolean.parseBoolean(
                    getTaggedPropertyValue(DB_ENDPOINT_ENABLED_PROPERTY, Boolean.TRUE.toString())));
        }
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
     */
    private String getTaggedPropertyValue(final String name, String... defaultValues) {
        final String value = resolver.apply(taggedPropertyName(name));
        return Strings.isNullOrEmpty(value)
                ? Arrays.stream(defaultValues).filter(Objects::nonNull).findFirst().orElse(null)
                : value;
    }

    private String taggedPropertyName(String propertyName) {
        return tag != null ? tag + TAG_PREFIX_SEPARATOR + propertyName : propertyName;
    }

    /**
     * Get the name of this component from configuration.
     *
     * <p>This is used as a default for some properties in an untagged endpoint.</p>
     *
     * @return component name, or null if not available
     */
    private String getComponentName() {
        return resolver.apply("component_type");
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
     */
    private Map<String, String> getDefaultDriverProperties() throws UnsupportedDialectException {
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
