package com.vmturbo.sql.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

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
 *         with the {@link DbEndpointBuilder#like(DbEndpoint)}  method. Not all properties are
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
    /** dbProvisioningSuffix property. */
    public static final String DB_NAME_SUFFIX_PROPERTY = "dbNameSuffix";

    /** system property name for retrieving component name for certain property defaults. */
    public static final String COMPONENT_TYPE_PROPERTY = "component_type";

    // default values for some properties

    /** default port for MariaDB and MySql. */
    public static final int DEFAULT_MARIADB_MYSQL_PORT = 3306;
    /** default port for PostgreSQL. */
    public static final int DEFAULT_POSTGRES_PORT = 5432;
    /** default for secure connection. */
    public static final Boolean DEFAULT_DB_SECURE_VALUE = Boolean.FALSE;
    /** default migration location. */
    public static final String DEFAULT_DB_MIGRATION_LOCATION_VALUE = "db.migration";
    /** default for access level. */
    public static final String DEFAULT_DB_ACCESS_VALUE = DbEndpointAccess.READ_ONLY.name();

    /** separator between tag name and property name when configuring tagged endpoints. */
    public static final String TAG_PREFIX_SEPARATOR = "_";

    private static final SpelExpressionParser spel = new SpelExpressionParser();

    private final DBPasswordUtil dbPasswordUtil;

    private final DbEndpointConfig config;
    private final DbEndpointConfig template;
    private final String tag;
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
        this.tag = config.getTag();
        this.dialect = config.getDialect();
        this.template = config.getTemplate() != null ? config.getTemplate().getConfig() : null;
    }

    void resolve() throws UnsupportedDialectException {
        resolveDbProvisioningSuffix();
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
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    private void resolveDbHost() throws UnsupportedDialectException {
        final String fromTemplate = template != null ? template.getDbHost() : null;
        config.setDbHost(choosePropertyValue(dbTag(DB_HOST_PROPERTY),
                or(config.getDbHost(), fromTemplate)));
    }

    /**
     * Resolve the dbPort property for this endpoint.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    private void resolveDbPort() throws UnsupportedDialectException {
        String fromTemplate = template != null ? Integer.toString(template.getDbPort()) : null;
        String fromDefault = Integer.toString(getDefaultPort(dialect));
        String curretnValue = config.getDbPort() != null ? config.getDbPort().toString() : null;
        String propValue = choosePropertyValue(dbTag(DB_PORT_PROPERTY),
                or(curretnValue, fromTemplate, fromDefault));
        config.setDbPort(Integer.parseInt(propValue));
    }

    /**
     * Resolve the dbDatabaseName property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbDatabaseName() throws UnsupportedDialectException {
        final String fromTemplate = template != null ? template.getDbDatabaseName() : null;
        final String value = choosePropertyValue(dbTag(DB_DATABASE_NAME_PROPERTY),
                or(config.getDbDatabaseName(), fromTemplate, tag, getComponentName()));
        config.setDbDatabaseName(addSuffix(value));
    }

    /**
     * Resolve the dbSchemaName for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbSchemaName() throws UnsupportedDialectException {
        final String fromTemplate = template != null ? template.getDbSchemaName() : null;
        final String value = choosePropertyValue(dbTag(DB_SCHEMA_NAME_PROPERTY),
                or(config.getDbSchemaName(), fromTemplate, tag, getComponentName()));
        config.setDbSchemaName(addSuffix(value));
    }

    /**
     * Resolve the dbUserName property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbUserName() throws UnsupportedDialectException {
        final String value = choosePropertyValue(dbTag(DB_USER_NAME_PROPERTY),
                or(config.getDbUserName(), tag, getComponentName()));
        config.setDbUserName(addSuffix(value));
    }

    /**
     * Resolve the dbPassword property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbPassword() throws UnsupportedDialectException {
        config.setDbPassword(choosePropertyValue(dbTag(DB_PASSWORD_PROPERTY),
                or(config.getDbPassword(), dbPasswordUtil.getSqlDbRootPassword())));
    }

    /**
     * Resolve the dbAccess property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbAccess() throws UnsupportedDialectException {
        String currentValue = config.getDbAccess() != null ? config.getDbAccess().name() : null;
        config.setDbAccess(DbEndpointAccess.valueOf(choosePropertyValue(
                dbTag(DB_ACCESS_PROPERTY), or(currentValue, DEFAULT_DB_ACCESS_VALUE))));
    }

    /**
     * Resolve the dbRootUserName property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbRootUserName() throws UnsupportedDialectException {
        final String fromTemplate = template != null ? template.getDbRootUserName() : null;
        config.setDbRootUserName(choosePropertyValue(dbTag(DB_ROOT_USER_NAME_PROPERTY),
                or(config.getDbRootUserName(), fromTemplate,
                        dbPasswordUtil.getSqlDbRootUsername(dialect.toString()))));
    }

    /**
     * Resolve the dbRootPassword for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbRootPassword() throws UnsupportedDialectException {
        final String fromTemplate = template != null ? template.getDbRootPassword() : null;
        config.setDbRootPassword(choosePropertyValue(dbTag(DB_ROOT_PASSWORD_PROPERTY),
                or(config.getDbRootPassword(), fromTemplate, dbPasswordUtil.getSqlDbRootPassword())));
    }


    /**
     * Resolve the dbDriverProperties property for this endpoint.
     *
     * @throws UnsupportedDialectException if this endpoint is mis-configured
     */
    public void resolveDbDriverProperties() throws UnsupportedDialectException {
        Map<String, String> base = config.getDbDriverProperties();
        if (base == null) {
            base = template != null ? template.getDbDriverProperties() : null;
        }
        if (base == null) {
            base = new HashMap<>(getDefaultDriverProperties(dialect));
        }
        final String injectedProperties = choosePropertyValue(dbTag(DRIVER_PROPERTIES_PROPERTY));
        if (injectedProperties != null) {
            @SuppressWarnings("unchecked")
            final Map<? extends String, ? extends String> injectedMap
                    = (Map<? extends String, ? extends String>)spel.parseRaw(injectedProperties).getValue();
            base.putAll(injectedMap != null ? injectedMap : Collections.emptyMap());
        }
        config.setDbDriverProperties(base);
    }

    /**
     * Resolve the dbSecure property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbSecure() throws UnsupportedDialectException {
        if (config.getDbSecure() == null) {
            final String fromTemplate = template != null
                    ? Boolean.toString(template.getDbSecure()) : null;
            final String currentValue =
                    config.getDbSecure() != null ? Boolean.toString(config.getDbSecure()) : null;
            config.setDbSecure(Boolean.parseBoolean(choosePropertyValue(dbTag(SECURE_PROPERTY_NAME),
                    or(currentValue, fromTemplate, DEFAULT_DB_SECURE_VALUE.toString()))));
        }
    }

    /**
     * Resolve the dbMigrationLocations property for this endpoint.
     */
    public void resolveDbMigrationLocations() {
        if (config.getDbMigrationLocations() == null) {
            String defaultLocations = template != null ? "" : DEFAULT_DB_MIGRATION_LOCATION_VALUE;
            config.setDbMigrationLocations(choosePropertyValue(tag(DB_MIGRATION_LOCATIONS_PROPERTY),
                    or(config.getDbMigrationLocations(), defaultLocations)));
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
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbDestructiveProvisioningEnabled() throws UnsupportedDialectException {
        final String currentValue = config.getDbDestructiveProvisioningEnabled() != null
                ? config.getDbDestructiveProvisioningEnabled().toString() : null;
        config.setDbDestructiveProvisioningEnabled(Boolean.parseBoolean(
                choosePropertyValue(dbTag(DB_DESTRUCTIVE_PROVISIONING_ENABLED_PROPERTY),
                        or(currentValue, Boolean.FALSE.toString()))));
    }

    /**
     * Resolve the dbEndpointEnabled property for this endpoint.
     *
     * @throws UnsupportedDialectException if endpoint has bad dialect
     */
    public void resolveDbEndpointEnabled() throws UnsupportedDialectException {
        final String currentValue = config.getDbEndpointEnabled() != null
                ? config.getDbEndpointEnabled().toString() : null;
        config.setDbEndpointEnabled(Boolean.parseBoolean(
                choosePropertyValue(dbTag(DB_ENDPOINT_ENABLED_PROPERTY),
                        or(currentValue, Boolean.TRUE.toString()))));
    }


    /**
     * Resolve the dbProvisioningSuffix property for this endpoint.
     */
    public void resolveDbProvisioningSuffix() {
        config.setDbNameSuffix(
                choosePropertyValue(tag(DB_NAME_SUFFIX_PROPERTY), or(config.getDbNameSuffix(), "")));
    }

    /**
     * Obtain a value for a property name, choosing the first non-null string among offered
     * choices.
     *
     * <p>Choices appear string streams, because certain options for some properties yield multiple
     * choices.</p>
     *
     * @param choiceStreams streams providing choices to be considered for this property
     * @return first non-null choice, or null if there was none
     */
    private String choosePropertyValue(final Stream<String>... choiceStreams) {
        return Arrays.stream(choiceStreams)
                .flatMap(Function.identity())
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    private Stream<String> dbTag(String propertyName) throws UnsupportedDialectException {
        String undbName = propertyName.startsWith("db") ? propertyName.substring(2) : propertyName;
        List<String> choices = new ArrayList<>();
        for (String tagPrefix : (Iterable<String>)tagPrefixes()::iterator) {
            for (String dbPrefix : dialectPropertyPrefixes()) {
                final String name = tagPrefix + (dbPrefix != null ? dbPrefix + undbName : propertyName);
                choices.add(resolver.apply(name));
            }
        }
        return choices.stream();
    }

    private List<String> dialectPropertyPrefixes() throws UnsupportedDialectException {
        switch (dialect) {
            case MYSQL:
                return Arrays.asList("mysql", "mariadb", null);
            case MARIADB:
                return Arrays.asList("mariadb", "mysql", null);
            case POSTGRES:
                return Arrays.asList("postgres", null);
            default:
                throw new UnsupportedDialectException(dialect);
        }
    }

    private Stream<String> tag(String propertyName) {
        return tagPrefixes().map(pfx -> pfx + propertyName).map(resolver);
    }

    private Stream<String> tagPrefixes() {
        return tag != null ? Stream.of(tag + "_", "") : Stream.of("");
    }

    private Stream<String> or(String... choices) {
        return Stream.of(choices);
    }

    /**
     * Prepend the tag, if non-null, to the given property name.
     *
     * @param tag          tag, or null for no tag
     * @param propertyName property name to tag
     * @return tag-prefixed property name
     */
    public static String taggedPropertyName(String tag, String propertyName) {
        return tag != null ? tag + TAG_PREFIX_SEPARATOR + propertyName : propertyName;
    }

    private String addSuffix(String value) {
        final String suffix = config.getDbNameSuffix();
        return !Strings.isNullOrEmpty(suffix) && !value.endsWith(suffix)
                ? value + suffix
                : value;
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
