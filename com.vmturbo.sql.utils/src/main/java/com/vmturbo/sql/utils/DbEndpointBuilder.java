package com.vmturbo.sql.utils;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.SQLDialect;
import org.springframework.core.env.Environment;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;

/**
 * Builder class to create a DbEndpoint.
 *
 * <p>This class creates and fills in fields in a {@link DbEndpointConfig} object based on invoked
 * builder methods. The {@link #build()} method registers the config with {@link DbEndpoint} and
 * gets back a {@link Future} that, when completed, will yield a {@link DbEndpoint} instance fully
 * prepared for use. Completion needs to await complete construction of the Spring application
 * context, so that endpoint properties not provided to the builder can be resolved using the spring
 * {@link Environment}.</p>
 *
 * <p>To simplify client code, the future is wrapped in a {@link Supplier}, which is
 * returned by the {@link #build()} method.</p>
 */
public class DbEndpointBuilder {

    private final DbEndpointConfig config;
    private final DbEndpointCompleter endpointCompleter;
    private final Gson gson = new Gson();

    /**
     * Internal constructor for a new endpoint instance.
     *
     * <p>Client code should use {@link DbEndpointsConfig#dbEndpoint(String, SQLDialect)} or
     * {@link DbEndpointsConfig#abstractDbEndpoint(String, SQLDialect)} or {@link
     * DbEndpointsConfig#derivedDbEndpoint(String, DbEndpoint)} to declare endpoints.</p>
     *
     * @param name              name for this endpoint
     * @param dialect           server type, identified by {@link SQLDialect}
     * @param endpointCompleter The {@link DbEndpointCompleter} that will complete the resulting
     *                          endpoint when the server is ready.
     */
    DbEndpointBuilder(final String name, final SQLDialect dialect,
            @Nonnull final DbEndpointCompleter endpointCompleter) {
        config = new DbEndpointConfig(name);
        config.setDialect(dialect);
        this.endpointCompleter = endpointCompleter;
    }

    /**
     * Obtain a "preview" of an endpoint being built by a builder. Unlike the {@link #build()}
     * method, this does not register the endpoint for completion. The returned {@link
     * DbEndpointConfig} object is a copy of the one under construction, so any changes made to it
     * will not affect the endpoint being built.
     * @return a "preview" of an endpoint being built by a builder.
     */
    public DbEndpointConfig preview() {
        return gson.fromJson(gson.toJson(config), DbEndpointConfig.class);
    }

    /**
     * Complete the build of this endpoint.
     *
     * <p>THe endpoint will not be ready to use until after completion of Spring application
     * context construction, so a {@link Supplier} is returned, which can be used to obtain the
     * endpoint at a later time. The supplier will block if it is used before the endpoint is
     * prepared.</p>
     *
     * @return a {@link Supplier} that can be used to obtain the fully initialized endpoint
     */
    public DbEndpoint build() {
        return endpointCompleter.register(config);
    }

    public SQLDialect getDialect() {
        return config.getDialect();
    }

    /**
     * Specify a host property value for this endpoint.
     *
     * @param host property value
     * @return this endpoint
     */
    public DbEndpointBuilder withHost(@Nonnull String host) {
        config.setHost(Objects.requireNonNull(host));
        return this;
    }

    /**
     * Specify a port property value for this endpoint.
     *
     * @param port property value
     * @return this endpoint
     */
    public DbEndpointBuilder withPort(int port) {
        config.setPort(port);
        return this;
    }

    /**
     * Specify a databaseName property value for this endpoint.
     *
     * @param databaseName property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDatabaseName(@Nonnull String databaseName) {
        config.setDatabaseName(Objects.requireNonNull(databaseName));
        return this;
    }

    /**
     * Specify a schemaName property value for this endpoint.
     *
     * @param schemaName property value
     * @return this endpoint
     */
    public DbEndpointBuilder withSchemaName(@Nonnull String schemaName) {
        config.setSchemaName(Objects.requireNonNull(schemaName));
        return this;
    }

    /**
     * Specify a userName property value for this endpoint.
     *
     * @param userName property value
     * @return this endpoint
     */
    public DbEndpointBuilder withUserName(@Nonnull String userName) {
        config.setUserName(Objects.requireNonNull(userName));
        return this;
    }

    /**
     * Specify a password property value for this endpoint.
     *
     * @param password property value
     * @return this endpoint
     */
    public DbEndpointBuilder withPassword(@Nonnull String password) {
        config.setPassword(Objects.requireNonNull(password));
        return this;
    }

    /**
     * Specify an access property value for this endpoint.
     *
     * @param access property value
     * @return this endpoint
     */
    public DbEndpointBuilder withAccess(@Nonnull DbEndpointAccess access) {
        config.setAccess(Objects.requireNonNull(access));
        return this;
    }

    /**
     * Specify a rootUserName property value for this endpoint.
     *
     * @param rootUserName property value
     * @return this endpoint
     */
    public DbEndpointBuilder withRootUserName(@Nonnull String rootUserName) {
        config.setRootUserName(Objects.requireNonNull(rootUserName));
        return this;
    }

    /**
     * Specify a rootPassword property value for this endpoint.
     *
     * @param rootPassword property value
     * @return this endpoint
     */
    public DbEndpointBuilder withRootPassword(@Nonnull String rootPassword) {
        config.setRootPassword(Objects.requireNonNull(rootPassword));
        return this;
    }

    /**
     * Specify whether root access is enabled for this endpoint. Provisioning operations that would
     * normally require root access will fail without attempting to obtain a connection if this is
     * disabled.
     *
     * @param rootAccessEnabled true of root access is enabled
     * @return this endpoint
     */
    public DbEndpointBuilder withRootAccessEnabled(boolean rootAccessEnabled) {
        config.setRootAccessEnabled(rootAccessEnabled);
        return this;
    }

    /**
     * Specify a driverProperties property value for this endpoint.
     *
     * @param driverProperties property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDriverProperties(@Nonnull Map<String, String> driverProperties) {
        config.setDriverProperties(ImmutableMap.copyOf(Objects.requireNonNull(driverProperties)));
        return this;
    }

    /**
     * Specify a secure property value for this endpoint.
     *
     * @param secure property value
     * @return this endpoint
     */
    public DbEndpointBuilder withSecure(boolean secure) {
        config.setSecure(secure);
        return this;
    }

    /**
     * Specify a migrationLocations property value for this endpoint.
     *
     * @param migrationLocations property value
     * @return this endpoint
     */
    public DbEndpointBuilder withMigrationLocations(String... migrationLocations) {
        config.setMigrationLocations(String.join(",", migrationLocations));
        return this;
    }

    /**
     * Suppress migrations for this endpoint.
     *
     * @return this endpoint
     */
    public DbEndpointBuilder withNoMigrations() {
        config.setMigrationLocations("");
        return this;
    }

    /**
     * Specify a flywayCallbacks property value for this endpoint.
     *
     * @param flywayCallbacks property value
     * @return this endpoint
     */
    public DbEndpointBuilder withFlywayCallbacks(FlywayCallback... flywayCallbacks) {
        config.setFlywayCallbacks(flywayCallbacks);
        return this;
    }

    /**
     * Specify a endpointEnabled property value for this endpoint.
     *
     * <p>Setting this to false will prevent initialization of the endpoint. It may be useful in
     * some debugging and testing scenarios, allowing the effective removal of an endpoint without
     * actually removing it from the code.
     *
     * @param endpointEnabled property value
     * @return this endpoint
     */
    public DbEndpointBuilder withEndpointEnabled(boolean endpointEnabled) {
        config.setEndpointEnabledFn(r -> endpointEnabled);
        return this;
    }

    /**
     * Specify a function to determine, during endpoint resolution, whether this endpoint should be
     * enabled or disabled.
     *
     * <p>The function will be handed the same "resolve" object used by the endpoint resolver,
     * in the form of a unary String operator (i.e. a String->String function). This can be used
     * to interrogate configured properties on which enablement may depend.</p>
     *
     * @param fn function to determine whether this endpoint should be enabled
     * @return this endpoint
     */
    public DbEndpointBuilder withEndpointEnabled(Function<UnaryOperator<String>, Boolean> fn) {
        config.setEndpointEnabledFn(fn);
        return this;
    }

    /**
     * Specify that this endpoint is abstract, and will never be made operational.
     *
     * @return this endpoint
     */
    public DbEndpointBuilder setAbstract() {
        config.setAbstract();
        return this;
    }

    /**
     * Enable or disable both database and user provisioning for this endpoint.
     *
     * @param shouldProvision whether database and user provisioning should be done
     * @return this endpoint
     */
    public DbEndpointBuilder withShouldProvision(boolean shouldProvision) {
        return withShouldProvisionDatabase(shouldProvision)
                .withShouldProvisionUser(shouldProvision);
    }

    /**
     * Enable or disable database provisioning for this endpoint.
     *
     * @param shouldProvisionDatabase whether database provisioning should be done
     * @return this endpoint
     */
    public DbEndpointBuilder withShouldProvisionDatabase(boolean shouldProvisionDatabase) {
        config.setShouldProvisionDatabase(shouldProvisionDatabase);
        return this;
    }

    /**
     * Enable or disable user provisioning for this endpoint.
     *
     * @param shouldProvisionUser whether user provisioning should be done
     * @return this endpoint
     */
    public DbEndpointBuilder withShouldProvisionUser(boolean shouldProvisionUser) {
        config.setShouldProvisionUser(shouldProvisionUser);
        return this;
    }

    /**
     * Specify that this endpoint should be configured like another used as a template.
     *
     * <p>See {@link DbEndpoint} for a list of the template properties that are used in for
     * the endpoint being constructed, where those properties have not been specified in this
     * endpoint's builder.</p>
     *
     * @param templateSupplier {@link Supplier} that will yield the template endpoint when it's
     *                         ready
     * @return this builder
     */
    public DbEndpointBuilder like(DbEndpoint templateSupplier) {
        config.setTemplate(templateSupplier);
        return this;
    }

    /**
     * Specify whether to use a connection pool for database connections.
     *
     * @param useConnectionPool if true, use a connection pool for database connections
     * @return this endpoint
     */
    public DbEndpointBuilder withUseConnectionPool(boolean useConnectionPool) {
        config.setUseConnectionPool(useConnectionPool);
        return this;
    }

    /**
     * Specify a min connection pool size property value for this endpoint.
     *
     * @param minPoolSize minimum number of connections to maintain in the pool
     * @return this endpoint
     */
    public DbEndpointBuilder withMinPoolSize(int minPoolSize) {
        config.setMinPoolSize(minPoolSize);
        return this;
    }

    /**
     * Specify a max connection pool size property value for this endpoint.
     *
     * @param maxPoolSize maximum number of connections to maintain in the pool
     * @return this endpoint
     */
    public DbEndpointBuilder withMaxPoolSize(int maxPoolSize) {
        config.setMaxPoolSize(maxPoolSize);
        return this;
    }

    /**
     * Specify DB plugins needed by this endpoint.
     * @param plugins required plugins
     * @return this endpoint builder
     */
    public DbEndpointBuilder withPlugins(DbPlugin... plugins) {
        config.setPlugins(Arrays.asList(plugins));
        return this;
    }

    /**
     * Specify the Pool monitor interval in seconds.
     * @param poolMonitorSecs the interval in seconds
     * @return this endpoint builder
     */
    public DbEndpointBuilder withPoolMonitorIntervalSec(int poolMonitorSecs) {
        config.setPoolMonitorIntervalSec(poolMonitorSecs);
        return this;
    }

    /**
     * Specify a rootDatabaseName property value for this endpoint.
     *
     * @param rootDatabaseName property value
     * @return this endpoint
     */
    public DbEndpointBuilder withRootDatabaseName(String rootDatabaseName) {
        config.setRootDatabaseName(rootDatabaseName);
        return this;
    }

    /**
     * Specify a provisioning prefix that will be added to certain endpoint properties.
     *
     * <p>This might be used for multi-tenant scenarios, or with Kibitzer activities.</p>
     *
     * @param provisioningPrefix prefix to be used
     * @return this endpoint builder
     */
    public DbEndpointBuilder withProvisioningPrefix(String provisioningPrefix) {
        config.setProvisioningPrefix(provisioningPrefix);
        return this;
    }

    @Override
    public String toString() {
        return config.detailedToString();
    }
}
