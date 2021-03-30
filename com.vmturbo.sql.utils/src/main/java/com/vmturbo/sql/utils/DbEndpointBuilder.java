package com.vmturbo.sql.utils;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

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

    /**
     * Internal constructor for a new endpoint instance.
     *
     * <p>Client code should use {@link SQLDatabaseConfig2#dbEndpoint(String, SQLDialect)} or
     * {@link SQLDatabaseConfig2#abstractDbEndpoint(String, SQLDialect)} or {@link
     * SQLDatabaseConfig2#derivedDbEndpoint(String, DbEndpoint)} to declare endpoints.</p>
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
     * Specify a access property value for this endpoint.
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
     * Specify a destructiveProvisioningEnabled property value for this endpoint.
     *
     * <p>This will permit operations like dropping a user or a database to be performed using
     * root credentials, during endpoint initialization, in order to attempt to fix a situation
     * where the non-root user credentials are found not to work.</p>
     *
     * @param destructiveProvisioningEnabled property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDestructiveProvisioningEnabled(boolean destructiveProvisioningEnabled) {
        config.setDestructiveProvisioningEnabled(destructiveProvisioningEnabled);
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
     * Set a provisioning suffix for this endpoint.
     *
     * <p>This is normally used in tests, and affects names of provisioned database objects.</p>
     *
     * @param provisioningSuffix provisioning suffix
     * @return this endpoint
     */
    public DbEndpointBuilder withProvisioningSuffix(String provisioningSuffix) {
        config.setProvisioningSuffix(provisioningSuffix);
        return this;
    }

    /**
     * Specify that this endpoint should be configured like another used as a template..
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
}
