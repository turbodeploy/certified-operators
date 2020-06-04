package com.vmturbo.sql.utils;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.SQLDialect;
import org.springframework.core.env.Environment;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;

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

    DbEndpointConfig config = new DbEndpointConfig();

    /**
     * Internal constructor for a new endpoint instance.
     *
     * <p>Client code should use {@link SQLDatabaseConfig2#primaryDbEndpoint(SQLDialect)} or
     * {@link SQLDatabaseConfig2#secondaryDbEndpoint(String, SQLDialect)} to declare endpoints.</p>
     *
     * @param tag     tag for secondary endpoint, or null for primary
     * @param dialect server type, identified by {@link SQLDialect}
     */
    DbEndpointBuilder(String tag, SQLDialect dialect) {
        config.setTag(tag);
        config.setDialect(dialect);
    }

    /**
     * Complete the build of this endpoint.
     *
     * <p>THe endpoint will not be ready to use until after completion of Spring application
     * contxt construction, so a {@link Supplier} is returned, which can be used to obtain the
     * endpoint at a later time. The supplier will block if it is used before the endpoint is
     * prepared.</p>
     *
     * @return a {@link Supplier} that can be used to obtain the fully initialized endpoint
     */
    public Supplier<DbEndpoint> build() {
        final Future<DbEndpoint> future = DbEndpoint.register(config);
        return () -> {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException("DbEndpoint was not ready when first requested", e);
            }
        };
    }

    /**
     * Specify a dbHost property value for this endpoint.
     *
     * @param dbHost property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbHost(@Nonnull String dbHost) {
        config.setDbHost(Objects.requireNonNull(dbHost));
        return this;
    }

    /**
     * Specify a dbPort property value for this endpoint.
     *
     * @param dbPort property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbPort(int dbPort) {
        config.setDbPort(dbPort);
        return this;
    }

    /**
     * Specify a dbDatabaseName property value for this endpoint.
     *
     * @param dbDatabaseName property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbDatabaseName(@Nonnull String dbDatabaseName) {
        config.setDbDatabaseName(Objects.requireNonNull(dbDatabaseName));
        return this;
    }

    /**
     * Specify a dbSchemaName property value for this endpoint.
     *
     * @param dbSchemaName property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbSchemaName(@Nonnull String dbSchemaName) {
        config.setDbSchemaName(Objects.requireNonNull(dbSchemaName));
        return this;
    }

    /**
     * Specify a dbUserName property value for this endpoint.
     *
     * @param dbUserName property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbUserName(@Nonnull String dbUserName) {
        config.setDbUserName(Objects.requireNonNull(dbUserName));
        return this;
    }

    /**
     * Specify a dbPassword property value for this endpoint.
     *
     * @param dbPassword property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbPassword(@Nonnull String dbPassword) {
        config.setDbPassword(Objects.requireNonNull(dbPassword));
        return this;
    }

    /**
     * Specify a dbAccess property value for this endpoint.
     *
     * @param dbAccess property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbAccess(@Nonnull DbEndpointAccess dbAccess) {
        config.setDbAccess(Objects.requireNonNull(dbAccess));
        return this;
    }

    /**
     * Specify a dbRootUserName property value for this endpoint.
     *
     * @param dbRootUserName property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbRootUserName(@Nonnull String dbRootUserName) {
        config.setDbRootUserName(Objects.requireNonNull(dbRootUserName));
        return this;
    }

    /**
     * Specify a dbRootPassword property value for this endpoint.
     *
     * @param dbRootPassword property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbRootPassword(@Nonnull String dbRootPassword) {
        config.setDbRootPassword(Objects.requireNonNull(dbRootPassword));
        return this;
    }

    /**
     * Specify a dbDriverProperties property value for this endpoint.
     *
     * @param dbDriverProperties property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbDriverProperties(@Nonnull Map<String, String> dbDriverProperties) {
        config.setDbDriverProperties(ImmutableMap.copyOf(Objects.requireNonNull(dbDriverProperties)));
        return this;
    }

    /**
     * Specify a dbSecure property value for this endpoint.
     *
     * @param dbSecure property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbSecure(boolean dbSecure) {
        config.setDbSecure(dbSecure);
        return this;
    }

    /**
     * Specify a dbMigrationLocations property value for this endpoint.
     *
     * @param dbMigrationLocations property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbMigrationLocations(String... dbMigrationLocations) {
        config.setDbMigrationLocations(String.join(",", dbMigrationLocations));
        return this;
    }

    /**
     * Suppress migrations for this endpoint.
     *
     * @return this endpoint
     */
    public DbEndpointBuilder withNoDbMigrations() {
        config.setDbMigrationLocations("");
        return this;
    }

    /**
     * Specify a dbFlywayCallbacks property value for this endpoint.
     *
     * @param dbFlywayCallbacks property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbFlywayCallbacks(FlywayCallback... dbFlywayCallbacks) {
        config.setDbFlywayCallbacks(dbFlywayCallbacks);
        return this;
    }

    /**
     * Specify a dbDestructiveProvisioningEnabled property value for this endpoint.
     *
     * <p>This will permit operations like dropping a user or a database to be performed using
     * root credentials, during endpoint initialization, in order to attempt to fix a situation
     * where the non-root user credentials are found not to work.</p>
     *
     * @param dbDestructiveProvisioningEnabled property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbDestructiveProvisioningEnabled(boolean dbDestructiveProvisioningEnabled) {
        config.setDbDestructiveProvisioningEnabled(dbDestructiveProvisioningEnabled);
        return this;
    }

    /**
     * Specify a dbEndpointEnabled property value for this endpoint.
     *
     * <p>Setting this to false will prevent initialization of the endpoint. It may be useful in
     * some debugging and testing scenarios, allowing the effective removal of an endpoint without
     * actually removing it from the code.
     *
     * @param dbEndpointEnabled property value
     * @return this endpoint
     */
    public DbEndpointBuilder withDbEndpointEnabled(boolean dbEndpointEnabled) {
        config.setDbEndpointEnabled(dbEndpointEnabled);
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
    public DbEndpointBuilder like(Supplier<DbEndpoint> templateSupplier) {
        config.setTemplateSupplier(templateSupplier);
        return this;
    }
}
