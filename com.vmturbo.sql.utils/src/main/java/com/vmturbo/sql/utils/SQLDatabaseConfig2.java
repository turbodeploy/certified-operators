package com.vmturbo.sql.utils;

import javax.annotation.Nonnull;

import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;

/**
 * Common configuration that should be imported by classes that want to create {@link DbEndpoint}s.
 *
 * <p/>This is intended to (eventually) replace {@link SQLDatabaseConfig}.
 */
@Configuration
public class SQLDatabaseConfig2 {

    @Value("${authHost:auth}")
    private String authHost;

    @Value("${authRoute:}")
    private String authRoute;

    @Value("${serverHttpPort:8080}")
    private int authPort;

    @Value("${authRetryDelaySecs:10}")
    private int authRetryDelaySec;

    /**
     * The Spring environment, injected by the framework.
     */
    @Autowired
    private Environment environment;

    /**
     * The {@link DbEndpointCompleter} that acts as a factory for {@link DbEndpoint}s.
     *
     * @return The {@link DbEndpointCompleter}.
     */
    @Bean
    @Lazy
    public DbEndpointCompleter endpointCompleter() {
        return new DbEndpointCompleter(environment::getProperty, endpointPasswordUtil());
    }

    /**
     * The {@link DBPasswordUtil} used to retrieve passwords from the auth component.
     *
     * @return A {@link DBPasswordUtil}.
     */
    @Bean
    @Lazy
    public DBPasswordUtil endpointPasswordUtil() {
        return new DBPasswordUtil(authHost, authPort, authRoute, authRetryDelaySec);
    }

    /**
     * Create a new {@link DbEndpoint primary database endpoint}, without a tag.
     *
     * <p>Properties for this endpoint should be configured with out tag prefixes. A component
     * may compare at most one primary endpoint.</p>
     *
     * @param dialect the SQL dialect (i.e. DB server type - MySQL, Postgres, etc.) for this
     *                endpoint
     * @return an endpoint that can provide access to the database
     */
    @Nonnull
    public DbEndpointBuilder primaryDbEndpoint(SQLDialect dialect) {
        return secondaryDbEndpoint("", dialect);
    }

    /**
     * Create a new {@link DbEndpoint secondary database endpoint} with a given tag.
     *
     * <p>Properties for this endpoint should be configured with property names that include the
     * given tag as a prefix, e.g. "xxx_dbPort" to configure the port number for an endpoint with
     * "xxx" as the tag.</p>,
     *
     * <p>A component may define any number of secondary endpoints (all with distinct tags), in
     * addition to a single primary endpoint, if the latter is required.</p>
     *
     * @param tag     the tag for this endpoint
     * @param dialect the SQL dialect (i.e. DB server type - MySQL, Postgres, etc.) for this
     *                endpoint
     * @return an endpoint that can provide access to the database
     */
    @Nonnull
    public DbEndpointBuilder secondaryDbEndpoint(String tag, SQLDialect dialect) {
        return endpointCompleter().newEndpointBuilder(tag, dialect);
    }

    /**
     * Create a new abstract endpoint.
     *
     * <p>An abstract DB endpoint will never be operationalized. It is a means of creating a
     * template that can be shared across components that need to access the same database objects.
     * Each such component should create its own endpoint(s) derived from the shared abstract
     * endpoint.</p>
     *
     * <p>Typically, an abstract endpoint will specify database name, and schema name, but
     * any of the properties can be specified in the builder. Any such property values will be
     * used instead of the normal built-in defaults in derived components.</p>
     *
     * <p>Abstract components are not affected by external configuration, but derived endpoints
     * can have their properties set or changed through external configuration, including
     * properties that are set in the abstract endpoint.</p>
     *
     * <p>An abstract endpoint also has no dialect, since it is neither resolved (hence no need to
     * access dialect-dependent property defaults), nor operational (so no need for an adapter).</p>
     *
     * @return a new abstract endpoint
     */
    @Nonnull
    public DbEndpointBuilder abstractDbEndpoint() {
        return new DbEndpointBuilder("", null, endpointCompleter()).setAbstract();
    }

}
