package com.vmturbo.sql.utils;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;

/**
 * Common configuration that should be subclassed by config classes that want to create {@link DbEndpoint}s.
 *
 * <p>This is intended to (eventually) replace {@link SQLDatabaseConfig}.</p>
 */
@Primary
@Configuration
public class DbEndpointsConfig {
    private static final Logger logger = LogManager.getLogger();

    @Value("${authHost:auth}")
    private String authHost;

    @Value("${authRoute:}")
    private String authRoute;

    @Value("${serverHttpPort:8080}")
    private int authPort;

    @Value("${authRetryDelaySecs:10}")
    private int authRetryDelaySec;

    @Value("${dbEndpointMaxAwaitCompletion:30m}")
    private String dbEndpointMaxAwaitCompletion;

    @Value("${sqlDialect}")
    protected SQLDialect sqlDialect;

    /**
     * The Spring environment, injected by the framework.
     */
    @Autowired
    private Environment springEnvironment;

    /**
     * The {@link DbEndpointCompleter} that acts as a factory for {@link DbEndpoint}s.
     *
     * @return The {@link DbEndpointCompleter}.
     */
    @Bean
    @Lazy
    public DbEndpointCompleter endpointCompleter() {
        return new DbEndpointCompleter(springEnvironment::getProperty, endpointPasswordUtil(),
                dbEndpointMaxAwaitCompletion);
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
     * Define a new {@link DbEndpoint}.
     *
     * <p>If there are any dots in the name, this endpoint will inherit endpoint properties that
     * appear with any prefix up to a dot boundary.</p>
     *
     * @param name    name for this endpoint
     * @param dialect the SQL dialect (i.e. DB server type - MySQL, Postgres, etc.) for this
     *                endpoint
     * @return a new endpoint builder
     */
    @Nonnull
    public DbEndpointBuilder dbEndpoint(String name, SQLDialect dialect) {
        return endpointCompleter().newEndpointBuilder(name, dialect);
    }

    /**
     * Create a new derived endpoint.
     *
     * <p>The derived endpoint will use values from the given base as defaults in lieu of built-in
     * defaults, but otherwise resolves like other endpoints, making use of externally configured
     * property values. Importantly, the dialect is obtained from the base.</p>
     *
     * @param name         name of this endpoint
     * @param baseEndpoint endpoint that will serve as a source of default property values
     * @return new endpoint builder
     */
    public DbEndpointBuilder derivedDbEndpoint(String name, DbEndpoint baseEndpoint) {
        return endpointCompleter().newEndpointBuilder(name, baseEndpoint.getConfig().getDialect())
                .like(baseEndpoint);
    }

    /**
     * Create a new abstract endpoint.
     *
     * <p>An abstract DB endpoint will never be made operational. It is a means of creating a
     * template that can be shared across components that need to access the same database objects.
     * Each such component should create its own endpoint(s) derived from the shared abstract
     * endpoint.</p>
     *
     * <p>Typically, an abstract endpoint will specify database name, and schema name, but
     * any of the properties can be specified in the builder. Any such property values will be used
     * instead of the normal built-in defaults in derived components.</p>
     *
     * <p>An abstract endpoint will be resolved just like any other endpoint, but to prevent
     * potentially conflicting definitions across components, they should only be configured in
     * "global" property blocks that will be have identical component values among accessing
     * components.</p>
     *
     * <p>An abstract endpoint will never be "completed" - i.e. turned operationally. It will only
     * serve as a container of property defaults for derived endpoints.</p>
     *
     * @param name    name for this endpoint
     * @param dialect the SQL dialect (i.e. DB server type - MySQL, Postgres, etc.) for this
     *                endpoint
     * @return a new abstract endpoint builder
     */
    @Nonnull
    public DbEndpointBuilder abstractDbEndpoint(String name, SQLDialect dialect) {
        return endpointCompleter().newEndpointBuilder(name, dialect).setAbstract();
    }
}
