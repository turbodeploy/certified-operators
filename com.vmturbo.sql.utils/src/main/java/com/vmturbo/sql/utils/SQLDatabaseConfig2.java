package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Configuration for interaction with database.
 *
 * <p>Components that want to connect to the database should import this configuration into their
 * Spring context with an @Import annotation (please do not @ComponentScan the sql.utils
 * package!).</p>
 *
 * <p>The component can then create one or more database endpoints by using the {@link
 * #primaryDbEndpoint(SQLDialect)} and {@link #secondaryDbEndpoint(String, SQLDialect)} static
 * methods. Most components will only need to define a main instance, but components that need
 * multiple DB endpoints will need to create tagged instances for all or all but one of them. Each
 * endpoint should be configured as a bean.</p>
 *
 * <p>It is important that the endpoints not be used before they are initialized, which normally
 * should happen in the component's onComponentStart() method. Importantly, this means that where
 * one might normally want to configure some other bean using a dataSource or (jooQ) DSL context
 * providing access to the endpoint, this will likely fail. Instead, the component should be given
 * the {@link DbEndpoint} bean instance, from which it can later obtain whatever sort of access
 * mechanism it desires.</p>
 *
 * <p>The reason for this requirement is the manner in which endpoints obtain their configuration
 * values. Values are obtained from the application context's final {@link Environment}, using
 * property names that include tag names for tagged instances. In addition, all the configuration
 * classes are scanned for fields with {@link Value} annotations, and those annotations are used to
 * create an additional layer atop the environment, for property resolution. For example, imagine
 * the following code:</p>
 *
 * <pre>
 *     {@literal @}Value("${dbPort:1234}") int dbPort;
 * </pre>
 *
 * <p>If the Spring-built environment does not define a value for the dbPort property, the value
 * "1234" will nevertheless be available in the added property layer, and the DB endpoint will be
 * configured accordingly. This essentially makes it possible to declare your own in-code defaults
 * for specific properties while still making it possible for those defaults to be overridden via
 * registered property sources.</p>
 *
 * <p>N.B. For this to work properly, the value-annotated field must be named with the intended
 * property name. The value-annotated field is not actually accessed; rather, its value template is
 * resolved using the spring environment.</p>
 *
 * <p>Some relevant endpoint properties are defaulted based on the {@link SQLDialect} configured
 * for each endpoint, and they should not normally be independently configured. However, if there is
 * a need to override the defaults, normal configuration mechanisms may be used. See {@link
 * DbEndpoint} for details on the properties relevant ot DB endpoints, including defaults.</p>
 */
@Configuration
public class SQLDatabaseConfig2 {
    // TODO Add debug logging
    private static final Logger logger = LogManager.getLogger();

    @Autowired
    private ApplicationContext applicationContext;

    @Value("${authHost}")
    protected String authHost;

    @Value("${authRoute:}")
    protected String authRoute;

    @Value("${serverHttpPort}")
    protected int authPort;

    @Value("${authRetryDelaySecs}")
    protected int authRetryDelaySecs;

    private final AtomicReference<UnaryOperator<String>> resolver = new AtomicReference<>();

    @Bean
    DBPasswordUtil dbPasswordUtil() {
        return new DBPasswordUtil(authHost, authPort, authRoute, authRetryDelaySecs);
    }

    private final Map<String, DbEndpoint> instanceByTag = new HashMap<>();

    /**
     * Create a new {@link DbEndpoint primary database endpoint}, without a tag.
     *
     * <p>Properties for this endpoint should be configured with out tag prefixes. A component
     * may compare at most one primary endpoint.</p>
     *
     * @param dialect the SQL dialect (i.e. DB server type - MySQL, Postgres, etc.) for this
     *                endpoint
     * @return an endpoint that can provide access to the database
     * @throws UnsupportedDialectException if the requested dialect is not supported
     */
    public DbEndpointBuilder primaryDbEndpoint(SQLDialect dialect) {
        return secondaryDbEndpoint(null, dialect);
    }

    /**
     * Create a new {@link DbEndpoint secondary database endpoint} with a given tag.
     *
     * <p>Properties for this endpoint should be configured with property names that include the
     * given tag as a prefix, e.g. "xxx_dbPort" to configure the port number for an endpoint with
     * "xxx" as the tag.</p>
     *
     * <p>A component may define any number of secondary endpoints (all with distinct tags), in
     * addition to a single primary endpoint, if the latter is required.</p>
     *
     * @param tag     the tag for this endpoint
     * @param dialect the SQL dialect (i.e. DB server type - MySQL, Postgres, etc.) for this
     *                endpoint
     * @return an endpoint that can provide access to the database
     * @throws UnsupportedDialectException if the requested dialect is not supported
     */
    public DbEndpointBuilder secondaryDbEndpoint(String tag, SQLDialect dialect) {
        return new DbEndpointBuilder(tag, dialect);
    }

    /**
     * Initialize all endpoints.
     *
     * <p>This method constructs the property resolver first, and then loops over the defined
     * endpoints and initializes each one in turn.</p>
     *
     * @throws UnsupportedDialectException if any endpoint uses an unsupported SQL dialect
     * @throws SQLException                if any endpoint fails initialization due to SQL execution
     *                                     failure
     * @throws InterruptedException        if interrupted
     */
    public void initAll() throws UnsupportedDialectException, SQLException, InterruptedException {
        final Environment env = applicationContext.getEnvironment();
        DbEndpoint.setResolver(env::getProperty, dbPasswordUtil());
    }

}
