package com.vmturbo.sql.utils;

import static com.vmturbo.sql.utils.DbEndpointResolver.COMPONENT_TYPE_PROPERTY;
import static com.vmturbo.sql.utils.DbEndpointResolver.DB_HOST_PROPERTY;
import static com.vmturbo.sql.utils.DbEndpointResolver.DB_NAME_SUFFIX_PROPERTY;
import static com.vmturbo.sql.utils.DbEndpointResolver.DB_PORT_PROPERTY;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Rule to manage database endpoints during tests.
 *
 * <p>This rule can act both as a a per-test {@link Rule @Rule} and as a class-level
 * {@link ClassRule @ClassRule}, and a single rule instance should normally carry both
 * annotations.</p>
 *
 * <p>As a @{@link Rule}, this rule:</p>
 *
 * <ul>
 *     <li>
 *
 *         <p>Initializes each endpoint identified for the rule instance. This will effectively act on
 *         a per-class basis, since subsequent initializations performed for other tests will be
 *         no-ops.</p>
 *
 *         <p>When provisioning database, schemas, and users for an endpoint, the configured names
 *         for these objects are all suffixed with a tag, so it is safe to run tests against a
 *         database server that is also used operational appliances. The same suffix will be used
 *         all objects provisioned during a given test class execution, so "like" endpoints will
 *         use same database and schema objects as their template endpoints, as intended.</p>
 *     </li>
 *     <li>
 *         Truncates all tables appearing in the databases configured for all the provided endpoints.
 *         This rolls back any database changes made by the prior test.
 *     </li>
 * </ul>
 *
 * <p>As a {@link ClassRule}, this rule drops all test databases/schemas and users
 * created during endpoint initialization.</p>
 *
 * <p>This class makes no use of Spring services, while {@link DbEndpoint} instances are generally
 * defined as Spring beans. The intended approach is to initialize endpoints for tests by directly
 * calling bean methods defined in production code. This ensures that tested endpoints are
 * configured as they will be in production (with the exception of name suffixes for temporary
 * objects, and differing property resolution approaches). One concern is Spring's default singleton
 * enforcement for beans is lost. This can lead to multiple instantiations of the same endpoint
 * when that endpoint and one or more others that are "like" it all participate in the test. While
 * this is true, it is, in fact, harmless; the multiple instances will all be configured identically,
 * and provisioning for all but the first will be no-ops.</p>
 *
 * <p>To minimize the risk of unintended side-effects, it is recommended that {@link DbEndpoint}
 * bean definitions appear in config classes that only contain those definitions, without additional
 * beans.</p>
 *
 * <p>Because Spring configuration context will not be available to provide endpoint property values,
 * the rule constructor includes a property settings map that can be used to provide any property
 * settings that should be used in configuring the endpoints. Note that these will be used in
 * preference to existing values supplied by the endpoint constructor, as is the case in non-test
 * scenarios as well.</p>
 */
public class DbEndpointTestRule implements TestRule {
    private static final Logger logger = LogManager.getLogger();

    private final Set<DbEndpoint> endpoints = new LinkedHashSet<>();
    private final Map<String, String> propertySettings;
    private final String provisioningSuffix;

    /**
     * Create a new rule instance to manage the provided endpoints.
     *
     * @param componentName name of component, for object naming defaults (those defaults are
     *                      normally set via the Spring-supplied component_type property)
     */
    public DbEndpointTestRule(String componentName) {
        this(componentName, Collections.emptyMap());
    }

    /**
     * Create a new rule instance to manage the provided endpoints.
     *
     * @param componentName    name of component, for object naming defaults (those defaults are
     *                         normally set via the Spring-supplied component_type property)
     * @param propertySettings property settings to be used as if provided by external
     *                         configuration
     */
    public DbEndpointTestRule(String componentName, Map<String, String> propertySettings) {
        this.propertySettings = new HashMap<>(propertySettings);
        this.propertySettings.put(COMPONENT_TYPE_PROPERTY, componentName);
        this.provisioningSuffix = "_" + System.currentTimeMillis();
    }

    private void setOverridesForEndpoint(final DbEndpoint endpoint, final String provisioningSuffix)
            throws UnsupportedDialectException {
        final String tag = endpoint.getConfig().getTag();
        final SQLDialect dialect = endpoint.getConfig().getDialect();
        // set dbHost property
        String dbHostDefault = System.getProperty(taggedPropertyName(tag, DB_HOST_PROPERTY));
        dbHostDefault = dbHostDefault != null ? dbHostDefault : System.getProperty(DB_HOST_PROPERTY);
        dbHostDefault = dbHostDefault != null ? dbHostDefault : "localhost";
        propertySettings.putIfAbsent(taggedPropertyName(tag, DB_HOST_PROPERTY), dbHostDefault);
        // set dbPort property
        String dbPortDefault = System.getProperty(taggedPropertyName(tag, DB_PORT_PROPERTY));
        dbPortDefault = dbPortDefault != null ? dbPortDefault : System.getProperty(DB_PORT_PROPERTY);
        dbPortDefault = dbPortDefault != null ? dbPortDefault
                : Integer.toString(DbEndpointResolver.getDefaultPort(dialect));
        propertySettings.putIfAbsent(taggedPropertyName(tag, DB_PORT_PROPERTY), dbPortDefault);
        // set suffix for provisioned object names
        propertySettings.putIfAbsent(taggedPropertyName(tag, DB_NAME_SUFFIX_PROPERTY), provisioningSuffix);
    }

    private void before() {
        endpoints.clear();
    }

    private void afterClass() throws Throwable {
        logger.info("Finished tests, dropping temporary databases");
        for (final DbEndpoint endpoint : endpoints) {
            if (endpoint.isReady()) {
                logger.info("Dropping database & user for {}", endpoint);
                endpoint.getAdapter().dropDatabase();
                endpoint.getAdapter().dropUser();
                endpoint.getAdapter().dropReadersGroupUser();
            }
        }
        // don't allow this rule's configurations to leak into other test classes involving
        // the same endpoints
        DbEndpoint.resetAll();
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        if (description.isTest()) {
            return new Statement() {
                public void evaluate() throws Throwable {
                    before();
                    base.evaluate();
                }
            };
        }
        if (description.isSuite()) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    try {
                        base.evaluate();
                    } finally {
                        afterClass();
                    }
                }
            };
        }
        return base;
    }

    /**
     * Provide endpoints to be managed by this rule instance.
     *
     * <p>This is normally called from within a @Before method in a test class, since it often
     * difficult to obtain endpoint instances statically. However, when that's possible, this can
     * instead be done in a @BeforeClass method.</p>
     *
     * @param endpoints endpoints to be managed by this rule
     * @throws InterruptedException        if we're interrupted
     * @throws UnsupportedDialectException if an endpoint is defined with an unsupported dialect
     * @throws SQLException                if there's a problem with provisioning
     */
    public void addEndpoints(final DbEndpoint... endpoints) throws InterruptedException, UnsupportedDialectException, SQLException {
        for (DbEndpoint endpoint : endpoints) {
            if (!endpoint.isReady()) {
                try {
                    setOverridesForEndpoint(endpoint, provisioningSuffix);
                } catch (UnsupportedDialectException e) {
                    logger.error("Failed to set properties for endpoint {}; testing is likely to fail",
                            endpoint, e);
                }
            }
        }
        DbEndpointCompleter.get().setResolver(propertySettings::get, mockPasswordUtil(), false);
        for (final DbEndpoint endpoint : endpoints) {
            if (!endpoint.isReady()) {
                try {
                    logger.info("Completing endpoint {}", endpoint);
                    DbEndpointCompleter.get().completePendingEndpoint(endpoint);
                } catch (Exception e) {
                    logger.warn("Endpoint {} initialization failed; entering retry loop", endpoint, e);
                }
                endpoint.awaitCompletion(30, TimeUnit.SECONDS);
            } else {
                if (endpoint.getConfig().getDbAccess().isWriteAccess()) {
                    endpoint.getAdapter().truncateAllTables();
                }
            }
            this.endpoints.add(endpoint);
        }
    }

    private static String taggedPropertyName(String tag, String propertyName) {
        return Strings.isNullOrEmpty(tag) ? propertyName
                : tag + DbEndpointResolver.TAG_PREFIX_SEPARATOR + propertyName;
    }

    private DBPasswordUtil mockPasswordUtil() {
        DBPasswordUtil dbPasswordUtil = mock(DBPasswordUtil.class);
        when(dbPasswordUtil.getSqlDbRootUsername(any())).thenAnswer(invocation -> DBPasswordUtil.obtainDefaultRootDbUser(invocation.getArgumentAt(0, String.class)));
        when(dbPasswordUtil.getSqlDbRootPassword()).thenReturn(DBPasswordUtil.obtainDefaultPW());
        return dbPasswordUtil;
    }

}
