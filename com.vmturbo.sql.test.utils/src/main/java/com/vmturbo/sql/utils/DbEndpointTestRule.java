package com.vmturbo.sql.utils;

import static com.vmturbo.sql.utils.DbEndpointResolver.COMPONENT_TYPE_PROPERTY;
import static com.vmturbo.sql.utils.DbEndpointResolver.HOST_PROPERTY;
import static com.vmturbo.sql.utils.DbEndpointResolver.PORT_PROPERTY;
import static com.vmturbo.sql.utils.DbEndpointResolver.USE_CONNECTION_POOL;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.jooq.SQLDialect;
import org.jooq.Schema;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.SchemaCleaner.Monitor;

/**
 * Rule to manage database endpoints during tests.
 *
 * <p>This rule completes supplied endpoints, yielding an instance of {@link TestDbEndpoint} for
 * each. During completion, any provisioning required for the endpoint will be performed, using
 * "mangled" names for provisioned databases, schemas, and users.</p>
 *
 * <p>At the completion of each test, schemas associated with the provided endpoints are returned
 * to initial state by truncating tables to which jOOQ INSERT operations were applied during the
 * test. Reference tables (tables that are non-empty after initial provisioning+migration) are
 * exempt from this cleaning operation. Also, see {@link CleanupOverrides} to see how you can
 * annotate test classes and methods to alter the tables affected by cleanup.</p>
 *
 * <p>The {@link TestDbEndpoint} created for a given endpoint survives across multiple test
 * classes, and will be re-used - without repeating provisioning - for any subsequent use of the
 * same endpoint in later test classes.  This dramatically improves performance of builds with
 * many live-DB test classes, as each needed schema is built just once. Provisioned objects are
 * ultimately dropped via a JVM shutdown hook.</p>
 *
 * <p>A previously provisioned endpoint is reused whenever a later test class requests completion
 * of an endpoint with the same name. There is no check that the endpoints actually align in
 * other ways. Using two endpoints with different configurations within the lifetime of the
 * running JVM (typically, all the tests in a given module) will yield unpredictable results.
 * This is not a scenario that is likely to arise in any case, since it would yield very confusing
 * code.</p>
 *
 * <p>Also, when it comes to name mangling, the rule is that if a database name, schema name
 * or user name appearing in a resolved endpoint is not equal to a previously mangled name, then
 * it is mangled and added to the list. This means that two endpoints intended ot use the same
 * schema, for instance, will do so. Again, it is possible to create scenarios where this will
 * misfire, but such scenarios would be confusing to begin with.</p>
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
    private static final Set<TestDbEndpoint> testDbEndpoints = new HashSet<>();
    private final Map<String, String> propertySettings;
    private SchemaCleaner schemaCleaner;

    /**
     * Create a new rule instance to manage the provided endpoints.
     *
     * <p>The `componentName` value will be used in various ways for endpoint resolution; most
     * importantly, it will be used in constructing a default value for `migrationLocations`.
     * Therefore, it probably needs to be what it will be in production. Since that value is
     * generally free-form, it will have non-alphanumeric characters removed if it is ever used for
     * database, schema, or user names - where such characters could cause problems.</p>
     *
     * @param componentName name of component, for object naming defaults
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
    }

    /**
     * Complete an endpoint using the test rule.
     *
     * <p>The {@link TestDbEndpoint} instance returned by this method may be shared with other
     * endpoints that have been completed (in this test class or another) that had identical
     * configuration, to avoid unnecessary rebuilding of schemas.</p>
     *
     * @param endpoint endpoint to be completed
     * @param schema   schema associated with the endpoint
     * @return TestDbEndpoint instance for completed endpoint
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interrupted
     * @throws SQLException                if there's a provisioning problem
     */
    public TestDbEndpoint completeEndpoint(DbEndpoint endpoint, Schema schema)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        setOverridesForEndpoint(endpoint);
        TestDbEndpoint testEndpoint
                = new TestDbEndpoint(endpoint, propertySettings, schema);
        testDbEndpoints.add(testEndpoint);
        return testEndpoint;
    }

    private void setOverridesForEndpoint(final DbEndpoint endpoint)
            throws UnsupportedDialectException {
        final SQLDialect dialect = endpoint.getConfig().getDialect();
        final String endpointName = endpoint.getConfig().getName();
        // set dbHost property
        String dbHostDefault = getFromSystemProperties(endpointName, HOST_PROPERTY, dialect);
        dbHostDefault = dbHostDefault != null ? dbHostDefault : "localhost";
        propertySettings.put(getPropertyName(endpointName, HOST_PROPERTY), dbHostDefault);
        // set dbPort property
        String dbPortDefault = getFromSystemProperties(endpointName, PORT_PROPERTY, dialect);
        dbPortDefault = dbPortDefault != null ? dbPortDefault
                                              : Integer.toString(
                                                      DbEndpointResolver.getDefaultPort(dialect));
        propertySettings.put(getPropertyName(endpointName, PORT_PROPERTY), dbPortDefault);
        propertySettings.put(getPropertyName(endpointName, USE_CONNECTION_POOL), "false");
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        if (description.isTest()) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    List<SchemaCleaner.Monitor> monitors = new ArrayList<>();
                    try {
                        // we may have many instances of TestDbEndpoint representing the same
                        // endpoint (one for each test class that used the endpoint). But we
                        // only need one cleanup monitor for each. So prune that list.
                        getMonitors(description).forEach(monitors::add);
                        base.evaluate();
                    } finally {
                        monitors.forEach(SchemaCleaner.Monitor::close);
                    }
                }
            };
        } else {
            return base;
        }
    }

    /**
     * Create insertion monitors for the current collection of test endpoints.
     *
     * <p>There may be many test endpoints that share a given actual endpoint, so we whittle down
     * the list to have just one representative of reach group. We then create monitors for those
     * endpoints.</p>
     *
     * @param description test description info provided to the rule by the JUnit
     * @return monitors that will monitor insertions during the test and then perform cleanups when
     *         closed
     */
    @NotNull
    private Stream<Monitor> getMonitors(Description description) {
        return testDbEndpoints.stream()
                .filter(te -> te.getDbEndpoint().getConfig().getAccess()
                        == DbEndpointAccess.ALL)
                .collect(Collectors.groupingBy(TestDbEndpoint::getDbEndpoint))
                .values().stream()
                .map(list -> list.get(0))
                .map(te -> te.getSchemaCleaner().monitor(description));
    }

    private static String getFromSystemProperties(String endpointName, String propertyName,
            SQLDialect dialect)
            throws UnsupportedDialectException {
        List<String> names = new ArrayList<>();
        names.add(endpointName);
        names.addAll(DbEndpointResolver.dialectPropertyPrefixes(dialect).stream()
                .filter(Objects::nonNull).collect(Collectors.toList()));
        for (final String name : names) {
            String prefix = name;
            while (true) {
                String value = System.getProperty(getPropertyName(prefix, propertyName));
                if (value != null) {
                    return value;
                }
                if (prefix.contains(".")) {
                    prefix = prefix.substring(0, prefix.lastIndexOf('.'));
                } else {
                    break;
                }
            }
        }
        return null;
    }

    private static String getPropertyName(String endpointName, String propertyName) {
        return endpointName + "." + propertyName;
    }
}
