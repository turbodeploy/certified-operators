package com.vmturbo.sql.utils;

import static com.vmturbo.sql.utils.DbEndpointResolver.COMPONENT_TYPE_PROPERTY;
import static com.vmturbo.sql.utils.DbEndpointResolver.DESTRUCTIVE_PROVISIONING_ENABLED_PROPERTY;
import static com.vmturbo.sql.utils.DbEndpointResolver.HOST_PROPERTY;
import static com.vmturbo.sql.utils.DbEndpointResolver.PORT_PROPERTY;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.components.common.utils.Strings;
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
 *         for these objects are all "mangled"  so it is safe to run tests against a
 *         database server that is also used operational appliances. The mangling takes the form
 *         of the first 8 characters of the natural identifier, followed by an underscore and
 *         a 7-character suffix comprising lower-case letters and digits. The suffix is computed by
 *         creating a 32-bit cryptographic hash of the full natural name, along with the current
 *         time as a 64-bit millis-since-epoch value. The natural name is part of the input for
 *         this hash, so that if two endpoints collide wrt their 9-character prefixes, they will
 *         nevertheless have different mangled names.</p>
 *
 *         <p>The 16-char overall limit is to conform with early versions of MySQL/MariaDB</p>
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
    private static final String BASE36_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";

    private final Set<DbEndpoint> endpoints = new LinkedHashSet<>();
    private final Map<String, String> propertySettings;
    private final long instantiationTime = System.currentTimeMillis();
    private static final XXHashFactory xxHash = XXHashFactory.safeInstance();

    /**
     * Create a new rule instance to manage the provided endpoints.
     *
     * @param componentName name of component, for object naming defaults (those defaults
     *         are
     *         normally set via the Spring-supplied component_type property)
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

    private void setOverridesForEndpoint(final DbEndpoint endpoint)
            throws UnsupportedDialectException {
        final SQLDialect dialect = endpoint.getConfig().getDialect();
        final String endpointName = endpoint.getConfig().getName();
        // set dbHost property
        String dbHostDefault = getFromSystemProperties(endpointName, HOST_PROPERTY, dialect);
        logger.info("Host: {}", dbHostDefault);
        dbHostDefault = dbHostDefault != null ? dbHostDefault : "localhost";
        propertySettings.putIfAbsent(getPropertyName(endpointName, HOST_PROPERTY), dbHostDefault);
        // set dbPort property
        String dbPortDefault = getFromSystemProperties(endpointName, PORT_PROPERTY, dialect);
        dbPortDefault = dbPortDefault != null ? dbPortDefault
                : Integer.toString(DbEndpointResolver.getDefaultPort(dialect));
        propertySettings.putIfAbsent(getPropertyName(endpointName, PORT_PROPERTY), dbPortDefault);
        // we should always allow destructive provisioning operations in a test database
        propertySettings.putIfAbsent(
                getPropertyName(endpointName, DESTRUCTIVE_PROVISIONING_ENABLED_PROPERTY), "true");
    }

    private void before() {
        endpoints.clear();
    }

    private void afterClass() throws Throwable {
        logger.info("Finished tests, dropping temporary databases");
        for (final DbEndpoint endpoint : endpoints) {
            if (endpoint.isReady()) {
                logger.info("Dropping database & user for {}", endpoint);
                endpoint.getAdapter().tearDown();
            }
        }
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
        for (final DbEndpoint endpoint : endpoints) {
            DbEndpointCompleter endpointCompleter = endpoint.getEndpointCompleter();
            setOverridesForEndpoint(endpoint);
            endpointCompleter.setResolver(propertySettings::get, mockPasswordUtil());
            endpoint.getConfig().setIdentifierMangler(this::mangleIdentifier);
            endpointCompleter.completeEndpoint(endpoint);

            if (!endpoint.isReady()) {
                try {
                    logger.info("Completing endpoint {}", endpoint);
                    endpoint.getEndpointCompleter().completePendingEndpoint(endpoint);
                } catch (Exception e) {
                    logger.warn("Endpoint {} initialization failed; entering retry loop", endpoint, e);
                }
                endpoint.awaitCompletion(30L, TimeUnit.SECONDS);
            } else {
                if (endpoint.getConfig().getAccess().isWriteAccess()) {
                    endpoint.getAdapter().truncateAllTables();
                }
            }
            this.endpoints.add(endpoint);
        }
    }

    private String mangleIdentifier(String original) {
        XXHash32 hash32 = xxHash.hash32();
        byte[] origBytes = original.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bytes = ByteBuffer.allocate(origBytes.length + Long.BYTES + Long.BYTES);
        bytes.put(origBytes);
        bytes.putLong(instantiationTime);
        bytes.putLong(Thread.currentThread().getId());
        bytes.position(0);
        int hash = hash32.hash(bytes, 0);
        return Strings.truncate(original, 8) + "_" + base36(hash);
    }

    private String base36(int i) {
        StringBuilder sb = new StringBuilder();
        // ensure we're working with a non-negative value
        long v = (long)i & 0xFFFFFFFFL;
        while (v != 0) {
            int next = (int)(v % 36);
            v = (v - next) / 36;
            sb.append(BASE36_CHARS.charAt(next));
        }
        return sb.toString();
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

    private DBPasswordUtil mockPasswordUtil() {
        DBPasswordUtil dbPasswordUtil = mock(DBPasswordUtil.class);
        when(dbPasswordUtil.getSqlDbRootUsername(any())).thenAnswer(invocation -> DBPasswordUtil.obtainDefaultRootDbUser(invocation.getArgumentAt(0, String.class)));
        when(dbPasswordUtil.getSqlDbRootPassword()).thenReturn(DBPasswordUtil.obtainDefaultPW());
        return dbPasswordUtil;
    }
}
