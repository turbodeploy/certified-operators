package com.vmturbo.sql.utils;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Longs;

import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Schema;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.impl.DSL;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * This class manages endpoints required by tests, and called for by {@link DbEndpointTestRule}
 * instances activated in test classes.
 *
 * <p>Each test endpoint is identified by its endpoint name. Each endpoint is completed into a
 * functioning endpoint the first time it is presented for use in a test, and then reused after
 * that. Endpoint instances thus constructed are retained in a static map, so the same endpoint can
 * be used in multiple test classes until the JVM exits. DB objects provisioned by endpoints
 * (database, schemas, users, etc.) are removed by a shutdown hook registered by this class, so they
 * should not normally persist in the database following tests, so long as the JVM is allowed to
 * terminate gracefully.</p>
 *
 * <p>If two endpoints differ in their configs and yet have identical names for any DB objects,
 * they will map to the same {@link TestDbEndpoint}, which is probably not helpful. Best to avoid
 * using the same name for distinct endpoints.</p>
 *
 * <p>There's a similar caveat for name mangling: When a database, schema, or user name needs to be
 * mangled and that same name has been previously mangled, the previously created mangled name will
 * be re-used. This means that different endpoints that are intended to reference the same db or
 * schema or user will do so in the test environment. It is probably possible to create problems by
 * abusing this scheme, so don't.</p>
 */
public class TestDbEndpoint {

    private static final Logger logger = LogManager.getLogger();

    private static final long startTime = System.currentTimeMillis();
    private static final Map<String, TestDbEndpoint> testEndpoints = new HashMap<>();
    private static final int XXHASH_SEED = 1234567890;
    private static final String BASE36_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static final Map<String, String> knownManglings = new HashMap<>();

    private static final DBPasswordUtil testPasswordUtil = getMockDbPasswordUtil();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(TestDbEndpoint::tearDownEndpoints));
    }

    private static final StreamingXXHash32 xxHash32 = XXHashFactory.safeInstance()
            .newStreamingHash32(XXHASH_SEED);
    private final DbEndpoint endpoint;
    private final Schema schema;
    private final DSLContext dsl;
    private final SchemaCleaner schemaCleaner;

    /**
     * Create a new instance.
     *
     * @param endpoint   endpoint on which the test endpoint is to be based
     * @param properties properties to be used during endpoint resolution
     * @param schema     Schema on which the endpoint will operate
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public TestDbEndpoint(DbEndpoint endpoint, Map<String, String> properties, Schema schema)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        // do we already have a matching instance?
        DbEndpointConfig config = endpoint.getConfig();
        String name = config.getName() + "-" + config.getDialect().name();
        if (testEndpoints.containsKey(name)) {
            // Yes! use it instead of creating a new one
            TestDbEndpoint testDbEndpoint = testEndpoints.get(name);
            // following copies are really just to avoid compiler errors about final fields
            // not being initialized
            this.endpoint = testDbEndpoint.getDbEndpoint();
            this.schema = testDbEndpoint.getSchema();
            this.dsl = testDbEndpoint.dsl;
            this.schemaCleaner = testDbEndpoint.schemaCleaner;
        } else {
            // no luck... create a mangled copy of this confignew DbEndpointResolver(endpoint.getConfig(), properties::get, testPasswordUtil).resolve();
            new DbEndpointResolver(config, properties::get, testPasswordUtil).resolve();
            mangle(config);
            complete(endpoint);
            this.endpoint = endpoint;
            // register the new test endpoint for later resuse
            testEndpoints.put(name, this);
            // now gather some other useful stuff
            this.schema = schema;
            this.dsl = addSchemaMapping(endpoint.dslContext());
            this.schemaCleaner = new SchemaCleaner(schema, dsl);
        }
    }

    public DbEndpoint getDbEndpoint() {
        return endpoint;
    }

    public Schema getSchema() {
        return schema;
    }

    public DSLContext getDslContext() {
        return dsl;
    }

    public SchemaCleaner getSchemaCleaner() {
        return schemaCleaner;
    }

    /**
     * Complete this endpoint so it can be used in tests.
     *
     * @param endpoint endpoint to complete
     * @throws InterruptedException if we're interrupted
     */
    private void complete(DbEndpoint endpoint) throws InterruptedException {
        MultiDbTestBase.getTestCompleter().completeEndpoint(endpoint);
        endpoint.awaitCompletion(60L, TimeUnit.SECONDS);
    }

    private DSLContext addSchemaMapping(DSLContext dsl) {
        Configuration config = dsl.configuration().derive();
        RenderMapping mapping = config.settings().getRenderMapping();
        if (mapping == null) {
            mapping = new RenderMapping();
            config.settings().setRenderMapping(mapping);
        }
        List<MappedSchema> schemaMappings = mapping.getSchemata();
        if (schemaMappings == null) {
            schemaMappings = new ArrayList<>();
            mapping.setSchemata(schemaMappings);
        }
        schemaMappings.add(new MappedSchema()
                .withInput(schema.getName())
                .withOutput(endpoint.getConfig().getSchemaName()));
        return DSL.using(config);
    }

    /**
     * Remove DB objects provisioned for our endpoints - triggered by a JVM shutdown hook.
     */
    private static void tearDownEndpoints() {
        for (TestDbEndpoint testEndpoint : testEndpoints.values()) {
            DbEndpoint endpoint = testEndpoint.getDbEndpoint();
            try {
                endpoint.getAdapter().tearDown();
            } catch (InterruptedException e) {
                logger.error("Failed to tear down test endpoint {}", endpoint, e);
            }
        }
        testEndpoints.clear();
    }

    private static void mangle(DbEndpointConfig config) {
        config.setUserName(mangle(config.getUserName()));
        config.setDatabaseName(mangle(config.getDatabaseName()));
        config.setSchemaName(mangle(config.getSchemaName()));
    }

    private static String mangle(String s) {
        return knownManglings.computeIfAbsent(s, _s -> {
            byte[] bytes = s.getBytes();
            xxHash32.update(bytes, 0, bytes.length);
            xxHash32.update(Longs.toByteArray(startTime), 0, Long.BYTES);
            String suffix = base36(xxHash32.getValue());
            String fixed = s.replaceAll("[^a-zA-Z0-9]", "");
            return StringUtils.truncate(fixed, 7) + "_" + suffix;
        });
    }

    /**
     * Encode a given int value in "base 36" - i.e. a sequence of lower-case letters and digits.
     * Result will be no more than seven chars long because 36^6 < 2^32 <= 36^7.
     *
     * @param i value to be encoded
     * @return encoded string
     */
    private static String base36(int i) {
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

    private static DBPasswordUtil getMockDbPasswordUtil() {
        DBPasswordUtil dbPasswordUtil = mock(DBPasswordUtil.class);
        when(dbPasswordUtil.getSqlDbRootUsername(any()))
                .thenAnswer(invocation -> DBPasswordUtil.obtainDefaultRootDbUser(
                        invocation.getArgumentAt(0, String.class)));
        when(dbPasswordUtil.getSqlDbRootPassword()).thenReturn(DBPasswordUtil.obtainDefaultPW());
        return dbPasswordUtil;
    }
}
