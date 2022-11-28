package com.vmturbo.sql.utils;

import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.function.Function;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.Schema;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.Parameterized;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * This class constructs a rule chain to manage DB provisioning and access for live DB tests, and
 * with a need to work in both legacy (SQLDatabaseConfig-based) and updated (DbEndpoint-based) DB
 * configuration. In the latter case, the rule chain can support varying dialects repreenting
 * different choices of DB server.
 *
 * <p>The rule chain is designed to be used in test classes that use the {@link Parameterized}
 * JUnit runner, with each set of parameter values representing a scenario (legacy/updated +
 * dialect). The test class should extend this class.</p>
 */
public class MultiDbTestBase {

    private static final Logger logger = LogManager.getLogger();

    // individual parameter sets for legacy, updated/MARIADB, and updated/POSTGRES scenarios
    protected static final Object[] LEGACY_MARIADB_PARAMS =
            new Object[]{false, SQLDialect.MARIADB};
    protected static final Object[] DBENDPOINT_MARIADB_PARAMS =
            new Object[]{true, SQLDialect.MARIADB};
    protected static final Object[] DBENDPOINT_POSTGRES_PARAMS =
            new Object[]{true, SQLDialect.POSTGRES};

    // collections of parameter values that will cause tests to be run for multiple scenarios.
    // Each set should be used for components tha have reached a corresponding phase of conversion
    // under the postgres conversion effort.
    protected static final Object[][] UNCONVERTED_PARAMS = new Object[][]{
            LEGACY_MARIADB_PARAMS};
    protected static final Object[][] DBENDPOINT_CONVERTED_PARAMS = new Object[][]{
            LEGACY_MARIADB_PARAMS, DBENDPOINT_MARIADB_PARAMS};
    protected static final Object[][] POSTGRES_CONVERTED_PARAMS = new Object[][]{
            LEGACY_MARIADB_PARAMS, DBENDPOINT_MARIADB_PARAMS, DBENDPOINT_POSTGRES_PARAMS};

    // Developer override param options
    protected static final Object[][] LEGACY_MARIADB_ONLY_PARAMS = new Object[][]{
            LEGACY_MARIADB_PARAMS};
    protected static final Object[][] MARIADB_ONLY_PARAMS = new Object[][]{
            DBENDPOINT_MARIADB_PARAMS};
    protected static final Object[][] POSTGRES_ONLY_PARAMS = new Object[][]{
            DBENDPOINT_POSTGRES_PARAMS};

    protected final FeatureFlagTestRule featureFlagTestRule;
    private final DbConfigurationRule dbConfigurationRule;
    private final DbCleanupRule dbCleanupRule;
    private final DbEndpointTestRule dbEndpointTestRule;
    protected final TestRule ruleChain;
    private final Schema schema;
    private final boolean configurableDbDialect;
    private final Function<SQLDialect, DbEndpoint> endpointByDialect;
    private SQLDialect dialect;
    private static DBPasswordUtil testPasswordUtil = mock(DBPasswordUtil.class);

    public static DbEndpointCompleter getTestCompleter() {
        return new DbEndpointCompleter(s -> null, testPasswordUtil, "10s");
    }

    protected FeatureFlagTestRule featureFlagTestRule(boolean configurableDbDialect) {
        return configurableDbDialect ? new FeatureFlagTestRule(FeatureFlags.POSTGRES_PRIMARY_DB)
                                     : new FeatureFlagTestRule();
    }

    /**
     * By default a MultiDbTestBase test will run once for each DB specified in the parameter array.
     * Developers can set the 'MultiDbTestParams' system property to override the default value of
     * POSTGRES_CONVERTED_PARAMS which specifies 1. legacy mariaDB, 2. dbendpoint mariadb, and
     * 3. dbendpoint postgres.  For example, set -DMultiDbTestParams=LEGACY_MARIADB_ONLY_PARAMS to
     * run the test once accessing mariaDB with SQLDatabaseConfig, or set as
     * -DMultiDbTestParams=MARIADB_ONLY_PARAMS to run the test once accessing mariadb
     * using DbEndpoint, or as DMultiDbTestParams=POSTGRES_ONLY_PARAMS to run the test once
     * using postgres.
     *
     * @return the Junit parameters that determine what DBs are tested.
     */
    protected static Object[][] getParameters() {
        String paramsProp = System.getProperty("MultiDbTestParams");
        if (paramsProp != null && !paramsProp.isEmpty()) {
            try {
                Field field = MultiDbTestBase.class.getDeclaredField(paramsProp.trim());
                Object[][] params = (Object[][])field.get(new Object[0][0]);
                logger.info("Using MultiDbTestBase parameter override: {}", paramsProp);
                return params;
            } catch (Exception e) {
                throw new IllegalStateException("Invalid MultiDbTestParams value: " + paramsProp
                        + ", no Object[][] field found with that name");
            }
        }
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    /**
     * Construct a rule chaing for a given scenario.
     *
     * @param schema                {@link Schema} that will be provisioned for this test class
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect               DB dialect to use
     * @param schemaName            used in configuring some DbEndpoint properties
     * @param endpointByDialect     obtain an endpoint when using configurable dialect
     */
    public MultiDbTestBase(Schema schema, boolean configurableDbDialect, SQLDialect dialect,
            String schemaName, @Nullable Function<SQLDialect, DbEndpoint> endpointByDialect) {
        if (configurableDbDialect && endpointByDialect == null) {
            throw new IllegalArgumentException(
                    "Null endpoint function not allowed with configurable DB dialect");
        }
        this.schema = schema;
        this.configurableDbDialect = configurableDbDialect;
        this.dialect = dialect;
        this.endpointByDialect = endpointByDialect;
        this.featureFlagTestRule = featureFlagTestRule(configurableDbDialect);
        this.dbConfigurationRule = dbConfigurationRule(schema);
        this.dbCleanupRule = dbConfigurationRule.cleanupRule();
        this.dbEndpointTestRule = dbEndpointTestRule(schemaName);
        this.ruleChain = multiDbRuleChain(configurableDbDialect);
        // enable POSTGRES_PRIMARY_DB in case it's needed in current environment (i.e. outside
        // of tests, where it will be reset prior to every tests)
        if (configurableDbDialect) {
            featureFlagTestRule.enable(FeatureFlags.POSTGRES_PRIMARY_DB);
        } else {
            featureFlagTestRule.disable(FeatureFlags.POSTGRES_PRIMARY_DB);
        }
    }

    /**
     * Declare a rule chain that will manage provisioning and access to DBs by tests.
     *
     * @return rule chain
     */
    @Rule
    public TestRule multiDbTestRule() {
        return ruleChain;
    }

    protected final DbConfigurationRule dbConfigurationRule(Schema schema) {
        return new DbConfigurationRule(schema);
    }

    protected DbEndpointTestRule dbEndpointTestRule(String componentName) {
        return new DbEndpointTestRule(componentName);
    }

    /**
     * Create the rule chain based on parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @return rule chain instance
     */
    public TestRule multiDbRuleChain(boolean configurableDbDialect) {
        // we always need to set up feature flags
        RuleChain ruleChain = RuleChain.outerRule(featureFlagTestRule);
        if (configurableDbDialect) {
            // if we're using endpoints, add the rule that manages them
            ruleChain = ruleChain
                    .around(dbEndpointTestRule);
        } else {
            // else add rules to manage legacy DB provisioning
            ruleChain = ruleChain
                    .around(dbConfigurationRule)
                    .around(dbCleanupRule);
        }
        return ruleChain;
    }

    protected DSLContext getDslContext()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        if (configurableDbDialect) {
            return getTestEndpoint().getDslContext();
        } else {
            return dbConfigurationRule.getDslContext();
        }
    }

    protected DataSource getDataSource()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        if (configurableDbDialect) {
            return getTestEndpoint().getDbEndpoint().datasource();
        } else {
            return dbConfigurationRule.getDataSource();
        }
    }

    private TestDbEndpoint testEndpoint;

    protected TestDbEndpoint getTestEndpoint()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        if (testEndpoint == null) {
            testEndpoint = dbEndpointTestRule.completeEndpoint(endpointByDialect.apply(dialect),
                    schema);
        }
        return testEndpoint;
    }

    /**
     * Add a DbEndpoint to be managed by the rule chain.
     *
     * @param endpoint endpoint
     * @return a {@link TestDbEndpoint} instance constructed for the endpoint
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    protected TestDbEndpoint addDbEndpoint(DbEndpoint endpoint)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        return dbEndpointTestRule.completeEndpoint(endpoint, schema);
    }

}
