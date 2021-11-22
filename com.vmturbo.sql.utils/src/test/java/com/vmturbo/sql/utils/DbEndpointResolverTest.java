package com.vmturbo.sql.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import com.google.common.collect.ImmutableMap;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Tests of {@link DbEndpointResolver} class.
 */
public class DbEndpointResolverTest {

    private final Map<String, String> configMap = new HashMap<>();
    private final UnaryOperator<String> resolver = configMap::get;
    private final DBPasswordUtil dbPasswordUtil = mock(DBPasswordUtil.class);
    private final DbEndpointCompleter completer = mock(DbEndpointCompleter.class);

    /**
     * Manage feature flags.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule()
            .testAllCombos(FeatureFlags.POSTGRES_PRIMARY_DB);

    /**
     * Set up our mock behaviors and establish a fake component name.
     */
    @Before
    public void before() {
        doAnswer(i -> new DbEndpoint((DbEndpointConfig)i.getArguments()[0], completer))
                .when(completer).register(any(DbEndpointConfig.class));
        doReturn("pw").when(dbPasswordUtil).getSqlDbRootPassword();
        doAnswer(i -> "POSTGRES".equals(i.getArgumentAt(0, String.class)) ? "postgres" : "root")
                .when(dbPasswordUtil).getSqlDbRootUsername(anyString());
        configMap.put("component_type", "xyzzy");
    }

    /**
     * Go through all config properties in a Postgres endpoint that is fully based on built-in
     * defaults, and check that those defaults are as expected.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatPostgresDefaultsAreCorrect() throws UnsupportedDialectException {
        final DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.POSTGRES, completer).build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpointConfig config = ep.getConfig();
        assertThat(config.getAccess(), is(DbEndpointAccess.READ_ONLY));
        assertThat(config.getDatabaseName(), is("xyzzy"));
        assertThat(config.getDialect(), is(SQLDialect.POSTGRES));
        assertThat(config.getDriverProperties(), is(anEmptyMap()));
        assertThat(config.getEndpointEnabled(), is(true));
        assertThat(config.getFlywayCallbacks(), emptyArray());
        assertThat(config.getHost(), is("localhost"));
        assertThat(config.getMigrationLocations(), is(FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
                                                      ? "db.migrations.xyzzy.postgres"
                                                      : "db.migration.xyzzy"));
        assertThat(config.getName(), is("test"));
        assertThat(config.getPassword(), is("pw"));
        assertThat(config.getPort(), is(5432));
        assertThat(config.getProvisioningSuffix(), is(""));
        assertThat(config.getRootPassword(), is("pw"));
        assertThat(config.getRootUserName(), is("postgres"));
        assertThat(config.isRootAccessEnabled(), is(false));
        assertThat(config.getSchemaName(), is("xyzzy"));
        assertThat(config.getSecure(), is(false));
        assertThat(config.getShouldProvisionDatabase(), is(false));
        assertThat(config.getShouldProvisionUser(), is(false));
        assertThat(config.getTemplate(), is(nullValue()));
        assertThat(config.getUserName(), is("xyzzy"));
        assertThat(config.getMinPoolSize(), is(1));
        assertThat(config.getMaxPoolSize(), is(10));
    }

    /**
     * Similar to {@link #testThatPostgresDefaultsAreCorrect()} but for Mariadb.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatMariaDBDefaultsAreCorrect() throws UnsupportedDialectException {
        final DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.MARIADB, completer).build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpointConfig config = ep.getConfig();
        assertThat(config.getAccess(), is(DbEndpointAccess.READ_ONLY));
        assertThat(config.getDatabaseName(), is("xyzzy"));
        assertThat(config.getDialect(), is(SQLDialect.MARIADB));
        assertThat(config.getDriverProperties(), is(ImmutableMap.of("useServerPrepStmts", "true")));
        assertThat(config.getEndpointEnabled(), is(true));
        assertThat(config.getFlywayCallbacks(), emptyArray());
        assertThat(config.getHost(), is("localhost"));
        assertThat(config.getMigrationLocations(), is(FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
                                                      ? "db.migrations.xyzzy.mariadb"
                                                      : "db.migration.xyzzy"));
        assertThat(config.getName(), is("test"));
        assertThat(config.getPassword(), is("pw"));
        assertThat(config.getPort(), is(3306));
        assertThat(config.getProvisioningSuffix(), is(""));
        assertThat(config.getRootPassword(), is("pw"));
        assertThat(config.getRootUserName(), is("root"));
        assertThat(config.isRootAccessEnabled(), is(false));
        assertThat(config.getSchemaName(), is("xyzzy"));
        assertThat(config.getSecure(), is(false));
        assertThat(config.getShouldProvisionDatabase(), is(false));
        assertThat(config.getShouldProvisionUser(), is(false));
        assertThat(config.getTemplate(), is(nullValue()));
        assertThat(config.getUserName(), is("xyzzy"));
        assertThat(config.getMinPoolSize(), is(1));
        assertThat(config.getMaxPoolSize(), is(10));
    }

    /**
     * Similar to {@link #testThatPostgresDefaultsAreCorrect()} but for MySQL.
     *
     * @throws UnsupportedDialectException shouldn't happen.
     */
    @Test
    public void testThatMySQLDefaultsAreCorrect() throws UnsupportedDialectException {
        final DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.MYSQL, completer).build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpointConfig config = ep.getConfig();
        assertThat(config.getAccess(), is(DbEndpointAccess.READ_ONLY));
        assertThat(config.getDatabaseName(), is("xyzzy"));
        assertThat(config.getDialect(), is(SQLDialect.MYSQL));
        assertThat(config.getDriverProperties(), is(anEmptyMap()));
        assertThat(config.getEndpointEnabled(), is(true));
        assertThat(config.getFlywayCallbacks(), emptyArray());
        assertThat(config.getHost(), is("localhost"));
        assertThat(config.getMigrationLocations(), is(FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
                                                      ? "db.migrations.xyzzy.mysql"
                                                      : "db.migration.xyzzy"));
        assertThat(config.getName(), is("test"));
        assertThat(config.getPassword(), is("pw"));
        assertThat(config.getPort(), is(3306));
        assertThat(config.getProvisioningSuffix(), is(""));
        assertThat(config.getRootPassword(), is("pw"));
        assertThat(config.getRootUserName(), is("root"));
        assertThat(config.isRootAccessEnabled(), is(false));
        assertThat(config.getSchemaName(), is("xyzzy"));
        assertThat(config.getSecure(), is(false));
        assertThat(config.getShouldProvisionDatabase(), is(false));
        assertThat(config.getShouldProvisionUser(), is(false));
        assertThat(config.getTemplate(), is(nullValue()));
        assertThat(config.getUserName(), is("xyzzy"));
        assertThat(config.getMinPoolSize(), is(1));
        assertThat(config.getMaxPoolSize(), is(10));
    }

    /**
     * Check that values for all properties specifiable via the builder are used in preference to
     * built-in defaults.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatBuilderSuppliedPropertiesOverrideDefaults() throws UnsupportedDialectException {
        final DbEndpoint ep = getFullySpecifiedEndpoint("test", SQLDialect.MYSQL);
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpointConfig config = ep.getConfig();
        assertThat(config.getAccess(), is(DbEndpointAccess.ALL));
        assertThat(config.getDatabaseName(), is("foobar-xxxx"));
        assertThat(config.getDialect(), is(SQLDialect.MYSQL));
        assertThat(config.getDriverProperties(), is(ImmutableMap.of("cookies", "yum")));
        assertThat(config.getEndpointEnabled(), is(false));
        assertThat(config.getFlywayCallbacks(), emptyArray());
        assertThat(config.getHost(), is("h"));
        assertThat(config.getMigrationLocations(), is("db.mig1,db.mig2"));
        assertThat(config.getName(), is("test"));
        assertThat(config.getPassword(), is("user-pw"));
        assertThat(config.getPort(), is(12345));
        assertThat(config.getProvisioningSuffix(), is("-xxxx"));
        assertThat(config.getRootPassword(), is("root-pw"));
        assertThat(config.getRootUserName(), is("myroot"));
        assertThat(config.isRootAccessEnabled(), is(true));
        assertThat(config.getSchemaName(), is("s-xxxx"));
        assertThat(config.getSecure(), is(true));
        assertThat(config.getShouldProvisionDatabase(), is(true));
        assertThat(config.getShouldProvisionUser(), is(true));
        assertThat(config.getTemplate(), is(nullValue()));
        assertThat(config.getUserName(), is("u-xxxx"));
        assertThat(config.getMinPoolSize(), is(1));
        // Should be set to the absolute maximum pool size (currently 500), since we configured it
        // higher than the maximum.
        assertThat(config.getMaxPoolSize(), is(500));
    }

    /**
     * Test that when all properties are specified in external configuration, those values take
     * precedence over built-in defaults.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatFullyConfiguredEndpointResolvesCorrectly() throws UnsupportedDialectException {
        final DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.MYSQL, completer).build();
        fullyConfigure("test");
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpointConfig config = ep.getConfig();
        assertThat(config.getAccess(), is(DbEndpointAccess.ALL));
        assertThat(config.getDatabaseName(), is("foobar-xxxx"));
        assertThat(config.getDialect(), is(SQLDialect.MYSQL));
        assertThat(config.getDriverProperties(), is(ImmutableMap.of("cookies", "yum")));
        assertThat(config.getEndpointEnabled(), is(false));
        assertThat(config.getFlywayCallbacks(), emptyArray());
        assertThat(config.getHost(), is("h"));
        assertThat(config.getMigrationLocations(), is("db.mig1,db.mig2"));
        assertThat(config.getName(), is("test"));
        assertThat(config.getPassword(), is("user-pw"));
        assertThat(config.getPort(), is(12345));
        assertThat(config.getProvisioningSuffix(), is("-xxxx"));
        assertThat(config.getRootPassword(), is("root-pw"));
        assertThat(config.getRootUserName(), is("myroot"));
        assertThat(config.isRootAccessEnabled(), is(true));
        assertThat(config.getSchemaName(), is("s-xxxx"));
        assertThat(config.getSecure(), is(true));
        assertThat(config.getShouldProvisionDatabase(), is(true));
        assertThat(config.getShouldProvisionUser(), is(true));
        assertThat(config.getTemplate(), is(nullValue()));
        assertThat(config.getUserName(), is("u-xxxx"));
        assertThat(config.getMinPoolSize(), is(3));
        assertThat(config.getMaxPoolSize(), is(7));
    }

    /**
     * Check that a derived endpoint uses the base endpoint's values in preference to built-in
     * defaults.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatDerivedEndpointUsesBaseValuesAsDefaults() throws UnsupportedDialectException {
        final DbEndpoint base = new DbEndpointBuilder("base", SQLDialect.MYSQL, completer).build();
        fullyConfigure("base");
        new DbEndpointResolver(base.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpoint derived = new DbEndpointBuilder("derived", SQLDialect.MYSQL, completer)
                .like(base).build();
        new DbEndpointResolver(derived.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpointConfig config = derived.getConfig();
        assertThat(config.getAccess(), is(DbEndpointAccess.ALL));
        assertThat(config.getDatabaseName(), is("foobar-xxxx"));
        assertThat(config.getDialect(), is(SQLDialect.MYSQL));
        assertThat(config.getDriverProperties(), is(ImmutableMap.of("cookies", "yum")));
        assertThat(config.getEndpointEnabled(), is(false));
        assertThat(config.getFlywayCallbacks(), emptyArray());
        assertThat(config.getHost(), is("h"));
        assertThat(config.getMigrationLocations(), is("db.mig1,db.mig2"));
        assertThat(config.getName(), is("derived"));
        assertThat(config.getPassword(), is("user-pw"));
        assertThat(config.getPort(), is(12345));
        assertThat(config.getProvisioningSuffix(), is("-xxxx"));
        assertThat(config.getRootPassword(), is("root-pw"));
        assertThat(config.getRootUserName(), is("myroot"));
        assertThat(config.isRootAccessEnabled(), is(true));
        assertThat(config.getSchemaName(), is("s-xxxx"));
        assertThat(config.getSecure(), is(true));
        assertThat(config.getShouldProvisionDatabase(), is(true));
        assertThat(config.getShouldProvisionUser(), is(true));
        assertThat(config.getTemplate(), is(base));
        assertThat(config.getUserName(), is("u-xxxx"));
        assertThat(config.getMinPoolSize(), is(3));
        assertThat(config.getMaxPoolSize(), is(7));
    }

    /**
     * Make sure that with multiple levels of derived endpoints, a derived endpoint has access to
     * values from ancestors other than its immediate base.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatMultiLevelDerivedEndpointWorks() throws UnsupportedDialectException {
        fullyConfigure("base");
        configMap.put("middle.databaseName", "middle");
        configMap.put("last.schemaName", "last");
        final DbEndpoint base = new DbEndpointBuilder("base", SQLDialect.MYSQL, completer).build();
        new DbEndpointResolver(base.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpoint middle = new DbEndpointBuilder("middle", SQLDialect.MYSQL, completer)
                .like(base).build();
        new DbEndpointResolver(middle.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpoint last = new DbEndpointBuilder("last", SQLDialect.MYSQL, completer)
                .like(middle).build();
        new DbEndpointResolver(last.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpointConfig config = last.getConfig();
        assertThat(config.getUserName(), is("u-xxxx"));
        assertThat(config.getDatabaseName(), is("middle-xxxx"));
        assertThat(config.getSchemaName(), is("last-xxxx"));
        assertThat(config.getMinPoolSize(), is(3));
        assertThat(config.getMaxPoolSize(), is(7));
    }

    /**
     * Check that with multi-level endpoint names, properties supplied using prefixes of the name
     * work, with longer prefixes taking precedence over shorter ones.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatNamePrefixConfigsWork() throws UnsupportedDialectException {
        configMap.put("x.y.z.userName", "u");
        configMap.put("x.y.userName", "wrong");
        configMap.put("x.userName", "bzzzt");
        configMap.put("x.y.password", "u-pw");
        configMap.put("x.databaseName", "d");
        final DbEndpoint ep = new DbEndpointBuilder("x.y.z", SQLDialect.MYSQL, completer).build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        final DbEndpointConfig config = ep.getConfig();
        assertThat(config.getUserName(), is("u"));
        assertThat(config.getPassword(), is("u-pw"));
        assertThat(config.getDatabaseName(), is("d"));
    }

    /**
     * Test that property values specified by `dbs.&lt;dialect&gt;` prefixes are used in the absence
     * of endpoint-name-based properties.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatDbDefaultsWork() throws UnsupportedDialectException {
        configMap.put("dbs.postgresDefault.port", "15432");
        configMap.put("dbs.mysqlDefault.port", "13306");
        configMap.put("dbs.mariadbDefault.port", "13307");
        DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.POSTGRES, completer).build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().getPort(), is(15432));
        ep = new DbEndpointBuilder("test", SQLDialect.MYSQL, completer).build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().getPort(), is(13306));
        ep = new DbEndpointBuilder("test", SQLDialect.MARIADB, completer).build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().getPort(), is(13307));
    }

    /**
     * Test that endpoint enablement can be specified via a function applied to the resolver during
     * resolution.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatEndpointEnablementFnWorks() throws UnsupportedDialectException {
        configMap.put("foo", "frotz");
        DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.POSTGRES, completer)
                .withEndpointEnabled(r -> r.apply("foo") != null).build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().getEndpointEnabled(), is(true));
        ep = new DbEndpointBuilder("test", SQLDialect.POSTGRES, completer)
                .withEndpointEnabled(r -> r.apply("bar") != null).build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().getEndpointEnabled(), is(false));
    }

    /**
     * Test that the {@link DbEndpointBuilder#withShouldProvision(boolean)} method applies to both
     * database and user provisioning.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatShouldProvisionAffectsUserAndDatabase() throws UnsupportedDialectException {
        DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.POSTGRES, completer)
                .withShouldProvision(true)
                .build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().getShouldProvisionUser(), is(true));
        assertThat(ep.getConfig().getShouldProvisionDatabase(), is(true));
    }

    /**
     * Test that {@link DbEndpointBuilder#withNoMigrations()} method sets a value that will prevent
     * migrations from being performed.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatWithNoMigrationsWorks() throws UnsupportedDialectException {
        DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.POSTGRES, completer)
                .withNoMigrations()
                .build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().getMigrationLocations(), is(""));
    }

    /**
     * Test that the {@link DbEndpointBuilder#withFlywayCallbacks(FlywayCallback...)} method works
     * correctly.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatWithFlywayCallbacksWorks() throws UnsupportedDialectException {
        final FlywayCallback cb1 = mock(FlywayCallback.class);
        final FlywayCallback cb2 = mock(FlywayCallback.class);
        DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.POSTGRES, completer)
                .withFlywayCallbacks(cb1, cb2)
                .build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().getFlywayCallbacks(), arrayContaining(cb1, cb2));
    }

    /**
     * Test that the {@link DbEndpointBuilder#setAbstract()} method works correctly.
     *
     * @throws UnsupportedDialectException shouldn't happen
     */
    @Test
    public void testThatSetAbstractWorks() throws UnsupportedDialectException {
        DbEndpoint ep = new DbEndpointBuilder("test", SQLDialect.POSTGRES, completer)
                .setAbstract()
                .build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().isAbstract(), is(true));

        ep = new DbEndpointBuilder("test", SQLDialect.POSTGRES, completer)
                .build();
        new DbEndpointResolver(ep.getConfig(), resolver, dbPasswordUtil).resolve();
        assertThat(ep.getConfig().isAbstract(), is(false));
    }

    /**
     * Provide values for all externally configurable properties, using the given name as a prefix
     * for all properties.
     *
     * @param name name to be used to prefix all properties
     */
    private void fullyConfigure(String name) {
        final String prefix = name + ".";
        configMap.put(prefix + "access", DbEndpointAccess.ALL.name());
        configMap.put(prefix + "databaseName", "foobar");
        configMap.put(prefix + "destructiveProvisioningEnabled", "true");
        configMap.put(prefix + "driverProperties", "{'cookies':'yum'}");
        configMap.put(prefix + "endpointEnabled", "false");
        configMap.put(prefix + "host", "h");
        configMap.put(prefix + "migrationLocations", "db.mig1,db.mig2");
        configMap.put(prefix + "password", "user-pw");
        configMap.put(prefix + "port", "12345");
        configMap.put(prefix + "nameSuffix", "-xxxx");
        configMap.put(prefix + "rootPassword", "root-pw");
        configMap.put(prefix + "rootUserName", "myroot");
        configMap.put(prefix + "rootAccessEnabled", "true");
        configMap.put(prefix + "schemaName", "s");
        configMap.put(prefix + "secure", "true");
        configMap.put(prefix + "shouldProvisionDatabase", "true");
        configMap.put(prefix + "shouldProvisionUser", "true");
        configMap.put(prefix + "userName", "u");
        configMap.put(prefix + "conPoolInitialSize", "3");
        configMap.put(prefix + "conPoolMaxActive", "7");
    }

    /**
     * Create an endpoint with all builder-configurable properties specified in the builder.
     *
     * @param name    name of endpoint
     * @param dialect DB dialect for endpoint
     * @return new endpoint
     */
    private DbEndpoint getFullySpecifiedEndpoint(String name, SQLDialect dialect) {
        return new DbEndpointBuilder(name, dialect, completer)
                .withAccess(DbEndpointAccess.ALL)
                .withDatabaseName("foobar")
                .withDriverProperties(ImmutableMap.of("cookies", "yum"))
                .withEndpointEnabled(false)
                .withHost("h")
                .withMigrationLocations("db.mig1,db.mig2")
                .withPassword("user-pw")
                .withPort(12345)
                .withProvisioningSuffix("-xxxx")
                .withRootPassword("root-pw")
                .withRootUserName("myroot")
                .withRootAccessEnabled(true)
                .withSchemaName("s")
                .withSecure(true)
                .withShouldProvisionDatabase(true)
                .withShouldProvisionUser(true)
                .withUserName("u")
                .withUseConnectionPool(true)
                .withMinPoolSize(1)
                // Intentionally setting this higher than the absolute max to ensure we are
                // enforcing the absolute max.
                .withMaxPoolSize(2525)
                .build();
    }
}
