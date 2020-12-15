package com.vmturbo.sql.utils;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.jooq.SQLDialect;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;

import com.vmturbo.auth.api.db.DBPasswordUtil;

/**
 * unit test for {@link SQLDatabaseConfig}.
 */
public class SQLDatabaseConfigTest {

    /**
     * Expected DB URL base for testing.
     */
    public static final String EXPECTED_DB_URL_BASE = "jdbc:mariadb://localhost:3306/vmtdb" +
            "?useServerPrepStmts=true";
    public static final String ENABLE_SECURE_DB_CONNECTION = "enableSecureDBConnection";

    @Test
    public void testSecureURL() {
        System.setProperty(ENABLE_SECURE_DB_CONNECTION, "true");
        TestSQLDataBaseConfigImpl testSQLDataBseConfig = new TestSQLDataBaseConfigImpl();
        assertEquals(EXPECTED_DB_URL_BASE +
                "&useSSL=true&trustServerCertificate=true", testSQLDataBseConfig.getURL());
    }

    @Test
    public void testInSecureURL() {
        System.setProperty("enableSecureDBConnection", "false");
        TestSQLDataBaseConfigImpl testSQLDataBseConfig = new TestSQLDataBaseConfigImpl();
        assertEquals(EXPECTED_DB_URL_BASE, testSQLDataBseConfig.getURL());
    }

    @Test
    public void tesDefalURL() {
        System.clearProperty("enableSecureDBConnection");
        TestSQLDataBaseConfigImpl testSQLDataBseConfig = new TestSQLDataBaseConfigImpl();
        assertEquals(EXPECTED_DB_URL_BASE, testSQLDataBseConfig.getURL());
    }

    /**
     * Verify if DB credential is provide by system, password reset logic will be invoked after
     * failing to establish connection. Note: when failing with system provided DB credential,
     * {@link BeanCreationException} is thrown.
     */
    @Test(expected = BeanCreationException.class)
    public void testPasswordReset() {
        System.setProperty("enableSecureDBConnection", "false");
        TestSQLDataBaseConfigImpl testSQLDataBseConfig = new TestSQLDataBaseConfigImpl();
        testSQLDataBseConfig.dataSourceConfig("vmtdb", "wronguser", "wrongpass", false);
    }

    /**
     * Verify DB user without GRANT permission will NOT reset/drop existing component user.
     * Set root password to "vmturbo" before running it.
     * @throws SQLException if DB exception is thrown.
     */
    @Ignore("Integration test which relies on default DB root credential")
    @Test
    public void testCanGrantPrivilegenNegative() throws SQLException {
        TestSQLDataBaseConfigImpl testSQLDataBseConfig = new TestSQLDataBaseConfigImpl();
        final String tmpuser = "tmpuser";
        final String tmppass = "tmppass";
        final String schemaName = "vmtdb ";
        final Connection connection =
                testSQLDataBseConfig.dataSource(EXPECTED_DB_URL_BASE, "root", "vmturbo")
                        .getConnection();
        dropDbUser(connection, tmpuser);
        createDbUser(connection, tmpuser, tmppass);
        final String requestUser = "'" + "tmpuser" + "'@'%'";
        connection.createStatement().execute(
                String.format("GRANT CREATE, SELECT ON `%s`.* TO %s IDENTIFIED BY '%s'", schemaName,
                        requestUser, tmppass));
        final String requestUserRemote = "'" + "tmpuser" + "'@'localhost'";
        connection.createStatement().execute(
                String.format("GRANT CREATE, SELECT ON `%s`.* TO %s IDENTIFIED BY '%s'", schemaName,
                        requestUserRemote, tmppass));
        connection.createStatement().execute(
                String.format("GRANT ALL PRIVILEGES ON `%s`.* TO %s IDENTIFIED BY '%s'", schemaName,
                        requestUser, tmppass));
        connection.createStatement().execute(
                String.format("GRANT ALL PRIVILEGES ON `%s`.* TO %s IDENTIFIED BY '%s'", schemaName,
                        requestUserRemote, tmppass));
        connection.createStatement().execute("FLUSH PRIVILEGES");
        assertFalse(SQLDatabaseConfig.hasGrantPrivilege(
                testSQLDataBseConfig.dataSource(EXPECTED_DB_URL_BASE, tmpuser, tmppass)
                        .getConnection(), tmpuser, "vmturbo", schemaName));
    }

    /**
     * Verify getting DB root password.
     * If DB password passed in from environment, use it;
     * Next, if `isGettingPassFromAuth` is true, return from Auth component, otherwise
     * return the default root DB password.
     */
    @Test
    public void tesGetDBRootPassword() {
        System.clearProperty("enableSecureDBConnection");
        SQLDatabaseConfig testSQLDataBseConfig = new SQLDatabaseConfig() {
            @Override
            public String getDbSchemaName() {
                return "test";
            }
        };
        DBPasswordUtil dbPasswordUtil = mock(DBPasswordUtil.class);
        testSQLDataBseConfig.setDbPasswordUtil(dbPasswordUtil);

        // if root DB is not available in the context and `isGettingPassFromAuth` is false,
        // get default root DB rootPass2
        assertEquals("vmturbo", testSQLDataBseConfig.getDBRootPassword(false));

        // if root DB is not available in the context and `isGettingPassFromAuth` is true,
        // call auth for root rootPass2
        final String rootPass = "pass1";
        when(dbPasswordUtil.getSqlDbRootPassword()).thenReturn(rootPass);
        assertEquals(rootPass, testSQLDataBseConfig.getDBRootPassword(true));

        // if root DB is available in the context use it.
        final String rootPass2 = "vmturbo";
        testSQLDataBseConfig.setDbRootPassword(rootPass2);
        assertEquals(rootPass2, testSQLDataBseConfig.getDBRootPassword(true));
        assertEquals(rootPass2, testSQLDataBseConfig.getDBRootPassword(false));
    }

    /**
     * Verify default root DB user have GRANT permission.
     * Set root password to "vmturbo" before running it.
     *
     * @throws SQLException if DB exception is thrown.
     */
    @Ignore("Integration test which relies on default DB root credential")
    @Test
    public void testCanGrantPrivilege() throws SQLException {
        TestSQLDataBaseConfigImpl testSQLDataBseConfig = new TestSQLDataBaseConfigImpl();
        assertTrue(SQLDatabaseConfig.hasGrantPrivilege(
                testSQLDataBseConfig.dataSource(EXPECTED_DB_URL_BASE, "root", "vmturbo")
                        .getConnection(), "root", "vmturbo", "vmtdb.* "));
    }

    /**
     * Connection config object for testing.
     */
    static class TestSQLDataBaseConfigImpl extends SQLDatabaseConfig {
        String getURL() {
            return getSQLConfigObject().getDbUrl();
        }

        @Override
        public SQLConfigObject getSQLConfigObject() {
            boolean secure = Boolean.valueOf(System.getProperty(ENABLE_SECURE_DB_CONNECTION));
            return new SQLDatabaseConfig.SQLConfigObject(
                    "localhost", 3306, "vmtdb", Optional.empty(), SQLDialect.MARIADB.name(),
                secure, ImmutableMap.of(SQLDialect.MARIADB, "useServerPrepStmts=true"));
        }

        @Override
        public String getDbSchemaName() {
            return "vmtdb";
        }
    }

    private static void createDbUser(@Nonnull final Connection rootConnection,
            @Nonnull final String requestUser, @Nonnull String dbPassword) {
        try {
            rootConnection.createStatement()
                    .execute(String.format("CREATE USER %s IDENTIFIED BY '%s'", requestUser,
                            dbPassword));
        } catch (SQLException e) {
            // did not previously exist
        }
    }

    private static void dropDbUser(@Nonnull final Connection rootConnection,
            @Nonnull final String user) {
        try {
            rootConnection.createStatement().execute(
                    // DROP USER IF EXISTS does not appear until MySQL 5.7, and breaks Jenkins build
                    String.format("DROP USER %s", user));
        } catch (SQLException e) {
            // did not previously exist
        }
    }
}
