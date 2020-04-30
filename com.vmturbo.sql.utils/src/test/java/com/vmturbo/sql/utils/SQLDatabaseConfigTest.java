package com.vmturbo.sql.utils;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import org.jooq.SQLDialect;
import org.junit.Test;

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
}
