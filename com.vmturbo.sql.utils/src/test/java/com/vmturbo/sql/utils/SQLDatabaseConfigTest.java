package com.vmturbo.sql.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * unit test for {@link SQLDatabaseConfig}
 */
public class SQLDatabaseConfigTest {

    @Test
    public void testSecureURL() {
        System.setProperty("enableSecureDBConnection", "true");
        TestSQLDataBseConfigImpl testSQLDataBseConfig = new TestSQLDataBseConfigImpl();
        assertEquals("jdbc:mysql:?useSSL=true&trustServerCertificate=true", testSQLDataBseConfig.getURL());
    }

    @Test
    public void testInSecureURL() {
        System.setProperty("enableSecureDBConnection", "false");
        TestSQLDataBseConfigImpl testSQLDataBseConfig = new TestSQLDataBseConfigImpl();
        assertEquals("jdbc:mysql:", testSQLDataBseConfig.getURL());
    }

    @Test
    public void tesDefalURL() {
        System.clearProperty("enableSecureDBConnection");
        TestSQLDataBseConfigImpl testSQLDataBseConfig = new TestSQLDataBseConfigImpl();
        assertEquals("jdbc:mysql:", testSQLDataBseConfig.getURL());
    }

    static class TestSQLDataBseConfigImpl extends SQLDatabaseConfig {
        String getURL() {
            return super.getDbUrl();
        }
    }
}