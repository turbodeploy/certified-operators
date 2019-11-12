package com.vmturbo.components.common.health.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.components.common.health.SimpleHealthStatus;

/**
 * Test the SQLDBHealthMonitor class.
 */
public class SQLDBHealthMonitorTest {
    private static final double POLLING_INTERVAL_SECS = 0.05;

    @Test
    public void testHealthyConnection() throws Exception {
        // set up a mock connection factory that returns a mock connection -- this should be enough
        // to count as a "healthy" database connection factory.
        DataSource mockConnectionFactory = Mockito.mock(DataSource.class);
        Mockito.when(mockConnectionFactory.getConnection()).thenReturn(Mockito.mock(Connection.class));
        SQLDBHealthMonitor testHealthMonitor = new SQLDBHealthMonitor("Test DB Monitor", POLLING_INTERVAL_SECS, mockConnectionFactory::getConnection);

        // verify that the next health check reports a healthy connection.
        SimpleHealthStatus healthStatus = testHealthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(1));
        Assert.assertTrue("Mock DB should look healthy.",healthStatus.isHealthy());
    }

    @Test
    public void testUnhealthyConnection() throws Exception {
        // set up a mock connection factory that throws an exception -- this should count as an
        // "unhealthy" database connection factory.
        DataSource mockConnectionFactory = Mockito.mock(DataSource.class);
        Mockito.when(mockConnectionFactory.getConnection()).thenThrow(new SQLException("Fake Error!"));
        SQLDBHealthMonitor testHealthMonitor = new SQLDBHealthMonitor("Another test monitor", POLLING_INTERVAL_SECS, mockConnectionFactory::getConnection);

        // verify that the next health check returns an unhealthy status.
        SimpleHealthStatus healthStatus = testHealthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(1));
        Assert.assertFalse("Mock DB should look unhealthy.",healthStatus.isHealthy());
    }
}
