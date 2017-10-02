package com.vmturbo.arangodb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.arangodb.ArangoDB;
import com.arangodb.entity.ArangoDBVersion;

import com.vmturbo.components.common.health.SimpleHealthStatus;

/**
 * Tests the ArangoHealthMonitor
 */
public class ArangoHealthMonitorTest {

    private ArangoHealthMonitor healthMonitor;
    private Supplier<ArangoDB> mockDBSupplier;
    private ArangoDB mockDB;

    @Before
    public void init() {
        // set up the db factory to return the mock db
        mockDBSupplier = new Supplier<ArangoDB>() {
            @Override
            public ArangoDB get() {
                return mockDB;
            }
        };
        mockDB = mock(ArangoDB.class);
    }

    /**
     * Test a "healthy" arango
     *
     * @throws Exception
     */
    @Test
    public void testHealthyArango() throws Exception {
        // setup a mock healthy arango
        ArangoDBVersion dummyVersion = mock(ArangoDBVersion.class);
        when(mockDB.getVersion()).thenReturn( dummyVersion );

        healthMonitor = new ArangoHealthMonitor(0.1, mockDBSupplier);
        // get the next check result and verify that it's healthy
        SimpleHealthStatus healthStatus = healthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(1));
        Assert.assertTrue("Mock Arango should be healthy.", healthStatus.isHealthy());
    }

    /**
     * Test an "unhealthy" arango
     *
     * @throws Exception
     */
    @Test
    public void testUnhealthyArango() throws Exception {
        // set up an unhealthy mock arango
        when(mockDB.getVersion()).thenReturn( null );

        // the mock DB should return a null version and trigger the unhealthy status
        healthMonitor = new ArangoHealthMonitor(0.1, mockDBSupplier);
        // get the next check result and verify that it's unhealthy
        SimpleHealthStatus healthStatus = healthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(1));
        Assert.assertFalse("Mock Arango should not be healthy.", healthStatus.isHealthy());
    }

}
