package com.vmturbo.influxdb;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.function.Supplier;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Pong;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.components.common.health.SimpleHealthStatus;

/**
 * Tests the InfluxHealthMonitor
 */
public class InfluxHealthMonitorTest {

    private InfluxHealthMonitor healthMonitor;
    private Supplier<InfluxDB> mockDBSupplier;
    private InfluxDB mockDB;

    @Before
    public void init() {
        // set up the db factory to return the mock db
        mockDBSupplier = () -> mockDB;
        mockDB = mock(InfluxDB.class);
    }

    /**
     * Test a "healthy" influx
     *
     * @throws Exception
     */
    @Test
    public void testHealthyInflux() throws Exception {
        // setup a mock healthy influx
        String dummyVersion = "foo";
        when(mockDB.ping()).thenReturn(mock(Pong.class));

        healthMonitor = new InfluxHealthMonitor(0.1, mockDBSupplier);
        // get the next check result and verify that it's healthy
        SimpleHealthStatus healthStatus = healthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(1));
        Assert.assertTrue("Mock Influx should be healthy.", healthStatus.isHealthy());
    }

    /**
     * Test an "unhealthy" influx
     *
     * @throws Exception
     */
    @Test
    public void testUnhealthyInflux() throws Exception {
        // set up an unhealthy mock influx
        doThrow(RuntimeException.class).when(mockDB).ping();

        // the mock DB should return a null version and trigger the unhealthy status
        healthMonitor = new InfluxHealthMonitor(0.1, mockDBSupplier);
        // get the next check result and verify that it's unhealthy
        SimpleHealthStatus healthStatus = healthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(1));
        Assert.assertFalse("Mock Influx should not be healthy.", healthStatus.isHealthy());
    }

}
