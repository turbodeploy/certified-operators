package com.vmturbo.components.test.utilities.alert;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.components.test.utilities.alert.MetricMeasurement.AlertStatus;

public class MetricMeasurementTest {

    @Test
    public void testGetNumberOfDeviations() {
        final MetricMeasurement measurement = new MetricMeasurement(1.0, 3.0, 1.0, 8.0,
            AlertMetricId.fromString("metric"), "test", "class");
        assertEquals(2.0, measurement.getNumberOfDeviations().get(), 0.00001);
    }

    @Test
    public void testGetNumberOfDeviationsWithZeroStdDev() {
        final MetricMeasurement measurement = new MetricMeasurement(1.0, 3.0, 0, 8.0,
            AlertMetricId.fromString("metric"), "test", "class");
        assertEquals(Optional.empty(), measurement.getNumberOfDeviations());
    }

    @Test
    public void testGetNumberOfDeviationsWithNoStdDev() {
        final MetricMeasurement measurement = new MetricMeasurement(1.0, 3.0, Optional.empty(), 8.0,
            AlertMetricId.fromString("metric"), "test", "class");
        assertEquals(Optional.empty(), measurement.getNumberOfDeviations());
    }

    @Test
    public void testDoesNotTriggerAlertByDefault() {
        final MetricMeasurement measurement = new MetricMeasurement(1.0, 3.0, 1.0, 8.0,
            AlertMetricId.fromString("metric"), "test", "class");
        assertEquals(AlertStatus.NORMAL, measurement.getAlertStatus());
    }

    @Test
    public void testPerformanceRegression() {
        final MetricMeasurement measurement = new MetricMeasurement(3.0, 1.0, 1.0, 8.0,
            AlertMetricId.fromString("metric"), "test", "class");
        measurement.setAlertStatus(AlertStatus.REGRESSION);
        assertEquals(AlertStatus.REGRESSION, measurement.getAlertStatus());
    }

    @Test
    public void testPerformanceImprovement() {
        final MetricMeasurement measurement = new MetricMeasurement(1.0, 3.0, 1.0, 8.0,
            AlertMetricId.fromString("metric"), "test", "class");
        measurement.setAlertStatus(AlertStatus.IMPROVEMENT);
        assertEquals(AlertStatus.IMPROVEMENT, measurement.getAlertStatus());
    }

    @Test
    public void testSlaViolation() {
        final MetricMeasurement measurement = new MetricMeasurement(1.0, 3.0, 1.0, 8.0,
            AlertMetricId.fromString("metric"), "test", "class");
        measurement.setAlertStatus(AlertStatus.SLA_VIOLATION);
        assertEquals(AlertStatus.SLA_VIOLATION, measurement.getAlertStatus());
    }
}