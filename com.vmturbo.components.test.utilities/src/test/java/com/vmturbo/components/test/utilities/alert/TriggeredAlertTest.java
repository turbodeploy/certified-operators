package com.vmturbo.components.test.utilities.alert;

import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.METRIC_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.components.test.utilities.alert.AlertTestUtil.TestRuleRegistry;
import com.vmturbo.components.test.utilities.alert.MetricMeasurement.AlertStatus;

public class TriggeredAlertTest {

    private static final String METHOD = "testName";
    private static final String TEST_CLASS_NAME = "testClassName";

    private final MetricMeasurement measurement = spy(new MetricMeasurement(
        1.0, 2.0, 3.0, 4.0, AlertMetricId.fromString(METRIC_NAME), METHOD, TEST_CLASS_NAME));

    @Test
    public void testToString() {
        final AlertRule rule = mock(AlertRule.class);
        TriggeredAlert triggeredAlert = new TriggeredAlert(rule, measurement);
        when(measurement.getAlertStatus()).thenReturn(AlertStatus.REGRESSION);
        when(rule.describe(any(AlertMetricId.class), any(AlertStatus.class)))
            .thenReturn("foo");

        final String ruleString = triggeredAlert.toString();
        assertThat(ruleString, containsString(METHOD));
        assertThat(ruleString, containsString("foo"));
    }

    @Test
    public void testRegressionIsRegressionOrSlaViolation() throws Exception {
        final TriggeredAlert alert = new TriggeredAlert(
            new TestRuleRegistry().getAlertRule(METRIC_NAME), measurement);
        when(measurement.getAlertStatus()).thenReturn(AlertStatus.REGRESSION);
        assertTrue(alert.isRegressionOrSlaViolation());
    }

    @Test
    public void testSlaIsRegressionOrSlaViolation() throws Exception {
        final TriggeredAlert alert = new TriggeredAlert(
            new TestRuleRegistry().getAlertRule(METRIC_NAME), measurement);
        when(measurement.getAlertStatus()).thenReturn(AlertStatus.SLA_VIOLATION);
        assertTrue(alert.isRegressionOrSlaViolation());
    }

    @Test
    public void testNormalIsNotRegressionOrSlaViolation() throws Exception {
        final TriggeredAlert alert = new TriggeredAlert(
            new TestRuleRegistry().getAlertRule(METRIC_NAME), measurement);
        when(measurement.getAlertStatus()).thenReturn(AlertStatus.NORMAL);
        assertFalse(alert.isRegressionOrSlaViolation());
    }

    @Test
    public void testImprovementIsNotRegressionOrSlaViolation() throws Exception {
        final TriggeredAlert alert = new TriggeredAlert(
            new TestRuleRegistry().getAlertRule(METRIC_NAME), measurement);
        when(measurement.getAlertStatus()).thenReturn(AlertStatus.IMPROVEMENT);
        assertFalse(alert.isRegressionOrSlaViolation());
    }
}