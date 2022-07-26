package com.vmturbo.components.test.utilities.alert;

import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.CUSTOM_BASELINE_MS;
import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.CUSTOM_COMPARISON_MS;
import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.CUSTOM_STD_DEVIATIONS;
import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.METRIC;
import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.METRIC_NAME;
import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.METRIC_NAME_2;
import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.makeAlert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.components.test.utilities.alert.AlertManager.AlertAnnotationsForTest;
import com.vmturbo.components.test.utilities.alert.AlertRule.MetricEvaluationResult;
import com.vmturbo.components.test.utilities.alert.AlertRule.Time;
import com.vmturbo.components.test.utilities.alert.AlertTestUtil.TestRuleRegistry;
import com.vmturbo.components.test.utilities.alert.RuleRegistry.DefaultRuleRegistry;

public class AlertManagerTest {
    private static final String KEY = "key";
    private static final String METHOD = "testMethod";
    private static final String TEST_CLASS_NAME = "testClass";

    private MetricsStore metricsStore = Mockito.mock(MetricsStore.class);

    private AlertManager alertManager = new AlertManager(metricsStore);

    @Before
    public void setup() {
        // Set up the stats store so that the custom rules trigger an
        // alert.
        final double stdDev = 1.0;
        final double baseline = 20.0;
        final double comparison = baseline + (CUSTOM_STD_DEVIATIONS + 1) * stdDev;

        when(metricsStore.isAvailable()).thenReturn(true);
        when(metricsStore.getStandardDeviation(eq(METHOD), eq(METRIC_NAME), any(), eq(CUSTOM_BASELINE_MS)))
                .thenReturn(Optional.of(stdDev));
        when(metricsStore.getAvg(eq(METHOD), eq(METRIC_NAME), any(), eq(CUSTOM_BASELINE_MS)))
                .thenReturn(baseline);
        when(metricsStore.getAvg(eq(METHOD), eq(METRIC_NAME), any(), eq(CUSTOM_COMPARISON_MS)))
                .thenReturn(comparison);

        when(metricsStore.getStandardDeviation(eq(METHOD), eq(METRIC_NAME_2), any(), eq(CUSTOM_BASELINE_MS)))
                .thenReturn(Optional.of(stdDev));
        when(metricsStore.getAvg(eq(METHOD), eq(METRIC_NAME_2), any(), eq(CUSTOM_BASELINE_MS)))
                .thenReturn(baseline);
        when(metricsStore.getAvg(eq(METHOD), eq(METRIC_NAME_2), any(), eq(CUSTOM_COMPARISON_MS)))
                .thenReturn(comparison);
    }

    @Test
    public void testAlert() {
        alertManager.addAlert(KEY, METHOD, TEST_CLASS_NAME,
                new AlertAnnotationsForTest(makeAlert(METRIC_NAME), null));

        final List<TriggeredAlert> alertList = getTriggeredAlerts();
        assertEquals(1, alertList.size());
        TriggeredAlert alert = alertList.get(0);
        assertEquals(METHOD, alert.getTestName());
        assertEquals(METRIC, alert.getMetric());
        assertEquals(true, alert.isRegressionOrSlaViolation());

        AlertRule rule = alert.getRule();
        assertEquals(CUSTOM_BASELINE_MS, rule.getBaselineTime().millis());
        assertEquals(CUSTOM_COMPARISON_MS, rule.getComparisonTime().millis());
        assertEquals(CUSTOM_STD_DEVIATIONS, rule.getStandardDeviations(), 0);
    }

    @Test
    public void testMetricOnly() {
        alertManager.addAlert(KEY, METHOD, TEST_CLASS_NAME,
                new AlertAnnotationsForTest(makeAlert(new String[]{}, new String[]{METRIC_NAME}), null));

        final List<TriggeredAlert> alertList = getTriggeredAlerts();
        assertEquals(1, alertList.size());
    }

    @Test
    public void testValueOnly() {
        alertManager.addAlert(KEY, METHOD, TEST_CLASS_NAME,
                new AlertAnnotationsForTest(makeAlert(new String[]{METRIC_NAME}, new String[]{}), null));

        final List<TriggeredAlert> alertList = getTriggeredAlerts();
        assertEquals(1, alertList.size());
    }

    @Test
    public void testMetricsOverridesValue() {
        alertManager.addAlert(KEY, METHOD, TEST_CLASS_NAME, new AlertAnnotationsForTest(
                makeAlert(new String[]{"value"}, new String[]{METRIC_NAME}), null));

        final List<TriggeredAlert> alertList = getTriggeredAlerts();
        assertEquals(1, alertList.size());
    }

    @Test
    public void testParentClassMetricsAdded() {
        Alert testAlert = makeAlert(METRIC_NAME);
        Alert parentAlert = makeAlert(METRIC_NAME_2);

        final AlertAnnotationsForTest alerts = new AlertAnnotationsForTest(testAlert, parentAlert);
        final List<String> metrics = alerts.getMetricIds().stream()
            .map(AlertMetricId::getMetricName)
            .collect(Collectors.toList());
        assertThat(metrics, Matchers.containsInAnyOrder(METRIC_NAME, METRIC_NAME_2));
    }

    @Test
    public void testParentClassSameMetric() {
        Alert testAlert = makeAlert(METRIC_NAME);
        Alert parentAlert = makeAlert(METRIC_NAME);

        final AlertAnnotationsForTest alerts = new AlertAnnotationsForTest(testAlert, parentAlert);
        assertEquals(1, alerts.getMetricIds().size());
        assertEquals(METRIC_NAME, alerts.getMetricIds().iterator().next().getMetricName());
    }

    @Test
    public void testParentClassRegistryOverridesTestDefault() {
        Alert testAlert = makeAlert(DefaultRuleRegistry.class, METRIC_NAME);
        Alert parentAlert = makeAlert(TestRuleRegistry.class, METRIC_NAME);

        final AlertAnnotationsForTest alerts = new AlertAnnotationsForTest(testAlert, parentAlert);
        assertEquals(TestRuleRegistry.class, alerts.getRegistry());
    }

    @Test
    public void testTestRegistryOverridesParentClassDefault() {
        Alert testAlert = makeAlert(TestRuleRegistry.class, METRIC_NAME);
        Alert parentAlert = makeAlert(DefaultRuleRegistry.class, METRIC_NAME);

        final AlertAnnotationsForTest alerts = new AlertAnnotationsForTest(testAlert, parentAlert);
        assertEquals(TestRuleRegistry.class, alerts.getRegistry());
    }

    @Test
    public void testTestRegistryOverridesParentClassRegistry() {
        Alert testAlert = makeAlert(TestRuleRegistry.class, METRIC_NAME);
        Alert parentAlert = makeAlert(OtherRuleRegistry.class, METRIC_NAME);

        final AlertAnnotationsForTest alerts = new AlertAnnotationsForTest(testAlert, parentAlert);
        assertEquals(TestRuleRegistry.class, alerts.getRegistry());
    }

    @Test
    public void testNoMetrics() {
        Alert testAlert = makeAlert(DefaultRuleRegistry.class);
        Alert parentAlert = makeAlert(DefaultRuleRegistry.class);

        final AlertAnnotationsForTest alerts = new AlertAnnotationsForTest(testAlert, parentAlert);
        assertTrue(alerts.getMetricIds().isEmpty());
        assertEquals(DefaultRuleRegistry.class, alerts.getRegistry());
    }

    @Test
    public void testAllNull() {
        final AlertAnnotationsForTest alerts = new AlertAnnotationsForTest(null, null);
        assertTrue(alerts.getMetricIds().isEmpty());
        assertEquals(DefaultRuleRegistry.class, alerts.getRegistry());
    }

    @Test
    public void testRemoveAlert() {
        alertManager.addAlert(KEY, METHOD, TEST_CLASS_NAME,
                new AlertAnnotationsForTest(makeAlert("metric"), null));
        assertEquals(1, alertManager.numAlerts());
        alertManager.removeAlert(KEY + "blah");
        assertEquals(1, alertManager.numAlerts());
        alertManager.removeAlert(KEY);
        assertEquals(0, alertManager.numAlerts());
    }

    @Test
    public void testMetricsStoreUnavailable() {
        final MetricsStore metricsStore = Mockito.mock(MetricsStore.class);
        final AlertManager alertManager = new AlertManager(metricsStore);

        when(metricsStore.isAvailable()).thenReturn(false);
        final List<MetricEvaluationResult> results = alertManager.evaluateAlerts();
        assertTrue(results.isEmpty());
        verify(metricsStore, never()).getAvg(anyString(), anyString(), any(), anyLong());
    }

    @Test
    public void testTimeMillis() {
        final Time time = new Time(1, TimeUnit.DAYS);
        assertEquals(TimeUnit.DAYS.toMillis(1), time.millis());
    }

    @Test
    public void testTimeString() {
        final Time time = new Time(1, TimeUnit.DAYS);
        assertEquals("1 day", time.toString());
    }

    @Nonnull
    private List<TriggeredAlert> getTriggeredAlerts() {
        return alertManager.evaluateAlerts().stream()
                .map(MetricEvaluationResult::getAlert)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private static class OtherRuleRegistry extends RuleRegistry {
        @Override
        protected Optional<AlertRule> overrideAlertRule(@Nonnull final String metricName) {
            return Optional.empty();
        }
    }
}
