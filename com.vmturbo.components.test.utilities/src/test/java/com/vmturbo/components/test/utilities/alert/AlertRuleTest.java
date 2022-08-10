package com.vmturbo.components.test.utilities.alert;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.components.test.utilities.alert.AlertRule.MetricEvaluationResult;
import com.vmturbo.components.test.utilities.alert.MetricMeasurement.AlertStatus;
import com.vmturbo.components.test.utilities.alert.MetricNameSuggestor.Suggestion;

public class AlertRuleTest {

    private static final String METRIC_STR = "metric";
    private static final AlertMetricId METRIC = AlertMetricId.fromString(METRIC_STR);
    private static final String TEST_NAME = "test";
    private static final String TEST_CLASS_NAME = "testClass";

    private static final long COMPARISON_MS = 1;
    private static final long BASELINE_MS = 7;
    private static final long STD_DEVIATIONS = 2;

    private static final String SLA_THRESHOLD_DESCRIPTION = "11.5s";
    private static final AlertMetricId SLA_METRIC =
        AlertMetricId.fromString(METRIC_STR + "/" + SLA_THRESHOLD_DESCRIPTION);

    private final AlertRule alertRule = AlertRule.newBuilder()
            .setBaselineTime(BASELINE_MS, TimeUnit.MILLISECONDS)
            .setComparisonTime(COMPARISON_MS, TimeUnit.MILLISECONDS)
            .setStandardDeviations(STD_DEVIATIONS)
            .build();

    @Test
    public void testEvaluateNoAlert() {
        final MetricsStore store = mock(MetricsStore.class);

        when(store.getStandardDeviation(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS)))
                .thenReturn(Optional.of(1.0));

        when(store.getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS)))
                .thenReturn(10.0);

        when(store.getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(COMPARISON_MS)))
                .thenReturn(9.0);

        final MetricEvaluationResult result = alertRule.evaluate(METRIC, TEST_NAME, TEST_CLASS_NAME, store).get();
        assertFalse(result.getAlert().isPresent());
        assertEquals(AlertStatus.NORMAL, result.getMeasurement().getAlertStatus());

        verify(store).getStandardDeviation(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS));
        verify(store).getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS));
        verify(store).getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(COMPARISON_MS));
    }

    @Test
    public void testEvaluateDecrease() {
        final MetricsStore store = mock(MetricsStore.class);

        when(store.getStandardDeviation(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS)))
                .thenReturn(Optional.of(1.0));

        when(store.getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS)))
                .thenReturn(10.0);

        when(store.getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(COMPARISON_MS)))
                .thenReturn(7.5);

        final MetricEvaluationResult result = alertRule.evaluate(METRIC, TEST_NAME, TEST_CLASS_NAME, store).get();
        final TriggeredAlert alert = result.getAlert().get();

        assertEquals(TEST_NAME, alert.getTestName());
        assertFalse(alert.isRegressionOrSlaViolation());
        assertEquals(alertRule, alert.getRule());
        assertEquals(AlertStatus.IMPROVEMENT, result.getMeasurement().getAlertStatus());

        verify(store).getStandardDeviation(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS));
        verify(store).getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS));
        verify(store).getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(COMPARISON_MS));
        verifyDescriptionHasKeywords(alertRule, alert,
            METRIC_STR,
            Long.toString(BASELINE_MS),
            "decreased",
            "milliseconds",
            Long.toString(COMPARISON_MS),
            Double.toString(STD_DEVIATIONS));
    }

    @Test
    public void testEvaluateIncrease() {
        final MetricsStore store = mock(MetricsStore.class);

        when(store.getStandardDeviation(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS)))
                .thenReturn(Optional.of(1.0));
        when(store.getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS)))
                .thenReturn(10.0);
        when(store.getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(COMPARISON_MS)))
                .thenReturn(12.1);

        final MetricEvaluationResult result = alertRule.evaluate(METRIC, TEST_NAME, TEST_CLASS_NAME, store).get();
        final TriggeredAlert alert = result.getAlert().get();

        assertEquals(TEST_NAME, alert.getTestName());
        assertTrue(alert.isRegressionOrSlaViolation());
        assertEquals(alertRule, alert.getRule());
        assertEquals(AlertStatus.REGRESSION, result.getMeasurement().getAlertStatus());

        verify(store).getStandardDeviation(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS));
        verify(store).getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS));
        verify(store).getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(COMPARISON_MS));
        verifyDescriptionHasKeywords(alertRule, alert,
            METRIC_STR,
            Long.toString(BASELINE_MS),
            "increased",
            "milliseconds",
            Long.toString(COMPARISON_MS),
            Double.toString(STD_DEVIATIONS));
    }

    @Test
    public void testEvaluateSlaViolation() {
        final MetricsStore store = Mockito.mock(MetricsStore.class);

        when(store.getStandardDeviation(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS)))
            .thenReturn(Optional.of(1.0));
        when(store.getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS)))
            .thenReturn(10.0);
        when(store.getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(COMPARISON_MS)))
            .thenReturn(12.1);

        final MetricEvaluationResult result = alertRule.evaluate(SLA_METRIC, TEST_NAME, TEST_CLASS_NAME, store).get();
        final TriggeredAlert alert = result.getAlert().get();

        assertEquals(TEST_NAME, alert.getTestName());
        assertTrue(alert.isRegressionOrSlaViolation());
        assertEquals(alertRule, alert.getRule());
        assertEquals(AlertStatus.SLA_VIOLATION, result.getMeasurement().getAlertStatus());

        verify(store).getStandardDeviation(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS));
        verify(store).getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS));
        verify(store).getAvg(eq(TEST_NAME), eq(METRIC_STR), any(), eq(COMPARISON_MS));
        verifyDescriptionHasKeywords(alertRule, alert,
            METRIC_STR,
            "is in violation of its promised SLA of",
            SLA_THRESHOLD_DESCRIPTION
        );
    }

    @Test
    public void testStatsStoreException() {
        final MetricsStore store = mock(MetricsStore.class);
        when(store.getStandardDeviation(eq(TEST_NAME), eq(METRIC_STR), any(), eq(BASELINE_MS)))
           .thenThrow(new RuntimeException("Exception occurred"));
        assertFalse(alertRule.evaluate(METRIC, TEST_NAME, TEST_CLASS_NAME, store).isPresent());
    }

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testNegativeComparison() {
        expectedException.expect(IllegalArgumentException.class);
        AlertRule.newBuilder().setComparisonTime(-1, TimeUnit.DAYS);
    }

    @Test
    public void testZeroComparison() {
        expectedException.expect(IllegalArgumentException.class);
        AlertRule.newBuilder().setComparisonTime(0, TimeUnit.DAYS);
    }

    @Test
    public void testNegativeBaseline() {
        expectedException.expect(IllegalArgumentException.class);
        AlertRule.newBuilder().setBaselineTime(-1, TimeUnit.DAYS);
    }

    @Test
    public void testZeroBaseline() {
        expectedException.expect(IllegalArgumentException.class);
        AlertRule.newBuilder().setBaselineTime(0, TimeUnit.DAYS);
    }

    @Test
    public void testNegativeStdDiv() {
        expectedException.expect(IllegalArgumentException.class);
        AlertRule.newBuilder().setStandardDeviations(-1);
    }

    @Test
    public void testZeroStdDiv() {
        expectedException.expect(IllegalArgumentException.class);
        AlertRule.newBuilder().setStandardDeviations(0);
    }

    @Test
    public void testComparisonHigherThanBaseline() {
        expectedException.expect(IllegalArgumentException.class);
        AlertRule.newBuilder()
            .setComparisonTime(10, TimeUnit.DAYS)
            .setBaselineTime(9, TimeUnit.DAYS)
            .build();
    }

    @Test
    public void testFindSuggestionsWithPerfectMatch() {
        final AlertMetricId metric = AlertMetricId.fromString("foo{label='value'}");
        final MetricNameSuggestor suggestor = mock(MetricNameSuggestor.class);

        when(suggestor.computeSuggestions(eq(metric.getMetricName()), anyCollection()))
            .thenReturn(Arrays.asList(
                new Suggestion(0, "foo"),
                new Suggestion(3, "bar")
            ));

        assertTrue(alertRule.findSuggestionsForMistypedName(metric, Arrays.asList("foo", "bar"), suggestor).isEmpty());
    }

    @Test
    public void testFindSuggestionsWithoutPerfectMatch() {
        final AlertMetricId metric = AlertMetricId.fromString("foo{label='value'}");
        final MetricNameSuggestor suggestor = mock(MetricNameSuggestor.class);

        when(suggestor.computeSuggestions(eq(metric.getMetricName()), anyCollection()))
            .thenReturn(Arrays.asList(
                new Suggestion(1, "food"),
                new Suggestion(3, "bar")
            ));

        final List<String> suggestions = alertRule
            .findSuggestionsForMistypedName(metric, Arrays.asList("foo", "bar"), suggestor);

        assertThat(suggestions, contains("food", "bar"));
    }

    private void verifyDescriptionHasKeywords(@Nonnull final AlertRule alertRule,
                                              @Nonnull final TriggeredAlert triggeredAlert,
                                              @Nonnull final String... expectedKeywords) {
        final String ruleDescription = alertRule.describe(triggeredAlert.getMetric(), triggeredAlert.getAlertStatus());
        for (String keywords: expectedKeywords) {
            assertThat(ruleDescription, containsString(keywords));
        }
    }
}
