package com.vmturbo.components.test.utilities.alert;

import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.makeAlert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.mockito.ArgumentCaptor;

import com.vmturbo.components.test.utilities.alert.AlertManager.AlertAnnotationsForTest;
import com.vmturbo.components.test.utilities.alert.AlertRule.MetricEvaluationResult;
import com.vmturbo.components.test.utilities.alert.jira.AlertProcessorForJira;
import com.vmturbo.components.test.utilities.alert.report.ResultsTabulator;

public class PerformanceAlertProcessorTest {
    private static final String KEY = "key";
    private static final String METHOD = "testMethod";

    private AlertManager alertManager = mock(AlertManager.class);

    private AlertProcessorForJira alertProcessorForJira = mock(AlertProcessorForJira.class);

    private ResultsTabulator tabulator = mock(ResultsTabulator.class);

    private PerformanceAlertProcessor listener = new PerformanceAlertProcessor(alertManager,
        alertProcessorForJira, tabulator);

    @Test
    public void testDefaultConstructor() {
        // Test that the default constructor doesn't crash...
        new PerformanceAlertProcessor();
    }

    private static class Foo {}

    @Test
    public void testStartedAddsAlert() throws Exception {
        final Alert alert = makeAlert("metric");

        final Description description = mock(Description.class);
        when(description.getAnnotation(eq(Alert.class))).thenReturn(alert);
        when(description.getDisplayName()).thenReturn(KEY);
        when(description.getMethodName()).thenReturn(METHOD);
        doReturn(Foo.class).when(description).getTestClass();

        listener.testStarted(description);

        final ArgumentCaptor<AlertAnnotationsForTest> alertsCaptor =
                ArgumentCaptor.forClass(AlertAnnotationsForTest.class);
        verify(alertManager).addAlert(eq(KEY), eq(METHOD), eq("Foo"), alertsCaptor.capture());

        final AlertAnnotationsForTest gotAlert = alertsCaptor.getValue();
        assertEquals(alert, gotAlert.getTestAlert().get());
        assertFalse(gotAlert.getTestClassAlert().isPresent());
    }

    @Alert("test")
    private static class ClassAlert {}

    @Test
    public void testStartedAddsClassAlert() throws Exception {
        final Alert alert = makeAlert("metric");

        final Description description = mock(Description.class);
        when(description.getAnnotation(eq(Alert.class))).thenReturn(alert);
        when(description.getDisplayName()).thenReturn(KEY);
        when(description.getMethodName()).thenReturn(METHOD);

        doReturn(ClassAlert.class).when(description).getTestClass();

        listener.testStarted(description);

        final ArgumentCaptor<AlertAnnotationsForTest> alertsCaptor =
                ArgumentCaptor.forClass(AlertAnnotationsForTest.class);
        verify(alertManager).addAlert(eq(KEY), eq(METHOD), eq("ClassAlert"),
                alertsCaptor.capture());

        final AlertAnnotationsForTest gotAlerts = alertsCaptor.getValue();
        assertEquals(alert, gotAlerts.getTestAlert().get());
        assertEquals(ClassAlert.class.getAnnotation(Alert.class),
                gotAlerts.getTestClassAlert().get());
    }

    @Test
    public void testFailureRemovesAlert() throws Exception {
        Failure failure = mock(Failure.class);
        Description description = mock(Description.class);
        when(description.getDisplayName()).thenReturn(KEY);
        when(failure.getDescription()).thenReturn(description);
        doReturn(Foo.class).when(description).getTestClass();

        listener.testFailure(failure);

        verify(alertManager).removeAlert(eq(KEY));
    }

    @Test
    public void testAssumptionFailureRemovesAlert() throws Exception {
        Failure failure = mock(Failure.class);
        Description description = mock(Description.class);
        when(description.getDisplayName()).thenReturn(KEY);
        doReturn(Foo.class).when(description).getTestClass();
        when(failure.getDescription()).thenReturn(description);

        listener.testAssumptionFailure(failure);

        verify(alertManager).removeAlert(eq(KEY));
    }

    @Test
    public void testRunFinishedEvaluatesAlerts() throws Exception {
        final MetricMeasurement measurement = mock(MetricMeasurement.class);
        final TriggeredAlert triggeredAlert = new TriggeredAlert(AlertRule.newBuilder().build(), measurement);
        final MetricEvaluationResult result = new MetricEvaluationResult(measurement, Optional.of(triggeredAlert));

        when(alertManager.evaluateAlerts()).thenReturn(Collections.singletonList(result));
        when(tabulator.renderTestResults(any(Stream.class))).thenReturn("");

        listener.testRunFinished(mock(Result.class));
        verify(tabulator).renderTestResults(any(Stream.class));
        verify(alertManager).evaluateAlerts();
        verify(alertProcessorForJira).processAlerts(Collections.singletonList(triggeredAlert));
    }

    /**
     * Test that exceptions from alert manager don't propagate outside of
     * the listener.
     */
    @Test
    public void testEvaluateAlertException() throws Exception {
        when(alertManager.evaluateAlerts()).thenThrow(new RuntimeException("BAD!"));
        listener.testRunFinished(mock(Result.class));
    }

    @Test
    public void testAddAlertException() throws Exception {
        final Alert alert = makeAlert("metric");

        final Description description = mock(Description.class);
        when(description.getAnnotations()).thenReturn(
                Collections.singletonList(alert));
        when(description.getDisplayName()).thenReturn(KEY);
        when(description.getMethodName()).thenReturn(METHOD);

        doThrow(new RuntimeException("BAD!")).when(alertManager).addAlert(any(), any(), any(), any());
        listener.testStarted(description);
    }
}
