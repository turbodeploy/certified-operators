package com.vmturbo.components.test.utilities.alert.jira;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.components.test.utilities.alert.AlertMetricId;
import com.vmturbo.components.test.utilities.alert.AlertRule;
import com.vmturbo.components.test.utilities.alert.AlertRule.Time;
import com.vmturbo.components.test.utilities.alert.MetricMeasurement;
import com.vmturbo.components.test.utilities.alert.MetricMeasurement.AlertStatus;
import com.vmturbo.components.test.utilities.alert.RuleSignature;
import com.vmturbo.components.test.utilities.alert.TriggeredAlert;
import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.Fields;
import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.JiraStatus;
import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.JiraTransition.Type;
import com.vmturbo.components.test.utilities.alert.jira.JiraIssue.Query;

public class AlertProcessorForJiraTest {

    private final JiraCommunicator jira = mock(JiraCommunicator.class);
    private AlertProcessorForJira alertHandler;
    private final TriggeredAlert alert = Mockito.mock(TriggeredAlert.class);
    private final TriggeredAlert otherAlert = Mockito.mock(TriggeredAlert.class);
    private final JiraIssue issue = JiraIssue.newBuilder().build();
    private final AlertRule alertRule = Mockito.mock(AlertRule.class);

    private static final String ISSUE_ID = "OM-12345";

    @Before
    public void setup() {
        System.setProperty(AlertProcessorForJira.CREATE_JIRA_ISSUES_PROPERTY, "true");
        MockitoAnnotations.initMocks(this);

        alertHandler = new AlertProcessorForJira(jira);
        when(alertRule.getBaselineTime()).thenReturn(new Time(20L, TimeUnit.DAYS));
        when(alertRule.getComparisonTime()).thenReturn(new Time(1L, TimeUnit.DAYS));
        when(alertRule.getStandardDeviations()).thenReturn(1.0);
    }

    @After
    public void teardown() {
        System.setProperty(AlertProcessorForJira.CREATE_JIRA_ISSUES_PROPERTY, "false");
    }

    @Captor
    private ArgumentCaptor<JiraIssue> issueCaptor;

    @Captor
    private ArgumentCaptor<String> commentCaptor;

    @Captor
    private ArgumentCaptor<Query> queryCaptor;

    @Test
    public void testEmptyAlerts() {
        alertHandler.processAlerts(Collections.emptyList());
        verify(jira, never()).exactSearch(any(JiraIssue.Query.class));
    }

    @Test
    public void testOnlyImprovementAlerts() {
        when(alert.isRegressionOrSlaViolation()).thenReturn(false);
        alertHandler.processAlerts(Collections.singletonList(alert));
        verify(jira, never()).exactSearch(any(JiraIssue.Query.class));
    }

    @Test
    public void testAlertWithDisabledJiraInteraction() {
        System.setProperty(AlertProcessorForJira.CREATE_JIRA_ISSUES_PROPERTY, "false");

        final AlertProcessorForJira alertHandler = new AlertProcessorForJira(jira);
        setupRegressionAlert(alert, "SomePerformanceTest", "some_metric_name");

        alertHandler.processAlerts(Collections.singletonList(alert));

        verify(jira, never()).exactSearch(any(JiraIssue.Query.class));
        verify(jira, never()).createIssue(any());
        verify(jira, never()).commentOnIssue(anyString(), anyString());
    }

    @Test
    public void testAlertWithNoExistingIssueCreation() {
        setupRegressionAlert(alert, "SomePerformanceTest", "some_metric_name");
        when(jira.exactSearch(any(JiraIssue.Query.class))).thenReturn(Optional.<JiraIssue>empty());
        when(jira.createIssue(any())).thenReturn(issue);
        when(jira.commentOnIssue(anyString(), anyString())).thenReturn("foo");

        alertHandler.processAlerts(Collections.singletonList(alert));
        verify(jira).createIssue(issueCaptor.capture());

        final Fields fields = issueCaptor.getValue().getFields();
        assertEquals("Bug", fields.getIssuetype().getName());
        assertEquals("Operations Manager", fields.getProject().getName());
        assertEquals("P2", fields.getPriority().getName());
        assertEquals(fields.getLabels(), Collections.singletonList(AlertProcessorForJira.AUTOMATION_ISSUE_LABEL));
        assertEquals("XL", fields.getComponents().get(0).getName());
        assertThat(fields.getSummary(),
            containsString("Performance regression on SomePerformanceTest (metric: some_metric_name)"));
        assertThat(fields.getDescription(),
            containsString("The XL Performance testing framework has detected a performance regression"));
    }

    @Test
    public void testAlertWithNoExistingComment() {
        setupRegressionAlert(alert, "SomePerformanceTest", "some_metric_name");
        when(jira.exactSearch(any(JiraIssue.Query.class))
        ).thenReturn(Optional.empty());

        when(jira.createIssue(any())).thenReturn(makeOpenIssue(ISSUE_ID));

        alertHandler.processAlerts(Collections.singletonList(alert));
        verify(jira).commentOnIssue(eq(ISSUE_ID), commentCaptor.capture());

        assertThat(commentCaptor.getValue(), containsString("some_metric_name"));
    }

    @Test
    public void testAlertWithOpenIssue() {
        setupRegressionAlert(alert, "SomePerformanceTest", "some_metric_name");
        final JiraIssue issue = makeOpenIssue(ISSUE_ID);
        when(jira.exactSearch(any(JiraIssue.Query.class)))
            .thenReturn(Optional.of(issue));

        alertHandler.processAlerts(Collections.singletonList(alert));
        verify(jira, never()).transitionIssue(anyString(), any());
    }

    @Test
    public void testMultipleAlertsSameClass() {
        setupRegressionAlert(alert, "SomePerformanceTest", "some_metric_name");
        setupRegressionAlert(otherAlert, "SomePerformanceTest", "some_metric_name");
        final JiraIssue issue = makeOpenIssue(ISSUE_ID);

        when(jira.exactSearch(any(JiraIssue.Query.class)))
            .thenReturn(Optional.of(issue));

        alertHandler.processAlerts(Arrays.asList(alert, otherAlert));
        verify(jira).commentOnIssue(eq(ISSUE_ID), commentCaptor.capture());

        assertThat(commentCaptor.getValue(), containsString("some_metric_name"));
    }

    @Test
    public void testMultipleAlertsDifferentClass() {
        setupRegressionAlert(alert, "SomePerformanceTest", "some_metric_name");
        setupRegressionAlert(otherAlert, "OtherPerformanceTest", "other_metric_name");
        final JiraIssue issue = makeOpenIssue(ISSUE_ID);
        final JiraIssue otherIssue = makeOpenIssue("OM-other");

        when(jira.exactSearch(argThat(matchesQueryForTestClass("SomePerformanceTest"))))
            .thenReturn(Optional.of(issue));
        when(jira.exactSearch(argThat(matchesQueryForTestClass("OtherPerformanceTest"))))
            .thenReturn(Optional.of(otherIssue));

        alertHandler.processAlerts(Arrays.asList(alert, otherAlert));
        verify(jira, times(2)).commentOnIssue(anyString(), commentCaptor.capture());

        final List<String> capturedValues = commentCaptor.getAllValues();
        final String first = capturedValues.get(0);
        final String second = capturedValues.get(1);
        assertTrue((first.contains("some_metric_name") && second.contains("other_metric_name")) ||
                (second.contains("some_metric_name") && first.contains("other_metric_name")));
    }

    @Test
    public void testAlertWithClosedIssue() {
        setupRegressionAlert(alert, "SomePerformanceTest", "some_metric_name");
        final JiraIssue issue = makeOpenIssue(ISSUE_ID);

        when(jira.exactSearch(any(JiraIssue.Query.class)))
            .thenReturn(Optional.of(issue));

        alertHandler.processAlerts(Collections.singletonList(alert));
        verify(jira, never()).transitionIssue(eq(ISSUE_ID), eq(Type.REOPEN));
        verify(jira).commentOnIssue(eq(ISSUE_ID), commentCaptor.capture());

        assertThat(commentCaptor.getValue(), containsString("SomePerformanceTest (metric: some_metric_name)"));
    }

    @Test
    public void testAlertWithTags() {
        setupRegressionAlert(alert, "SomePerformanceTest", "ao_populate_store_duration_seconds_sum{store_type='Live'}");
        final RuleSignature signature = new RuleSignature(alert.getTestClassName(), alert.getMetric());
        final String summary = alertHandler.summaryFor(signature);

        assertThat(
            summary,
            containsString("ao_populate_store_duration_seconds_sum(store_type='Live')")
        );
    }

    private void setupRegressionAlert(@Nonnull final TriggeredAlert alert,
                                      @Nonnull final String className,
                                      @Nonnull final String metricName) {
        final MetricMeasurement measurement = new MetricMeasurement(4.0, 2.0, 1.0, 5.0,
            AlertMetricId.fromString(metricName), "someTestName", className);

        when(alert.isRegressionOrSlaViolation()).thenReturn(true);
        when(alert.getAlertStatus()).thenReturn(AlertStatus.REGRESSION);
        when(alert.getTestName()).thenReturn("someTestName");
        when(alert.getTestClassName()).thenReturn(className);
        when(alert.getMetric()).thenReturn(AlertMetricId.fromString(metricName));
        when(alert.getMeasurement()).thenReturn(measurement);
        when(alert.getRule()).thenReturn(alertRule);
    }

    private JiraIssue makeOpenIssue(@Nonnull final String issueId) {
        final JiraIssue issue = JiraIssue.newBuilder().build();
        issue.setKey(issueId);

        final JiraStatus status = new JiraStatus();
        status.setName("Assigned");
        issue.getFields().setStatus(status);

        return issue;
    }

    private ArgumentMatcher<Query> matchesQueryForTestClass(@Nonnull final String testClassName) {
        return o -> {
            if (!(o instanceof Query)) {
                return false;
            }
            final Query query = (Query)o;
            return query.getSummarySearch().get().contains(testClassName);
        };
    }
}