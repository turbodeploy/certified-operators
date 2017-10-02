package com.vmturbo.components.test.utilities.alert;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.components.test.utilities.alert.AlertManager.AlertAnnotationsForTest;
import com.vmturbo.components.test.utilities.alert.AlertRule.MetricEvaluationResult;
import com.vmturbo.components.test.utilities.alert.jira.AlertProcessorForJira;
import com.vmturbo.components.test.utilities.alert.jira.JiraCommunicator;
import com.vmturbo.components.test.utilities.alert.report.ResultsTabulator;

/**
 * JUnit {@link RunListener} that detects {@link Alert} annotations in running tests,
 * and is responsible for evaluating whether to trigger the alert, and actually triggering it.
 */
public class PerformanceAlertProcessor extends RunListener {

    private final Logger logger = LogManager.getLogger();

    /**
     * The {@link AlertManager} is responsible for actually evaluating whether or not to trigger
     * the alert.
     */
    private final AlertManager alertManager;

    /**
     * The {@link AlertProcessorForJira} is used to process alerts and create or update jira issues
     * related to performance regressions.
     */
    private final AlertProcessorForJira alertProcessorForJira;

    /**
     * A tabulator for use in processing the results of evaluating all rules
     * registered with the {@link AlertManager}.
     */
    private final ResultsTabulator resultsTabulator;

    @VisibleForTesting
    PerformanceAlertProcessor(@Nonnull final AlertManager alertManager,
                              @Nonnull final AlertProcessorForJira alertProcessorForJira,
                              @Nonnull final ResultsTabulator resultsTabulator) {
        this.alertManager = Objects.requireNonNull(alertManager);
        this.alertProcessorForJira = Objects.requireNonNull(alertProcessorForJira);
        this.resultsTabulator = Objects.requireNonNull(resultsTabulator);
    }

    public PerformanceAlertProcessor() {
        this(new AlertManager(), new AlertProcessorForJira(new JiraCommunicator()), new ResultsTabulator());
    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        try {
            logger.info("Evaluating whether to trigger alerts...");
            final List<MetricEvaluationResult> alertResults = alertManager.evaluateAlerts();
            final List<TriggeredAlert> triggeredAlerts = alertResults.stream()
                .map(MetricEvaluationResult::getAlert)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

            logger.info(
                resultsTabulator.renderTestResults(alertResults.stream()
                    .map(MetricEvaluationResult::getMeasurement)));

            if (triggeredAlerts.isEmpty()) {
                logger.info("No alerts triggered.");
            } else {
                triggeredAlerts.forEach(logger::error);
                alertProcessorForJira.processAlerts(triggeredAlerts);
            }
        } catch (RuntimeException e) {
            logger.error("Encountered error when evaluating alerts.", e);
        }
    }

    @Override
    public void testStarted(Description description) throws Exception {
        try {
            final Alert testAlert = description.getAnnotation(Alert.class);
            final Alert parentAlert = description.getTestClass().getAnnotation(Alert.class);

            alertManager.addAlert(description.getDisplayName(),
                    description.getMethodName(),
                    description.getTestClass().getSimpleName(),
                    new AlertAnnotationsForTest(testAlert, parentAlert));
        } catch (RuntimeException e) {
            logger.error("Encountered error adding an alert for test "
                    + description.getDisplayName() + " to the alert manager.", e);
        }
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        alertManager.removeAlert(failure.getDescription().getDisplayName());
    }

    @Override
    public void testAssumptionFailure(Failure failure) {
        alertManager.removeAlert(failure.getDescription().getDisplayName());
    }
}
