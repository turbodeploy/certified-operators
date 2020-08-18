package com.vmturbo.plan.orchestrator.plan;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;

/**
 *  Listen to plan status changes to expose Prometheus metrics about things like plan run times.
 */
public class PlanMetricsListener implements PlanStatusListener {
    private static final Logger logger = LogManager.getLogger();

    private static final String DELETED_STATUS = "DELETED";

    private static final DataMetricHistogram PLAN_RUNTIME_HISTOGRAM = DataMetricHistogram.builder()
        .withName("plan_run_time_seconds")
        .withHelp("Runtime for plan execution in seconds. " +
            "This time includes both time spent queued and in execution.")
        .withBuckets(0.01, 0.05, 0.2, 1, 3, 10, 30, 60, 300)
        .withLabelNames("status", "plan_type")
        .build()
        .register();

    /**
     * Record plans that are run and their status.
     */
    private static final DataMetricCounter PLAN_TYPES_COUNTER = DataMetricCounter.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "plans_total")
            .withHelp("Counter for Plans that have been run.")
            .withLabelNames("type", "status")
            .build()
            .register();

    @Override
    public void onPlanStatusChanged(@Nonnull final PlanInstance plan) throws PlanStatusListenerException {
        if (plan.getStatus().equals(PlanStatus.SUCCEEDED) ||
            plan.getStatus().equals(PlanStatus.FAILED) ||
            plan.getStatus().equals(PlanStatus.STOPPED)) {

            recordCompletedPlanMetrics(plan);
        }
    }

    @Override
    public void onPlanDeleted(@Nonnull final PlanInstance plan) throws PlanStatusListenerException {
        try {
            final double runtimeSeconds = (plan.getEndTime() - plan.getStartTime()) * 0.001;

            PLAN_RUNTIME_HISTOGRAM
                .labels(plan.getStatus().name(), plan.getScenario().getScenarioInfo().getType())
                .observe(runtimeSeconds);
            PLAN_TYPES_COUNTER
                .labels(plan.getScenario().getScenarioInfo().getType(), DELETED_STATUS).increment();
        } catch (Exception e) {
            logger.error("Encountered exception while trying to log plan metrics: ", e);
        }
    }

    private void recordCompletedPlanMetrics(@Nonnull final PlanInstance plan) {
        try {
            final double runtimeSeconds = (plan.getEndTime() - plan.getStartTime()) * 0.001;

            PLAN_RUNTIME_HISTOGRAM
                .labels(plan.getStatus().name(), plan.getScenario().getScenarioInfo().getType())
                .observe(runtimeSeconds);
            PLAN_TYPES_COUNTER.labels(plan.getScenario().getScenarioInfo().getType(),
                    plan.getStatus().name()).increment();
        } catch (Exception e) {
            logger.error("Encountered exception while trying to log plan metrics: ", e);
        }
    }
}