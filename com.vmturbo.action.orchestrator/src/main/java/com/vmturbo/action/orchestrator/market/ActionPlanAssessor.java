package com.vmturbo.action.orchestrator.market;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;

/**
 * A small helper class to assess action plans to decide if they should be processed or dropped.
 */
public class ActionPlanAssessor {
    private final long liveTopologyContextId;

    private final Clock clock;

    private final long maxLiveActionPlanAgeSeconds;

    /**
     * Create a new action plan assessor.
     *
     * @param clock The clock to use to generate the current time. Used to determine the age of
     *              an action plan.
     * @param realtimeTopologyContextId The canonical live topologyContextId.
     * @param maxLiveActionPlanAgeSeconds The maximum age of an action plan in seconds. Action plans older than
     *                                    this maximum should be dropped.
     */
    public ActionPlanAssessor(@Nonnull final Clock clock,
                              final long realtimeTopologyContextId,
                              final long maxLiveActionPlanAgeSeconds) {
        Preconditions.checkArgument(maxLiveActionPlanAgeSeconds >= 0);

        this.clock = Objects.requireNonNull(clock);
        this.liveTopologyContextId = realtimeTopologyContextId;
        this.maxLiveActionPlanAgeSeconds = maxLiveActionPlanAgeSeconds;
    }

    /**
     * Assess an action plan to decide if an action plan is expired.
     *
     * @param orderedActions The action plan to assess.
     * @return True if the action plan should be processed, false if it should be dropped.
     */
    public boolean isActionPlanExpired(@Nonnull final ActionPlan orderedActions) {
        if (isLiveActionPlan(orderedActions)) {
            // Drop live action plans older than the maximum age.
            final long secondsSinceAnalysisCompletion = Instant
                .ofEpochMilli(orderedActions.getAnalysisCompleteTimestamp())
                .until(clock.instant(), ChronoUnit.SECONDS);

            return secondsSinceAnalysisCompletion > maxLiveActionPlanAgeSeconds;
        } else {
            // Plan action plans are never considered expired because they are not
            // time-sensitive in the way that live ones are.
            return false;
        }
    }

    /**
     * Decide if an action plan is a live action plan.
     *
     * @param orderedActions The action plan to assess.
     * @return True if the action plan is due to a live analysis, false if it is not.
     */
    private boolean isLiveActionPlan(@Nonnull final ActionPlan orderedActions) {
        return orderedActions.getTopologyContextId() ==  liveTopologyContextId;
    }
}
