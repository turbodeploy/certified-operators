package com.vmturbo.action.orchestrator.store;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * A factory for creating actions.
 */
public class ActionFactory implements IActionFactory {

    private final ActionModeCalculator actionModeCalculator;
    public ActionFactory(ActionModeCalculator actionModeCalculator) {
        this.actionModeCalculator = actionModeCalculator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Action newAction(@Nonnull final ActionDTO.Action recommendation,
                            final long actionPlanId) {
        return new Action(recommendation, actionPlanId, actionModeCalculator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Action newAction(@Nonnull final ActionDTO.Action recommendation,
                            final EntitiesCache entitySettingsCache,
                            final long actionPlanId) {
        return new Action(recommendation, entitySettingsCache, actionPlanId, actionModeCalculator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Action newAction(@Nonnull final ActionDTO.Action recommendation,
                            @Nonnull final LocalDateTime recommendationTime,
                            final long actionPlanId) {
        return new Action(recommendation, recommendationTime, actionPlanId, actionModeCalculator);
    }
}
