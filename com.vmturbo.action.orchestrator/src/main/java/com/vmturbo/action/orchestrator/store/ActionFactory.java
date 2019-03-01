package com.vmturbo.action.orchestrator.store;

import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * A factory for creating actions.
 */
public class ActionFactory implements IActionFactory {

    public ActionFactory() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Action newAction(@Nonnull final ActionDTO.Action recommendation,
                            final long actionPlanId) {
        return new Action(recommendation, actionPlanId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Action newAction(@Nonnull final ActionDTO.Action recommendation,
                            final EntitySettingsCache entitySettingsCache,
                            final long actionPlanId) {
        return new Action(recommendation, entitySettingsCache, actionPlanId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Action newAction(@Nonnull final ActionDTO.Action recommendation,
                            @Nonnull final LocalDateTime recommendationTime,
                            final long actionPlanId) {
        return new Action(recommendation, recommendationTime, actionPlanId);
    }
}
