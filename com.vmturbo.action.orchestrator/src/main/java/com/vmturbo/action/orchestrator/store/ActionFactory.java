package com.vmturbo.action.orchestrator.store;

import java.time.LocalDateTime;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionStateMachine;
import com.vmturbo.action.orchestrator.action.ActionStateMachine.ActionEventListener;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * A factory for creating actions.
 */
public class ActionFactory implements IActionFactory {

    private final ActionModeCalculator actionModeCalculator;
    private final List<ActionStateMachine.ActionEventListener> actionEventListeners;

    public ActionFactory(
            @Nonnull ActionModeCalculator actionModeCalculator,
            @Nonnull List<ActionEventListener> actionEventListeners) {
        this.actionModeCalculator = actionModeCalculator;
        this.actionEventListeners = actionEventListeners;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Action newAction(@Nonnull final ActionDTO.Action recommendation,
                            final long actionPlanId, long recommendationOid) {
        return new Action(recommendation, actionPlanId, actionModeCalculator, recommendationOid,
                actionEventListeners);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Action newPlanAction(@Nonnull final ActionDTO.Action recommendation,
                                @Nonnull final LocalDateTime recommendationTime,
                                final long actionPlanId,
                                String description,
                                @Nullable final Long associatedAccountId,
                                @Nullable final Long associatedResourceGroupId) {
        return new Action(recommendation, recommendationTime, actionPlanId,
            actionModeCalculator, description, associatedAccountId, associatedResourceGroupId,
                IdentityGenerator.next(),
                actionEventListeners);
    }
}
