package com.vmturbo.mediation.actionscript.executor;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.sdk.probe.IProgressTracker;

/**
 * This class is used as a progress tracker in unit tests. In addition to implementing the
 * {@link IProgressTracker} interface, it provides getters for all the components of a progress
 * update
 */
class TestProgressTracker implements IProgressTracker {
    private ActionResponseState state = null;
    private String description = null;
    private int percentage = 0;

    @Override
    public void updateActionProgress(@Nonnull final ActionResponseState actionResponseState, @Nonnull final String s, final int i) {
        this.state = actionResponseState;
        this.description = s;
        this.percentage = i;

    }

    ActionResponseState getState() {
        return state;
    }

    String getDescription() {
        return description;
    }

    int getPercentage() {
        return percentage;
    }
}
