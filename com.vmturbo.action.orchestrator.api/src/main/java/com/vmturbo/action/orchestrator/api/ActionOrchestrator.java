package com.vmturbo.action.orchestrator.api;

import javax.annotation.Nonnull;

/**
 * The client interface to a remote action orchestrator
 * component.
 */
public interface ActionOrchestrator extends AutoCloseable {

    /**
     * Registers a listener for action recommendations.
     *
     * @param listener Listener to register.
     */
    void addActionsListener(@Nonnull final ActionsListener listener);
}
