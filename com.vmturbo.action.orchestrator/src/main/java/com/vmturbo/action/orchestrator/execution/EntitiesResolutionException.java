package com.vmturbo.action.orchestrator.execution;

/**
 * Exception thrown when entities could not be resolved from TP.
 */
public class EntitiesResolutionException extends Exception {

    /**
     * Constructs the exception
     *
     * @param message message to specify
     */
    public EntitiesResolutionException(final String message) {
        super(message);
    }
}
