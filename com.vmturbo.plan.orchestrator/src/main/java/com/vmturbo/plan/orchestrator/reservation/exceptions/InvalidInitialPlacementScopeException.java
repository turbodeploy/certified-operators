package com.vmturbo.plan.orchestrator.reservation.exceptions;

/**
 * Throw exception when the initial placement scope is not valid.
 */
public class InvalidInitialPlacementScopeException extends Exception {
    /**
     * Invalid Initial Placement Scope Exception.
     * @param message error message
     */
    public InvalidInitialPlacementScopeException(String message) {
            super(message);
    }
}
