package com.vmturbo.plan.orchestrator.plan;

/**
 * Exception to be thrown when operation on discovered template or deployment profile are not allowed.
 */
public class DiscoveredNotSupportedOperationException extends Exception {
    public DiscoveredNotSupportedOperationException(String message) {
        super(message);
    }
}