package com.vmturbo.plan.orchestrator.plan;

/**
 * Exception to throw when some referential integrity checks failed.
 */
public class IntegrityException extends Exception {
    public IntegrityException(String message) {
        super(message);
    }
}
