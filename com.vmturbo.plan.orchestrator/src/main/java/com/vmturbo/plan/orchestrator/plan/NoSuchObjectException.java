package com.vmturbo.plan.orchestrator.plan;

/**
 * Exception to be thown when operation requested on an object, which is not present.
 */
public class NoSuchObjectException extends Exception {
    public NoSuchObjectException(String message) {
        super(message);
    }
}
