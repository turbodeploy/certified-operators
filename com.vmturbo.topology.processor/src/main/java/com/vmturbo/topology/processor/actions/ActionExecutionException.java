package com.vmturbo.topology.processor.actions;

import javax.annotation.Nonnull;

/**
 * Error encountered when executing an action.
 */
public class ActionExecutionException extends Exception {

    public ActionExecutionException(String message) {
        super(message);
    }

    public ActionExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

}
