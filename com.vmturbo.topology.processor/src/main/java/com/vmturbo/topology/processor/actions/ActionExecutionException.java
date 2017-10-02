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

    public static ActionExecutionException noEntity(final String entityType, final long entityId) {
        return new ActionExecutionException("Missing " + entityType +
                " entity (ID " + entityId + ")");
    }

    public static ActionExecutionException noEntityTargetInfo(final String entityType, final long entityId, final long targetId) {
        return new ActionExecutionException("Missing target info for " + entityType +
                " entity (ID " + entityId + "). This means that target " + targetId +
                " has not discovered this entity.");

    }
}
