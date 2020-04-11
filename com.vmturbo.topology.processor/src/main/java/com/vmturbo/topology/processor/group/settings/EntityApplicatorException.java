package com.vmturbo.topology.processor.group.settings;

import javax.annotation.Nonnull;

/**
 * General exception for entity applicators.
 */
public class EntityApplicatorException extends Exception {

    private static final long serialVersionUID = 1998324619443250217L;

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public EntityApplicatorException(@Nonnull String message) {
        super(message);
    }
}
