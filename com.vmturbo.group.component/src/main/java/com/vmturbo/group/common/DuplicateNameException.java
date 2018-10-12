package com.vmturbo.group.common;

import javax.annotation.Nonnull;

/**
 * An exception to indicate that another object of the same name exists.
 */
public class DuplicateNameException extends Exception {
    public DuplicateNameException(final long existingId, @Nonnull final String name, final int type) {
        super("Cannot create object with name " + name
                + " and type " + type + " because an object with the same name and type (id: "
                + existingId + ") already exists.");
    }

    public DuplicateNameException(final long existingId, @Nonnull final String name) {
        super("Cannot create object with name " + name
                + " because an object with the same name and type (id: "
                + existingId + ") already exists.");
    }
}
