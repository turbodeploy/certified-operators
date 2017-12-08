package com.vmturbo.group.persistent;

import javax.annotation.Nonnull;

/**
 * Thrown when the user of {@link SettingStore} attempts to create a setting policy with
 * a name that is already taken.
 */
public class DuplicateNameException extends Exception {
    public DuplicateNameException(final long existingId, @Nonnull final String name) {
        super("A setting policy " + existingId + " already has name " + name + "!");
    }

    public DuplicateNameException(@Nonnull final String name) {
        super("A setting with name: '" + name + "' already exists");
    }
}
