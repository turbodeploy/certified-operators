package com.vmturbo.group.persistent;

import com.google.common.annotations.VisibleForTesting;

/**
 * Exception thrown from the {@link SettingStore} to indicate that a setting policy
 * does not exist.
 */
public class SettingPolicyNotFoundException extends Exception {

    @VisibleForTesting
    public SettingPolicyNotFoundException(final long id) {
        super("Setting policy " + id + " was not found!");
    }
}
