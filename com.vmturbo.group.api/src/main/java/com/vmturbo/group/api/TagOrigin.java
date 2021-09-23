package com.vmturbo.group.api;

/**
 * The origin represents how the tag came to be in the system.
 */
public enum TagOrigin {
    /**
     * Explicitly created by the user.
     */
    USER_CREATED,
    /**
     * Was imported and interpreted as part of a discovery.
     */
    DISCOVERED
}
