package com.vmturbo.topology.processor.communication;

/**
 * A map value that knows the time at which it will expire.
 */
public interface ExpiringValue {
    /**
     * Determine the expiration time for the given value.
     *
     * @return the expiration time value measured in milliseconds. A
     *         negative return value indicates the entry never expires.
     */
    long expirationTime();

    /**
     * A callback to be called when a value is expired from the map.
     */
    void onExpiration();
}
