package com.vmturbo.clustermgr.management;

import java.util.Optional;

/**
 * The possible health statuses of a component.
 */
public enum ComponentHealth {
    /**
     * Unknown - there has not been a health check so far.
     */
    UNKNOWN(0),

    /**
     * Healthy - component is responding to health checks with a healthy status.
     */
    HEALTHY(1),

    /**
     * Component is responding to health checks with an unhealthy status.
     */
    WARNING(2),

    /**
     * Component is not responsive.
     */
    CRITICAL(3);

    private static final ComponentHealth[] VALUES = ComponentHealth.values();

    /**
     * We save the index in the database for component registration, so DO NOT change the index
     * of an existing status!
     */
    private final int index;

    ComponentHealth(final int index) {
        this.index = index;
    }

    /**
     * Get the numeric representation of this status. This may be the same as {@link ComponentHealth#ordinal()},
     * but it may not be. DO NOT save the ordinal in the database.
     *
     * @return The number to represent the status.
     */
    public int getNumber() {
        return index;
    }

    /**
     * Get the {@link ComponentHealth} for a particular number
     * (as returned by {@link ComponentHealth#getNumber()}).
     *
     * @param number The number.
     * @return The associated {@link ComponentHealth}, if found. Empty optional otherwise.
     */
    public static Optional<ComponentHealth> fromNumber(final int number) {

        for (ComponentHealth status : VALUES) {
            if (number == status.getNumber()) {
                return Optional.of(status);
            }
        }

        return Optional.empty();
    }
}
