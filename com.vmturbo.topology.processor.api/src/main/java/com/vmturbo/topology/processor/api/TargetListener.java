package com.vmturbo.topology.processor.api;

import javax.annotation.Nonnull;

/**
 * Listener to receive target-related events.
 */
public interface TargetListener {
    /**
     * Triggered when new target has been registered in topology processor.
     *
     * @param target new target added.
     */
    default void onTargetAdded(@Nonnull TargetInfo target) {}

    /**
     * Triggered when target has been removed from topology processor.
     *
     * @param target removed target id.
     */
    default void onTargetRemoved(long target) {}

    /**
     * Triggered when target has been validated.
     *
     * @param result of target validation
     */
    default void onTargetValidated(@Nonnull ValidationStatus result) {}

    /**
     * Triggered when target has been discovered.
     *
     * @param result of target discovery
     */
    default void onTargetDiscovered(@Nonnull DiscoveryStatus result) {}

    /**
     * Triggered when an existing target is changed.
     *
     * @param target target info of the target, that has been changed.
     */
    default void onTargetChanged(@Nonnull TargetInfo target) {}
}
