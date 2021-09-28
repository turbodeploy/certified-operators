package com.vmturbo.topology.processor.api;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Represents a probe registration in topology processor.
 */
@Immutable
public interface ProbeRegistrationInfo {
    /**
     * Returns the uuid of this probe registration.
     *
     * @return uuid of the probe registration
     */
    long getId();

    /**
     * Return the probe registration display name.
     *
     * @return the display name, or an empty string if there is no display name
     */
    @Nonnull
    String getDisplayName();

    /**
     * Returns id of the probe type that this probe registration is associated with.
     *
     * @return id of the probe type
     */
    long getProbeId();

    /**
     * Returns the communication channel of the probe registration.
     *
     * @return the communication channel of the probe registration or {@link Optional}.empty if none present
     */
    @Nonnull
    Optional<String> getCommunicationBindingChannel();

    /**
     * Return version of the probe registration.
     *
     * @return version of the probe registration, or an empty string if there is no detected version.
     */
    @Nonnull
    String getVersion();

    /**
     * Return the time when this probe is first registered.
     *
     * @return time when the probe is registered
     */
    long getRegisteredTime();
}
