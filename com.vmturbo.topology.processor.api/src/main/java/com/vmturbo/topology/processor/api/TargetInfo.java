package com.vmturbo.topology.processor.api;

import java.time.LocalDateTime;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import common.HealthCheck.HealthState;

/**
 * Represents target in topology processor.
 */
@Immutable
public interface TargetInfo extends TargetData {
    /**
     * Returns id of this target.
     *
     * @return id of the target
     */
    long getId();


    /**
     * Return the target display name.
     *
     * @return the display name, if one exists.
     */
    @Nonnull
    String getDisplayName();

    /**
     * Returns id of the probe this target is associated with.
     *
     * @return id of the probe
     */
    long getProbeId();

    /**
     * Returns date of the latest finished validation or discovery (failed of succeeded).
     *
     * @return date of the latest validation
     */
    @Nullable
    LocalDateTime getLastValidationTime();

    /**
     * Return status of the target.
     *
     * @return status of the target
     */
    @Nullable
    String getStatus();

    /**
     * Returns the boolean value to know if the target is hidden
     *
     * @return boolean that whether we hide the target
     */
    boolean isHidden();

    /**
     * Returns the boolean value to know if the target is read-only:
     * determines whether the target cannot be changed through public APIs.
     *
     * @return true if a target is read-only, otherwise false.
     */
    boolean isReadOnly();

    /**
     * Returns a List of the derived target IDs associated with this target.
     *
     * @return List of the derived target IDs associated with this target.
     */
    List<Long> getDerivedTargetIds();

    /**
     * Return the last editing user.
     *
     * @return the last editing user
     */
    @Nullable
    String getLastEditingUser();

    /**
     * Return the last edit time.
     *
     * @return the last edit time
     */
    @Nullable
    Long getLastEditTime();

    /**
     * Return the health state of this target.
     *
     * @return the health state of this target
     */
    @Nonnull
    HealthState getHealthState();

    /**
     * Return the list of parent target IDs or an empty list if none found.
     *
     * @return the list of parent target ids.
     */
    @Nonnull
    List<Long> getParentTargetIds();
}
