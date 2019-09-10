package com.vmturbo.topology.processor.api;

import java.time.LocalDateTime;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

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
     * Returns optional parent id of this target. E.g. if the target is belonged to storage browsing or
     * billing categories, it should has a parent target like VC or Azure target. The id will point to the
     * instance of target. If there is no parent target, the field will be empty.
     *
     * @return optional of parent id of the target
     */
    Optional<Long> getParentId();

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
}
