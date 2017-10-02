package com.vmturbo.topology.processor.api;

import java.time.LocalDateTime;

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
}
