package com.vmturbo.topology.processor.staledata;

import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;

/**
 * Exposes stale data information to the interested parties.
 * For now, at target level only.
 */
public interface StalenessInformationProvider {
    /**
     * Get the last known target health, calculate immediately if not known yet.
     * The health information should be no older than {@link StaleDataConfig} ${staleDataCheckFrequencyMinutes}
     *
     * @param targetOid target's identifier
     * @return target health, can be null if all attempts to calculate so far failed
     */
    @Nullable
    TargetHealth getLastKnownTargetHealth(long targetOid);
}
