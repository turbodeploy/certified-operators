package com.vmturbo.topology.processor.staledata;

import common.HealthCheck.HealthState;

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
    HealthState getLastKnownTargetHealth(long targetOid);
}
