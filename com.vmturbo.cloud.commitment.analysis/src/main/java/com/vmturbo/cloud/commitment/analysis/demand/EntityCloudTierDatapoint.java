package com.vmturbo.cloud.commitment.analysis.demand;

/**
 * A data point representing the allocation of an entity on a cloud tier for a single point in time.
 */
public interface EntityCloudTierDatapoint extends ScopedCloudTierDemand {

    /**
     * The entity OID.
     * @return The entity OID.
     */
    long entityOid();
}
