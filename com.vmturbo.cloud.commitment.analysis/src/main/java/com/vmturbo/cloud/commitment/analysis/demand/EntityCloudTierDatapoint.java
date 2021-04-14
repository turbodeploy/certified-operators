package com.vmturbo.cloud.commitment.analysis.demand;

/**
 * A data point representing the allocation of an entity on a cloud tier for a single point in time.
 */
public interface EntityCloudTierDatapoint extends ScopedCloudTierInfo {

    /**
     * The entity OID.
     * @return The entity OID.
     */
    long entityOid();

    /**
     * The entity type.
     * @return The entity type.
     */
    int entityType();
}
