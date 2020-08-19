package com.vmturbo.cloud.commitment.analysis.demand;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

/**
 * A mapping of an entity to cloud tier over a period of time, as represented by the start and end
 * times of the mapping.
 */
@Immutable
public interface EntityCloudTierMapping extends TimeSeriesData, ScopedCloudTierDemand {

    /**
     * The time interval (recorded in UTC) of the mapping between the entity and cloud tier.
     * @return The time interval of this mapping.
     */
    @Nonnull
    TimeInterval timeInterval();

    /**
     * The entity OID of the mapping.
     * @return The entity OID.
     */
    long entityOid();

}
