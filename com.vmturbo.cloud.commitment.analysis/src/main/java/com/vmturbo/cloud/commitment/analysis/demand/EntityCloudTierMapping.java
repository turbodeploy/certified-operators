package com.vmturbo.cloud.commitment.analysis.demand;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.data.TimeSeriesData;

/**
 * A mapping of an entity to cloud tier over a period of time, as represented by the start and end
 * times of the mapping.
 */
@Immutable
public interface EntityCloudTierMapping extends TimeSeriesData, ScopedCloudTierInfo {

    /**
     * The entity OID of the mapping.
     * @return The entity OID.
     */
    long entityOid();

}
