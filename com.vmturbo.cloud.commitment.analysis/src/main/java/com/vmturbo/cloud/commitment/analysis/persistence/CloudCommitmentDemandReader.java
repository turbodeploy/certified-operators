package com.vmturbo.cloud.commitment.analysis.persistence;

import java.time.Instant;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;

/**
 * The reader is responsible for wrapping the underlying demand stores and querying demand from the
 * database as part of an analysis.
 */
public interface CloudCommitmentDemandReader {


    /**
     * Queries entity allocation demand, based on the requested {@code cloudTierType},
     * from the appropriate demand stores.
     * @param cloudTierType The cloud tier demand type to query.
     * @param demandScope The demand scope to query.
     * @param earliestEndTime The earliest point from which demand should be queried.
     * @return A {@link Stream} containing EntityCloudTierMapping entries from the requested demand.
     */
    Stream<EntityCloudTierMapping> getAllocationDemand(@Nonnull CloudTierType cloudTierType,
                                                       @Nonnull DemandScope demandScope,
                                                       @Nonnull Instant earliestEndTime);
}
