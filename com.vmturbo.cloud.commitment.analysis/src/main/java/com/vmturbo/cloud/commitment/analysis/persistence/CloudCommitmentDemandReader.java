package com.vmturbo.cloud.commitment.analysis.persistence;

import java.time.Instant;
import java.util.Collection;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.DemandSegment;

/**
 * The reader is responsible for wrapping the underlying demand stores and querying demand from the
 * database as part of an analysis.
 */
public interface CloudCommitmentDemandReader {


    /**
     * Queries entity demand, based on the requested {@code cloudTierType}, from the appropriate demand
     * stores.
     * @param cloudTierType The cloud tier demand type to query
     * @param demandSegments The demand segments to query for the target cloud tier. Segments contain
     *                       both the scope and type of demand (allocated|projected) to query.
     * @param earliestEndTime The earliest point from which demand should be queried.
     * @return A {@link Stream} containing EntityCloudTierMapping entries from the requested demand.
     */
    Stream<EntityCloudTierMapping> getDemand(@Nonnull CloudTierType cloudTierType,
                                             @Nonnull Collection<DemandSegment> demandSegments,
                                             @Nonnull Instant earliestEndTime);
}
