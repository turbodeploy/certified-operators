package com.vmturbo.cloud.commitment.analysis.persistence;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.common.data.TimeInterval;
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
     * @param selectionWindow The time window to select demand. Any entries with overlapping time
     *                        with respect to the selection window will be returned. This may mean
     *                        the returned records start before the selection window and/or end
     *                        after it.
     * @return A {@link Stream} containing EntityCloudTierMapping entries from the requested demand.
     */
    Stream<EntityCloudTierMapping> getAllocationDemand(@Nonnull CloudTierType cloudTierType,
                                                       @Nonnull DemandScope demandScope,
                                                       @Nonnull TimeInterval selectionWindow);
}
