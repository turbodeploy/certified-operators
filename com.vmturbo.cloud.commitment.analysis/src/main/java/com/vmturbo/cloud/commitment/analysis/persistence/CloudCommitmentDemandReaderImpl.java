package com.vmturbo.cloud.commitment.analysis.persistence;

import java.time.Instant;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableTimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter.TimeComparator;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.demand.store.EntityComputeTierAllocationFilter;
import com.vmturbo.cloud.commitment.analysis.demand.store.ImmutableEntityComputeTierAllocationFilter;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;

/**
 * An implementation of {@link CloudCommitmentDemandReader}.
 */
public class CloudCommitmentDemandReaderImpl implements CloudCommitmentDemandReader {

    private final ComputeTierAllocationStore computeTierAllocationStore;


    /**
     * Constructs a new instance of the demand reader, using the provided stores.
     * @param computeTierAllocationStore A store containing compute tier allocation demand.
     */
    public CloudCommitmentDemandReaderImpl(@Nonnull ComputeTierAllocationStore computeTierAllocationStore) {
        this.computeTierAllocationStore = Objects.requireNonNull(computeTierAllocationStore);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<EntityCloudTierMapping> getAllocationDemand(@Nonnull CloudTierType cloudTierType,
                                                              @Nonnull DemandScope demandScope,
                                                              @Nonnull Instant earliestEndTime) {

        final EntityComputeTierAllocationFilter filter = ImmutableEntityComputeTierAllocationFilter.builder()
                // We filter based on end time, in order to include records that started before
                // the target start time, but ended after. We'll later adjust the start time
                // of any records that spread across the start time to the start time, effectively
                // only selecting the demand from the target start time.
                .endTimeFilter(ImmutableTimeFilter.builder()
                        .time(earliestEndTime)
                        .comparator(TimeComparator.AFTER_OR_EQUAL_TO)
                        .build())
                .addAllEntityOids(demandScope.getEntityOidList())
                .addAllAccountOids(demandScope.getAccountOidList())
                .addAllRegionOids(demandScope.getRegionOidList())
                .addAllServiceProviderOids(demandScope.getServiceProviderOidList())
                // If the pass through filter is passed in, all these filters will be empty (default).
                // In the future, we should make this conditional when multiple tier types are supported.
                .addAllPlatforms(demandScope.getComputeTierScope().getPlatformList())
                .addAllTenancies(demandScope.getComputeTierScope().getTenancyList())
                .addAllComputeTierOids(demandScope.getCloudTierOidList())
                .build();

        // Right now only compute tier demand is supported. Therefore, we don't check
        // the cloudTierType.
        return computeTierAllocationStore.streamAllocations(filter)
                .map(EntityCloudTierMapping.class::cast);
    }
}
