package com.vmturbo.cloud.commitment.analysis.persistence;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocationFilter;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableEntityComputeTierAllocationFilter;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableTimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter.TimeComparator;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.DemandSegment;

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
    public Stream<EntityCloudTierMapping<?>> getDemand(@Nonnull final CloudTierType cloudTierType,
                                                       @Nonnull final Collection<DemandSegment> demandSegments,
                                                       @Nonnull final Instant earliestEndTime) {


        return demandSegments.stream()
                .flatMap(demandSegment -> {
                    final DemandScope demandScope = demandSegment.getScope();
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
                            .addAllPlatforms(demandScope.getComputeTierScope().getPlatformList())
                            .addAllTenancies(demandScope.getComputeTierScope().getTenancyList())
                            .addAllComputeTierOids(demandScope.getComputeTierScope().getComputeTierOidList())
                            .build();

                    return computeTierAllocationStore.streamAllocations(filter);
                });
    }
}
