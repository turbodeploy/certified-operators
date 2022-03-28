package com.vmturbo.cloud.commitment.analysis.persistence;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter.TimeComparator;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.demand.store.EntityComputeTierAllocationFilter;
import com.vmturbo.cloud.common.data.TimeInterval;
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
    public void getAllocationDemand(@Nonnull CloudTierType cloudTierType,
                                    @Nonnull DemandScope demandScope,
                                    @Nonnull TimeInterval selectionWindow,
                                    @Nonnull Consumer<EntityCloudTierMapping> consumer) {

        final EntityComputeTierAllocationFilter filter = EntityComputeTierAllocationFilter.builder()
                .startTimeFilter(TimeFilter.builder()
                        .time(selectionWindow.endTime())
                        .comparator(TimeComparator.BEFORE_OR_EQUAL_TO)
                        .build())
                // We filter based on end time, in order to include records that started before
                // the target start time, but ended after. We'll later adjust the start time
                // of any records that spread across the start time to the start time, effectively
                // only selecting the demand from the target start time.
                .endTimeFilter(TimeFilter.builder()
                        .time(selectionWindow.startTime())
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
        computeTierAllocationStore.streamAllocations(filter, consumer::accept);
    }
}
