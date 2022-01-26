package com.vmturbo.cost.component.cloud.commitment.coverage;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentCoverage;
import com.vmturbo.cost.component.stores.DataFilterApplicator;

/**
 * A filter application of {@link TopologyEntityCoverageFilter} onto {@link CoverageInfo}.
 */
public class TopologyCoverageFilterApplicator implements DataFilterApplicator<CoverageInfo, TopologyEntityCoverageFilter> {

    @Override
    public CoverageInfo filterData(@Nonnull CoverageInfo sourceData, @Nonnull TopologyEntityCoverageFilter filter) {


        return sourceData.toBuilder()
                .entityCoverageMap(
                        Maps.filterValues(
                                sourceData.entityCoverageMap(),
                                coverage -> filterCoverageEntry(coverage, filter)))
                .build();
    }

    private boolean filterCoverageEntry(@Nonnull ScopedCommitmentCoverage coverage,
                                        @Nonnull TopologyEntityCoverageFilter coverageFilter) {

        final boolean entityFilter = !coverageFilter.hasEntityFilter()
                || coverageFilter.entityFilter().getEntityIdList().contains(coverage.getEntityOid());
        final boolean regionFilter = !coverageFilter.hasRegionFilter()
                || coverageFilter.regionFilter().getRegionIdList().contains(coverage.getRegionOid());
        final boolean accountFilter = !coverageFilter.hasAccountFilter()
                || coverageFilter.accountFilter().getAccountIdList().contains(coverage.getAccountOid());
        final boolean serviceProviderFilter = !coverageFilter.hasServiceProviderFilter()
                || coverageFilter.serviceProviderFilter().getServiceProviderIdList().contains(coverage.getServiceProviderOid());

        return entityFilter && regionFilter && accountFilter && serviceProviderFilter;
    }
}
