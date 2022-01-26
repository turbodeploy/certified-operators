package com.vmturbo.cost.component.cloud.commitment.utilization;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentUtilization;
import com.vmturbo.cost.component.stores.DataFilterApplicator;

/**
 * A filter applicator of {@link TopologyCommitmentUtilizationFilter} on {@link UtilizationInfo}.
 */
public class TopologyUtilizationFilterApplicator implements
        DataFilterApplicator<UtilizationInfo, TopologyCommitmentUtilizationFilter> {

    @Override
    public UtilizationInfo filterData(UtilizationInfo sourceData,
                                      TopologyCommitmentUtilizationFilter filter) {
        return sourceData.toBuilder()
                .commitmentUtilizationMap(Maps.filterValues(
                        sourceData.commitmentUtilizationMap(),
                        utilization -> filterUtilization(utilization, filter)))
                .build();
    }

    private boolean filterUtilization(@Nonnull ScopedCommitmentUtilization utilization,
                                      @Nonnull TopologyCommitmentUtilizationFilter filter) {

        final boolean commitmentFilter = !filter.hasCommitmentFilter()
                || filter.cloudCommitmentFilter().getCloudCommitmentIdList().contains(utilization.getCloudCommitmentOid());
        final boolean accountFilter = !filter.hasAccountFilter()
                || filter.accountFilter().getAccountIdList().contains(utilization.getAccountOid());
        final boolean regionFilter = !filter.hasRegionFilter()
                || (utilization.hasRegionOid()
                && filter.regionFilter().getRegionIdList().contains(utilization.getRegionOid()));
        final boolean serviceProviderFilter = !filter.hasServiceProviderFilter()
                || (filter.serviceProviderFilter().getServiceProviderIdList().contains(utilization.getServiceProviderOid()));

        return commitmentFilter && accountFilter && regionFilter && serviceProviderFilter;
    }
}
