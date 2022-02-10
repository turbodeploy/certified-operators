package com.vmturbo.cloud.common.commitment.filter;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.commitment.CloudCommitmentResourceScope.ComputeTierResourceScope;
import com.vmturbo.cloud.common.commitment.aggregator.AggregationInfo;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.filter.CloudCommitmentFilterCriteria.ComputeScopeFilterCriteria;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;

/**
 * A filter for cloud commitments through {@link CloudCommitmentAggregate}.
 */
public class CloudCommitmentFilter {

    private final CloudCommitmentFilterCriteria filterCriteria;

    /**
     * Constructs a new {@link CloudCommitmentFilter} instance.
     * @param filterCriteria The filter criteria.
     */
    public CloudCommitmentFilter(@Nonnull CloudCommitmentFilterCriteria filterCriteria) {
        this.filterCriteria = Objects.requireNonNull(filterCriteria);
    }

    /**
     * Checks whether {@code commitmentAggregate} passes the filter.
     * @param commitmentAggregate The target {@link CloudCommitmentAggregate}.
     * @return True, if the commitment aggregate passes the filter. False otherwise.
     */
    public boolean filter(@Nonnull CloudCommitmentAggregate commitmentAggregate) {


        final AggregationInfo aggregationInfo = commitmentAggregate.aggregationInfo();

        return filterType(commitmentAggregate)
                && filterEntityScope(aggregationInfo)
                && filterLocation(aggregationInfo)
                && filterResourceScope(aggregationInfo);
    }

    private boolean filterType(@Nonnull CloudCommitmentAggregate commitmentAggregate) {

        return commitmentAggregate.commitmentType() == filterCriteria.type()
                && commitmentAggregate.aggregationInfo().coverageType() == filterCriteria.coverageType();
    }

    private boolean filterEntityScope(@Nonnull AggregationInfo aggregateInfo) {

        final CloudCommitmentScope riScope = aggregateInfo.entityScope().getScopeType();
        return filterCriteria.entityScopes().isEmpty() || filterCriteria.entityScopes().contains(riScope);

    }

    private boolean filterResourceScope(@Nonnull AggregationInfo aggregationInfo) {

        if (filterCriteria.resourceScopeCriteria() instanceof ComputeScopeFilterCriteria
                && aggregationInfo.resourceScope() instanceof ComputeTierResourceScope) {

            return filterComputeResourceScope(
                    (ComputeTierResourceScope)aggregationInfo.resourceScope(),
                    (ComputeScopeFilterCriteria)filterCriteria.resourceScopeCriteria());
        } else {
            return false;
        }
    }

    private boolean filterComputeResourceScope(@Nonnull ComputeTierResourceScope resourceScope,
                                               @Nonnull ComputeScopeFilterCriteria computeFilterCriteria) {

        final boolean sizeFlexibility = computeFilterCriteria.isSizeFlexible()
                .map(sizeFlexible -> sizeFlexible == resourceScope.isSizeFlexible())
                .orElse(true);

        final boolean platformFlexibility = computeFilterCriteria.isPlatformFlexible()
                .map(platformFlexible -> platformFlexible == resourceScope.platformInfo().isPlatformFlexible())
                .orElse(true);

        return sizeFlexibility && platformFlexibility;
    }

    private boolean filterLocation(@Nonnull AggregationInfo aggregateInfo) {

        final CloudCommitmentLocationType riLocation = aggregateInfo.location().getLocationType();

        return filterCriteria.locations().isEmpty() || filterCriteria.locations().contains(riLocation);
    }
}
