package com.vmturbo.cost.component.cloud.commitment.mapping;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.cost.component.stores.DataFilterApplicator;

/**
 * A filter applicator for applying a {@link CommitmentMappingFilter} to {@link MappingInfo}.
 */
public class CommitmentMappingFilterApplicator implements DataFilterApplicator<MappingInfo, CommitmentMappingFilter> {

    @Override
    public MappingInfo filterData(MappingInfo sourceData, CommitmentMappingFilter filter) {
        return sourceData.toBuilder()
                .cloudCommitmentMappings(sourceData.cloudCommitmentMappings()
                        .stream()
                        .filter(mapping -> filterMapping(mapping, filter))
                        .collect(ImmutableList.toImmutableList()))
                .build();
    }

    private boolean filterMapping(@Nonnull CloudCommitmentMapping mapping,
                                  @Nonnull CommitmentMappingFilter filter) {
        return (!filter.hasEntityFilter()
                || filter.entityFilter().getEntityIdList().contains(mapping.getEntityOid()))
                && (!filter.hasCommitmentFilter()
                || filter.commitmentFilter().getCloudCommitmentIdList().contains(mapping.getCloudCommitmentOid()));
    }
}
