package com.vmturbo.cost.component.cloud.commitment.mapping;

import javax.annotation.Nullable;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudCommitmentFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;

/**
 * A filter for cloud commitment mappings (entity <-> commitment).
 */
@HiddenImmutableImplementation
@Immutable
public interface CommitmentMappingFilter {

    /**
     * Checks whether this filter has an entity filter.
     * @return True, if an entity filter is set and that filter is not "allow all".
     */
    default boolean hasEntityFilter() {
        return entityFilter() != null && entityFilter().getEntityIdCount() > 0;
    }

    /**
     * The entity filter.
     * @return The entity filter.
     */
    @Nullable
    EntityFilter entityFilter();

    /**
     * Checks whether this filter has a commitment filter.
     * @return True, if a commitment filter is set and that filter is not "allow all".
     */
    default boolean hasCommitmentFilter() {
        return commitmentFilter() != null && commitmentFilter().getCloudCommitmentIdCount() > 0;
    }

    /**
     * The commitment filter.
     * @return The commitment filter.
     */
    @Nullable
    CloudCommitmentFilter commitmentFilter();

    /**
     * A builder class for contructing immutable {@link CloudCommitmentFilter} instances.
     */
    class Builder extends ImmutableCommitmentMappingFilter.Builder {}

}
