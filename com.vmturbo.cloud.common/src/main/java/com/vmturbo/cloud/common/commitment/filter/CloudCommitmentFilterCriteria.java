package com.vmturbo.cloud.common.commitment.filter;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocationType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;

/**
 * A base filter configuration for {@link CloudCommitmentFilter}.
 */
@HiddenImmutableImplementation
@Immutable
public interface CloudCommitmentFilterCriteria {

    /**
     * The type of cloud commitment required to pass the filter.
     * @return The cloud commitment type to filter against.
     */
    @Nonnull
    CloudCommitmentType type();

    /**
     * The coverage type required to pass the filter.
     * @return The coverage type required to pass the filter.
     */
    @Nonnull
    CloudCommitmentCoverageType coverageType();

    /**
     * The locations allowed to pass the filter.
     * @return The locations allowed to pass the filter. An empty set means the filter will not
     * check cloud commitment locations.
     */
    @Nonnull
    Set<CloudCommitmentLocationType> locations();

    /**
     * The scopes allowed to pass the filter.
     * @return The scopes allowed to pass the filter. AN empty set means the filter will not
     * check cloud commitment scopes.
     */
    @Nonnull
    Set<CloudCommitmentScope> entityScopes();

    /**
     * The criteria for filtering the resource scope of a cloud commitment.
     * @return The criteria for filtering the resource scope of a cloud commitment.
     */
    @Nonnull
    ResourceScopeFilterCriteria resourceScopeCriteria();

    /**
     * The criteria for filtering the resource scope of a cloud commitment.
     */
    interface ResourceScopeFilterCriteria {}

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link CloudCommitmentFilterCriteria} instances.
     */
    class Builder extends ImmutableCloudCommitmentFilterCriteria.Builder {}

    /**
     * The criteria for filtering a compute scoped cloud commitment.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface ComputeScopeFilterCriteria extends ResourceScopeFilterCriteria {

        /**
         * Indicates whether RIs should be filtered based on platform flexibility. If the {@link Optional}
         * is empty, RIs will not be filtered based on platform flexibility.
         * @return An optional boolean indicating whether to filter against platform flexibility of RIs.
         */
        @Nonnull
        Optional<Boolean> isPlatformFlexible();

        /**
         * Indicates whether RIs should be filtered based on size flexibility. If the {@link Optional}
         * is empty, RIs will not be filtered based on size flexibility.
         * @return An optional boolean indicating whether to filter against size flexibility of RIs.
         */
        @Nonnull
        Optional<Boolean> isSizeFlexible();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing immutable {@link ComputeScopeFilterCriteria} instances.
         */
        class Builder extends ImmutableComputeScopeFilterCriteria.Builder {}
    }
}
