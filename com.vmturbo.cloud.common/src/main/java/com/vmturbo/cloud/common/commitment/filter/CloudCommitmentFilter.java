package com.vmturbo.cloud.common.commitment.filter;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocationType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;

/**
 * A filter of cloud commitments, based on the commitment attributes.
 */
public interface CloudCommitmentFilter {

    /**
     * Checks whether {@code commitmentAggregate} passes the filter.
     * @param commitmentAggregate The target {@link CloudCommitmentAggregate}.
     * @return True, if the commitment aggregate passes the filter. False otherwise.
     */
    boolean filter(@Nonnull CloudCommitmentAggregate commitmentAggregate);

    /**
     * A base filter configuration for {@link CloudCommitmentFilter}.
     */
    interface CloudCommitmentFilterConfig {

        /**
         * The type of cloud commitment required to pass the filter.
         * @return The cloud commitment type to filter against.
         */
        @Nonnull
        CloudCommitmentType type();

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
        Set<CloudCommitmentScope> scopes();
    }
}
