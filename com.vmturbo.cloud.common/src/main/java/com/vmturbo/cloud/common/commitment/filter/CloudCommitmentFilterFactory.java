package com.vmturbo.cloud.common.commitment.filter;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.cloud.common.commitment.filter.CloudCommitmentFilter.CloudCommitmentFilterConfig;
import com.vmturbo.cloud.common.commitment.filter.ReservedInstanceFilter.ReservedInstanceFilterConfig;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;

/**
 * A factory class for producing {@link CloudCommitmentFilter} implementations. The filter implementation
 * will be based on the configuration type.
 */
public class CloudCommitmentFilterFactory {

    /**
     * Creates and returns a {@link CloudCommitmentFilter} specific to the cloud commitment type
     * of the {@code filterConfig}.
     * @param filterConfig The filter configuration.
     * @return The newly constructed {@link CloudCommitmentFilter} instance.
     */
    @Nonnull
    public CloudCommitmentFilter createFilter(@Nonnull CloudCommitmentFilterConfig filterConfig) {
        Preconditions.checkNotNull(filterConfig);

        if (filterConfig.coverageType() == CloudCommitmentCoverageType.COUPONS) {
            return new ReservedInstanceFilter((ReservedInstanceFilterConfig)filterConfig);
        } else {
            throw new UnsupportedOperationException("Unsupported cloud commitment type");
        }
    }

}
