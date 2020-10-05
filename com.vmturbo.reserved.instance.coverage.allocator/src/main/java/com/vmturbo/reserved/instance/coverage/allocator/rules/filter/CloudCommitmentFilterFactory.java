package com.vmturbo.reserved.instance.coverage.allocator.rules.filter;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.reserved.instance.coverage.allocator.rules.filter.CloudCommitmentFilter.CloudCommitmentFilterConfig;
import com.vmturbo.reserved.instance.coverage.allocator.rules.filter.ReservedInstanceFilter.ReservedInstanceFilterConfig;

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

        if (filterConfig.type() == CloudCommitmentType.RESERVED_INSTANCE) {
            return new ReservedInstanceFilter((ReservedInstanceFilterConfig)filterConfig);
        } else {
            throw new UnsupportedOperationException("Unsupported cloud commitment type");
        }
    }

}
