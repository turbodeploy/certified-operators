package com.vmturbo.cloud.common.commitment.filter;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

/**
 * A factory class for producing {@link CloudCommitmentFilter} implementations. The filter implementation
 * will be based on the configuration type.
 */
public class CloudCommitmentFilterFactory {

    /**
     * Creates and returns a {@link CloudCommitmentFilter} specific to the cloud commitment type
     * of the {@code filterConfig}.
     * @param filterCriteria The filter criteria.
     * @return The newly constructed {@link CloudCommitmentFilter} instance.
     */
    @Nonnull
    public CloudCommitmentFilter createFilter(@Nonnull CloudCommitmentFilterCriteria filterCriteria) {
        Preconditions.checkNotNull(filterCriteria);

        return new CloudCommitmentFilter(filterCriteria);
    }

}
