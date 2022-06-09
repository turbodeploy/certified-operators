package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageRulesFactory;

/**
 * A factory for creating instances of {@link CloudCommitmentCoverageAllocator}.
 */
public interface CoverageAllocatorFactory {

    /**
     * Creates a new instance of {@link CloudCommitmentCoverageAllocator}, based on the provided
     * {@link CoverageAllocationConfig}.
     *
     * @param config The configuration of the requested {@link CloudCommitmentCoverageAllocator}
     *         instance
     * @return The newly created {@link CloudCommitmentCoverageAllocator} instance
     */
    @Nonnull
    CloudCommitmentCoverageAllocator createAllocator(@Nonnull CoverageAllocationConfig config);

    /**
     * The default instance of {@link CoverageAllocatorFactory}.
     */
    class DefaultCoverageAllocatorFactory implements CoverageAllocatorFactory {

        private final CoverageRulesFactory coverageRulesFactory;

        /**
         * Instantiates a new Default coverage allocator factory.
         *
         * @param coverageRulesFactory the coverage rules factory
         */
        public DefaultCoverageAllocatorFactory(@Nonnull CoverageRulesFactory coverageRulesFactory) {
            this.coverageRulesFactory = Objects.requireNonNull(coverageRulesFactory);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CloudCommitmentCoverageAllocator createAllocator(@Nonnull final CoverageAllocationConfig config) {
            Preconditions.checkNotNull(config);
            return new CloudCommitmentCoverageAllocator(coverageRulesFactory, config);
        }
    }
}
