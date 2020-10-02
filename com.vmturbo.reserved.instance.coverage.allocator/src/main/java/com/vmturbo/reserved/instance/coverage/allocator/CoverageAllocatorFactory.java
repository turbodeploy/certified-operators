package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageRulesFactory;

/**
 * A factory for creating instances of {@link ReservedInstanceCoverageAllocator}.
 */
public interface CoverageAllocatorFactory {

    /**
     * Creates a new instance of {@link ReservedInstanceCoverageAllocator}, based on the provided
     * {@link CoverageAllocationConfig}.
     * @param config The configuration of the requested {@link ReservedInstanceCoverageAllocator} instance
     * @return The newly created {@link ReservedInstanceCoverageAllocator} instance
     */
    @Nonnull
    ReservedInstanceCoverageAllocator createAllocator(@Nonnull CoverageAllocationConfig config);

    /**
     * The default instance of {@link CoverageAllocatorFactory}.
     */
    class DefaultCoverageAllocatorFactory implements CoverageAllocatorFactory {

        private final CoverageRulesFactory coverageRulesFactory;

        public DefaultCoverageAllocatorFactory(@Nonnull CoverageRulesFactory coverageRulesFactory) {
            this.coverageRulesFactory = Objects.requireNonNull(coverageRulesFactory);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ReservedInstanceCoverageAllocator createAllocator(@Nonnull final CoverageAllocationConfig config) {
            Preconditions.checkNotNull(config);
            return new ReservedInstanceCoverageAllocator(coverageRulesFactory, config);
        }
    }
}
