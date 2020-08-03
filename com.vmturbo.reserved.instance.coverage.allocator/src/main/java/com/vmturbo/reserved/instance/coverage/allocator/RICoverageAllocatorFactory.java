package com.vmturbo.reserved.instance.coverage.allocator;

import javax.annotation.Nonnull;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocator.RICoverageAllocatorConfig;

/**
 * A factory for creating instances of {@link ReservedInstanceCoverageAllocator}.
 */
public interface RICoverageAllocatorFactory {

    /**
     * Creates a new instance of {@link ReservedInstanceCoverageAllocator}, based on the provided
     * {@link RICoverageAllocatorConfig}.
     * @param config The configuration of the requested {@link ReservedInstanceCoverageAllocator} instance
     * @return The newly created {@link ReservedInstanceCoverageAllocator} instance
     */
    ReservedInstanceCoverageAllocator createAllocator(@Nonnull RICoverageAllocatorConfig config);

    /**
     * The default instance of {@link RICoverageAllocatorFactory}.
     */
    class DefaultRICoverageAllocatorFactory implements RICoverageAllocatorFactory {

        /**
         * {@inheritDoc}
         */
        @Override
        public ReservedInstanceCoverageAllocator createAllocator(@Nonnull final RICoverageAllocatorConfig config) {
            return new ReservedInstanceCoverageAllocator(config);
        }
    }
}
