package com.vmturbo.reserved.instance.coverage.allocator;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

/**
 * An interface for providing initial coverage assignments as input to {@link ReservedInstanceCoverageAllocator}.
 */
@FunctionalInterface
public interface ReservedInstanceCoverageProvider {

    /**
     * @return A {@link Table} of {@literal <Entity OID, RI OID, Coverage Amount>}
     */
    @Nonnull
        Table<Long, Long, Double> getAllCoverages();
}
