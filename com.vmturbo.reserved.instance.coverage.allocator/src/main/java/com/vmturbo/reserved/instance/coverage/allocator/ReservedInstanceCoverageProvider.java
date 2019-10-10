package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

/**
 * An interface for provider coverage capacity of topology entities and RIs, as well as initial
 * coverage assignments as input to {@link ReservedInstanceCoverageAllocator}.
 */
public interface ReservedInstanceCoverageProvider {

    /**
     * @return A {@link Table} of {@literal <Entity OID, RI OID, Coverage Amount>}
     */
    @Nonnull
    Table<Long, Long, Double> getAllCoverages();

    /**
     * Provides a map indicating the coverage capacity of each entity. For entities in invalid coverage
     * states (e.g. POWERED_OFF), it is expected the capacity will be 0 (or the entity will not be
     * included and therefore capacity is assumed to be 0)
     *
     * @return A {@link Map} of {@literal <Entity OID, Coverage Capacity>}
     */
    @Nonnull
    Map<Long, Double> getCapacityForEntities();

    /**
     * @return A {@link Map} of {@literal <RI OID, Coverage Capacity>}
     */
    @Nonnull
    Map<Long, Double> getCapacityForReservedInstances();
}
