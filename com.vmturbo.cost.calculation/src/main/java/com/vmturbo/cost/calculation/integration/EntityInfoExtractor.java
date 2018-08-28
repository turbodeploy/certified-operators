package com.vmturbo.cost.calculation.integration;

import java.util.Optional;

import javax.annotation.Nonnull;

import jdk.nashorn.internal.ir.annotations.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;

/**
 * An interface provided by users of the cost calculation library to extract the
 * necessary information from entities in a topology, regardless of the Java class used to represent
 * the entities.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology.
 */
public interface EntityInfoExtractor<ENTITY_CLASS> {

    /**
     * Get the entity type of a particular entity.
     *
     * @param entity The entity.
     * @return The entity type for the entity.
     */
    int getEntityType(@Nonnull final ENTITY_CLASS entity);

    /**
     * Get the ID of a particular entity.
     *
     * @param entity The entity.
     * @return The ID of the entity.
     */
    long getId(@Nonnull final ENTITY_CLASS entity);

    /**
     * Get the compute configuration of a particular entity.
     *
     * The compute configuration consists of all the properties of the entity that
     * affect the compute tier price (within a specific region and service).
     *
     * @param entity The entity.
     * @return An optional containing the {@link ComputeConfig}. An empty optional if there is no
     *         compute costs associated with this entity.
     */
    @Nonnull
    Optional<ComputeConfig> getComputeConfig(@Nonnull ENTITY_CLASS entity);

    /**
     * A wrapper class around the compute configuration of an entity.
     */
    @Immutable
    class ComputeConfig {
        private final OSType os;
        private final Tenancy tenancy;

        public ComputeConfig(final OSType os, final Tenancy tenancy) {
            this.os = os;
            this.tenancy = tenancy;
        }

        @Nonnull
        public OSType getOs() {
            return os;
        }

        @Nonnull
        public Tenancy getTenancy() {
            return tenancy;
        }

        public boolean matchesPriceTableConfig(@Nonnull final ComputeTierConfigPrice computeTierConfigPrice) {
            return computeTierConfigPrice.getGuestOsType() == os &&
                computeTierConfigPrice.getTenancy() == tenancy;
        }
    }
}
