package com.vmturbo.cost.calculation.integration;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import jdk.nashorn.internal.ir.annotations.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList.DatabaseTierConfigPrice;

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
     * Get the database configuration of a particular entity.
     *
     * The database configuration consists of all the properties of the entity that
     * affect the database tier price (within a specific region and service).
     *
     * @param entity The entity.
     * @return An optional containing the {@link DatabaseConfig}. An empty optional if there is no
     *         database costs associated with this entity.
     */
    @Nonnull
    Optional<DatabaseConfig> getDatabaseConfig(@Nonnull ENTITY_CLASS entity);

    /**
     * Get the network configuration of a particular entity.
     *
     * The network configuration consists of all the properties of the entity that
     * affect the network tier price (within a specific region and service).
     *
     * @param entity The entity.
     * @return An optional containing the {@link NetworkConfig}. An empty optional if there is no
     *         ip costs associated with this entity.
     */
    @Nonnull
    Optional<NetworkConfig> getNetworkConfig(@Nonnull ENTITY_CLASS entity);

    /**
     * Get the compute tier configuration of a particular entity.
     *
     * @param entity The entity.
     * @return An optional containing the {@link ComputeTierConfig}. An empty optional if there is
     *         no compute tier information associated with this entity - for example, if it's not
     *         a compute tier.
     */
    @Nonnull
    Optional<ComputeTierConfig> getComputeTierConfig(@Nonnull ENTITY_CLASS entity);

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

    /**
     * A wrapper class around the database configuration of an entity.
     */
    @Immutable
    class DatabaseConfig {
        private final DatabaseEdition edition;
        private final DatabaseEngine engine;

        public DatabaseConfig(final DatabaseEdition edition, final DatabaseEngine engine) {
            this.engine = engine;
            this.edition = edition;
        }

        @Nonnull
        public DatabaseEdition getEdition() {
            return edition;
        }

        @Nonnull
        public DatabaseEngine getEngine() {
            return engine;
        }

        public boolean matchesPriceTableConfig(@Nonnull final DatabaseTierConfigPrice databaseTierConfigPrice) {
            return databaseTierConfigPrice.getDbEdition() == edition &&
                    databaseTierConfigPrice.getDbEngine() == engine;
        }
    }

    /**
     * A wrapper class around the network configuration of an entity.
     */
    @Immutable
    class NetworkConfig {
        private final List<IpAddress> ipAddresses;

        public NetworkConfig(final List<IpAddress> ipAddresses) {
            this.ipAddresses = ipAddresses;
        }

        @Nonnull
        public List<IpAddress> getIPAddresses() {
            return ipAddresses;
        }

        /**
         * @return the number of elastic IPs in this network configuration
         */
        public long getNumElasticIps() {
            return ipAddresses.stream().filter(IpAddress::getIsElastic).count();
        }
    }

    /**
     * A wrapper class around the compute tier configuration of an entity.
     */
    @Immutable
    class ComputeTierConfig {
        /**
         * The number of coupons sold by this compute tier.
         * See: {@link ComputeTierInfo#getNumCoupons()}
         */
        private final int numCoupons;

        public ComputeTierConfig(final int numCoupons) {
            this.numCoupons = numCoupons;
        }

        public int getNumCoupons() {
            return numCoupons;
        }
    }

}
