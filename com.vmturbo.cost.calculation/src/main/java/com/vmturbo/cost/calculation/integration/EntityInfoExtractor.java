package com.vmturbo.cost.calculation.integration;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.immutables.value.Value;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierConfigPrice;

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

    String getName(@Nonnull final ENTITY_CLASS entity);

    /**
     * Get the state of an entity.
     *
     * @param entity Entity to get the state for.
     * @return {@link EntityState} of the entity.
     */
    @Nonnull
    EntityState getEntityState(@Nonnull ENTITY_CLASS entity);

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
     * Get the volume configuration of a particular entity.
     *
     * @param entity The entity.
     * @return An optional containing the {@link VirtualVolumeConfig}. An empty optional if there is
     *         no volume configuration associated with this entity - e.g. if the entity is not a
     *         virtual volume.
     */
    @Nonnull
    Optional<VirtualVolumeConfig> getVolumeConfig(@Nonnull ENTITY_CLASS entity);

    /**
     * Get storage amount for DB or DBS  entity (which are referred to as RDB).
     *
     * @param entity The entity.
     * @return An Optional StorageAmount in float.
     */
    @Nonnull
    Optional<Float> getRDBCommodityCapacity(@Nonnull ENTITY_CLASS entity, CommodityType commodityType);

    /*
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
        private final VMBillingType billingType;
        private final int numCores;
        private final EntityDTO.LicenseModel licenseModel;
        private final Map<com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType, Double> pricedCommoditiesBought;

        public ComputeConfig(final OSType os, final Tenancy tenancy, final VMBillingType billingType,
                             final int numCores, final EntityDTO.LicenseModel licenseModel,
                             final Map<com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType, Double> pricedCommoditiesBought) {
            this.os = os;
            this.tenancy = tenancy;
            this.billingType = billingType;
            this.numCores = numCores;
            this.licenseModel = licenseModel;
            this.pricedCommoditiesBought = pricedCommoditiesBought;
        }

        @Nonnull
        public OSType getOs() {
            return os;
        }

        @Nonnull
        public Tenancy getTenancy() {
            return tenancy;
        }

        @Nonnull
        public VMBillingType getBillingType() {
            return billingType;
        }

        public int getNumCores() {
            return numCores;
        }

        public EntityDTO.LicenseModel getLicenseModel() {
            return licenseModel;
        }

        @Nonnull
        public Map<com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType, Double> getPricedCommoditiesBought() {
            return pricedCommoditiesBought;
        }
    }

    /**
     * A wrapper class around the database configuration of an entity.
     */
    @Immutable
    class DatabaseConfig {
        private final DatabaseEdition edition;
        private final DatabaseEngine engine;
        private final LicenseModel licenseModel;
        private final DeploymentType deploymentType;
        private final Double hourlyBilledOps;

        public DatabaseConfig(
                final DatabaseEdition edition,
                final DatabaseEngine engine,
                @Nonnull  LicenseModel licenseModel,
                @Nullable DeploymentType deploymentType,
                @Nullable Double hourlyBilledOps) {
            this.engine = engine;
            this.edition = edition;
            this.licenseModel = licenseModel;
            this.deploymentType = deploymentType;
            this.hourlyBilledOps = hourlyBilledOps;
        }

        public DatabaseConfig(final DatabaseEdition edition,
                              final DatabaseEngine engine,
                              @Nonnull  LicenseModel licenseModel,
                              @Nullable DeploymentType deploymentType) {
            this(edition, engine, licenseModel, deploymentType, null);
        }

        @Nonnull
        public DatabaseEdition getEdition() {
            return edition;
        }

        @Nonnull
        public DatabaseEngine getEngine() {
            return engine;
        }

        @Nonnull
        public Optional<LicenseModel> getLicenseModel() {
            return Optional.ofNullable(licenseModel);
        }

        @Nonnull
        public Optional<DeploymentType> getDeploymentType() {
            return Optional.ofNullable(deploymentType);
        }

        @Nullable
        public Double getHourlyBilledOps() {
            return hourlyBilledOps;
        }

        public boolean matchesPriceTableConfig(@Nonnull final DatabaseTierConfigPrice databaseTierConfigPrice) {
            DeploymentType otherDeploymentType = databaseTierConfigPrice.hasDbDeploymentType() ?
                databaseTierConfigPrice.getDbDeploymentType() : null;
            LicenseModel otherLicenseModel = databaseTierConfigPrice.hasDbLicenseModel() ?
                databaseTierConfigPrice.getDbLicenseModel() : null;

            return databaseTierConfigPrice.getDbEdition() == edition &&
                    databaseTierConfigPrice.getDbEngine() == engine &&
                    otherLicenseModel == licenseModel &&
                    otherDeploymentType == deploymentType;
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
    @Style(visibility = ImplementationVisibility.PACKAGE,
            overshadowImplementation = true,
            depluralize = true)
    @Value.Immutable
    interface ComputeTierConfig {

        long computeTierOid();

        /**
         * The number of coupons sold by this compute tier.
         * See: {@link ComputeTierInfo#getNumCoupons()}
         */
        double numCoupons();

        /**
         * The number of cores of this compute tier.
         * See: {@link ComputeTierInfo#getNumCores()}
         */
        int numCores();

        /**
         * Determine if this compute tier support burstableCPUs.
         * See: {@link ComputeTierInfo#getBurstableCPU()}
         */
        boolean isBurstableCPU();

        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        class Builder extends ImmutableComputeTierConfig.Builder {}
    }

    /**
     * A wrapper class around the volume configuration of an entity.
     */
    @Immutable
    class VirtualVolumeConfig {
        private final float accessCapacityMillionIops;
        private final float amountCapacityMb;
        private final float ioThroughputCapacityMBps;
        private final float hourlyBilledOps;
        private final boolean isEphemeral;
        private final RedundancyType redundancyType;

        public VirtualVolumeConfig(final float accessCapacityMillionIops,
                                   final float amountCapacityMb,
                                   final float ioThroughputCapacityMBps,
                                   float hourlyBilledOps,
                                   final boolean isEphemeral,
                                   @Nullable final RedundancyType redundancyType) {
            this.accessCapacityMillionIops = accessCapacityMillionIops;
            this.amountCapacityMb = amountCapacityMb;
            this.ioThroughputCapacityMBps = ioThroughputCapacityMBps;
            this.hourlyBilledOps = hourlyBilledOps;
            this.isEphemeral = isEphemeral;
            this.redundancyType = redundancyType;
        }

        public float getAccessCapacityMillionIops() {
            return accessCapacityMillionIops;
        }

        public float getAmountCapacityGb() {
            return amountCapacityMb / 1024;
        }

        public float getIoThroughputCapacityMBps() {
            return ioThroughputCapacityMBps;
        }

        /**
         * Get average number of billed operations per hour (applies to AWS Magnetic (Standard)
         * volumes).
         *
         * @return Average number of billed operations per hour.
         */
        public float getHourlyBilledOps() {
            return hourlyBilledOps;
        }

        public boolean isEphemeral() {
            return isEphemeral;
        }

        @Nullable
        public RedundancyType getRedundancyType() {
            return redundancyType;
        }
    }
}
