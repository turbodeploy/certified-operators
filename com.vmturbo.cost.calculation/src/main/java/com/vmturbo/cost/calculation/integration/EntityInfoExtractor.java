package com.vmturbo.cost.calculation.integration;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
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
     * Get storage amount for DB entity.
     *
     * @param entity The entity.
     * @return An Optional StorageAmount in float.
     */
    @Nonnull
    Optional<Float> getDBStorageCapacity(@Nonnull ENTITY_CLASS entity);

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

        public ComputeConfig(final OSType os, final Tenancy tenancy,
                        final VMBillingType billingType, final int numCores) {
            this(os, tenancy, billingType, numCores, EntityDTO.LicenseModel.LICENSE_INCLUDED);
        }

        public ComputeConfig(final OSType os, final Tenancy tenancy, final VMBillingType billingType,
                             final int numCores, final EntityDTO.LicenseModel licenseModel) {
            this.os = os;
            this.tenancy = tenancy;
            this.billingType = billingType;
            this.numCores = numCores;
            this.licenseModel = licenseModel;
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
        private final LicenseModel licenseModel;
        private final DeploymentType deploymentType;

        public DatabaseConfig(final DatabaseEdition edition, final DatabaseEngine engine,
                @Nonnull  LicenseModel licenseModel, @Nullable DeploymentType deploymentType) {
            this.engine = engine;
            this.edition = edition;
            this.licenseModel = licenseModel;
            this.deploymentType = deploymentType;
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
    @Immutable
    class ComputeTierConfig {
        /**
         * The number of coupons sold by this compute tier.
         * See: {@link ComputeTierInfo#getNumCoupons()}
         */
        private final int numCoupons;

        /**
         * The number of cores of this compute tier.
         * See: {@link ComputeTierInfo#getNumCores()}
         */
        private final int numCores;

        /**
         * Determine if this compute tier support burstableCPUs.
         * See: {@link ComputeTierInfo#getBurstableCPU()}
         */
        private final boolean burstableCPU;

        public ComputeTierConfig(final int numCoupons, final int numCores, final boolean burstableCPU) {
            this.numCoupons = numCoupons;
            this.numCores = numCores;
            this.burstableCPU = burstableCPU;
        }

        public int getNumCoupons() {
            return numCoupons;
        }

        public int getNumCores() {
            return numCores;
        }

        public boolean isBurstableCPU() {
            return burstableCPU;
        }
    }

    /**
     * A wrapper class around the volume configuration of an entity.
     */
    @Immutable
    class VirtualVolumeConfig {
        private final float accesCapacityMillionIops;
        private final float amountCapacityMb;
        private final float ioThroughputCapacityMBps;
        private final boolean isEphemeral;
        private final RedundancyType redundancyType;

        public VirtualVolumeConfig(final float accesCapacityMillionIops,
                                   final float amountCapacityMb,
                                   final float ioThroughputCapacityMBps,
                                   final boolean isEphemeral,
                                   @Nullable final RedundancyType redundancyType) {
            this.accesCapacityMillionIops = accesCapacityMillionIops;
            this.amountCapacityMb = amountCapacityMb;
            this.ioThroughputCapacityMBps = ioThroughputCapacityMBps;
            this.isEphemeral = isEphemeral;
            this.redundancyType = redundancyType;
        }

        public float getAccessCapacityMillionIops() {
            return accesCapacityMillionIops;
        }

        public float getAmountCapacityGb() {
            return amountCapacityMb / 1024;
        }

        public float getIoThroughputCapacityMBps() {
            return ioThroughputCapacityMBps;
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
