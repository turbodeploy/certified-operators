package com.vmturbo.topology.graph;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ServiceInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.KubernetesServiceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
/**
 * An implementation of {@link SearchableProps} that copies the minimal required data,
 * used for graph entities that do not want to retain a full
 * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}.
 */

public class ThinSearchableProps implements SearchableProps {

    private final TagIndex tagIndex;

    private final CommodityValueFetcher commodities;

    private static final String UNKNOWN = "UNKNOWN";

    private ThinSearchableProps(@Nonnull final TagIndex tagIndex,
            @Nonnull final CommodityValueFetcher commodities) {
        this.tagIndex = tagIndex;
        this.commodities = commodities;
    }

    @Nonnull
    @Override
    public TagIndex getTagIndex() {
        return tagIndex;
    }

    @Override
    public boolean hasBoughtCommodity(@Nonnull CommodityType commodityType, @Nullable EntityType providerType) {
        return commodities.hasBoughtCommodity(commodityType, providerType);
    }

    @Override
    public float getCommodityCapacity(int type) {
        return commodities.getCommodityCapacity(type);
    }

    @Override
    public float getCommodityUsed(final int type) {
        return commodities.getCommodityUsed(type);
    }

    @Override
    public boolean isHotAddSupported(int commodityType) {
        return commodities.isHotAddSupported(commodityType);
    }

    @Override
    public boolean isHotRemoveSupported(int commodityType) {
        return commodities.isHotRemoveSupported(commodityType);
    }

    /**
     * Helper interface to support commodity value lookups.
     */
    public interface CommodityValueFetcher {
        /**
         * Return capacity.
         *
         * @param type The type of the commodity.
         * @return The capacity of the first found commodity of this type, or -1 if there is no
         *         such commodity.
         */
        float getCommodityCapacity(int type);

        /**
         * Checks whether entity has bought commodity of the specified {@link CommodityType}.
         * Returns {@code true} in case it has such a commodity, otherwise it returns {@code
         * false}.
         *
         * @param commodityType type of the commodity which existence has to be
         *                 verified.
         * @param providerType  type of the provider from which commodity is expected to be bought.
         * @return {@code true} in case entity has a commodity with that type, otherwise
         *                 returns {@code false}.
         */
        boolean hasBoughtCommodity(@Nonnull CommodityType commodityType,
                        @Nullable EntityType providerType);

        /**
         * Return used value.
         *
         * @param type The type of the commodity.
         * @return The used value of the first found commodity of this type, or -1 if there is no
         *         such commodity.
         */
        float getCommodityUsed(int type);

        /**
         * Checks whether the VM supports hot add feature for the selected type of commodity.
         *
         * @param commodityType commodity type number.
         * @return Returns true if the feature is supported, otherwise false.
         */
        boolean isHotAddSupported(int commodityType);

        /**
         * Checks whether the VM supports hot remove feature for the selected type of commodity.
         *
         * @param commodityType commodity type number
         * @return Returns true if the feature is supported, otherwise false.
         */
        boolean isHotRemoveSupported(int commodityType);
    }

    /**
     * Create a new instance.
     *
     * @param tagIndex The {@link TagIndex}.
     * @param commodities A {@link CommodityValueFetcher}. Used to reduce
     * @param entity The {@link TopologyEntityDTO} for the entity.
     * @return The {@link SearchableProps} for the entity, backed by the entity itself.
     */
    @Nonnull
    public static SearchableProps newProps(@Nonnull final TagIndex tagIndex,
            @Nonnull final CommodityValueFetcher commodities,
            @Nonnull final TopologyEntityDTO entity) {
        TypeSpecificInfo info = entity.getTypeSpecificInfo();
        switch (ApiEntityType.fromType(entity.getEntityType())) {
            case PHYSICAL_MACHINE:
                return new ThinPmProps(tagIndex, commodities, info);
            case STORAGE:
                return new ThinStorageProps(tagIndex, commodities, info);
            case VIRTUAL_MACHINE:
                return new ThinVmProps(tagIndex, commodities, info);
            case VIRTUAL_VOLUME:
                return new ThinVolumeProps(tagIndex, commodities, entity);
            case WORKLOAD_CONTROLLER:
                return new ThinWorkloadControllerProps(tagIndex, commodities, info);
            case BUSINESS_ACCOUNT:
                return new ThinBusinessAccountProps(tagIndex, commodities, info);
            case DATABASE_SERVER:
                return new ThinDatabaseServerProps(tagIndex, commodities, entity);
            case DATABASE:
                return new ThinDatabaseProps(tagIndex, commodities, entity);
            case VIRTUAL_MACHINE_SPEC:
                return new ThinVirtualMachineSpecProps(tagIndex, commodities, entity);
            case SERVICE:
                return new ThinServiceProps(tagIndex, commodities, info);
            case COMPUTE_TIER:
                return new ThinComputeTierProps(tagIndex, commodities, entity);
            default:
                return new ThinSearchableProps(tagIndex, commodities);
        }
    }

    /**
     * Physical machine properties.
     */
    public static class ThinPmProps extends ThinSearchableProps implements PmProps {
        private final int numCpus;
        private final String vendor;
        private final String cpuModel;
        private final String model;
        private final String timezone;

        private ThinPmProps(@Nonnull final TagIndex tagIndex,
                @Nonnull final CommodityValueFetcher commodities,
                @Nonnull final TypeSpecificInfo typeSpecificInfo) {
            super(tagIndex, commodities);
            PhysicalMachineInfo pmInfo = typeSpecificInfo.getPhysicalMachine();
            numCpus = pmInfo.getNumCpus();
            vendor = pmInfo.getVendor();
            cpuModel = pmInfo.getCpuModel();
            model = pmInfo.getModel();
            timezone = pmInfo.getTimezone();
        }

        @Override
        public int getNumCpus() {
            return numCpus;
        }

        @Nonnull
        @Override
        public String getVendor() {
            return vendor;
        }

        @Nonnull
        @Override
        public String getCpuModel() {
            return cpuModel;
        }

        @Nonnull
        @Override
        public String getModel() {
            return model;
        }

        @Nonnull
        @Override
        public String getTimezone() {
            return timezone;
        }
    }

    /**
     * Storage properties.
     */
    public static class ThinStorageProps extends ThinSearchableProps implements StorageProps {
        private final boolean isLocal;
        private final StorageType storageType;

        private ThinStorageProps(@Nonnull final TagIndex tagIndex,
                @Nonnull final CommodityValueFetcher commodities,
                @Nonnull final TypeSpecificInfo typeSpecificInfo) {
            super(tagIndex, commodities);
            this.isLocal = typeSpecificInfo.getStorage().getIsLocal();
            this.storageType = typeSpecificInfo.getStorage().getStorageType();
        }

        @Override
        public boolean isLocal() {
            return isLocal;
        }
    }

    /**
     * Virtual machine properties.
     */
    public static class ThinVmProps extends ThinSearchableProps implements VmProps {
        private final List<String> connectedNetworks;
        private final String guesOs;
        private final int numCpus;
        private final int coresPerSocket;
        private final String vendorToolsVersion;
        private final boolean isVdi;

        private ThinVmProps(@Nonnull final TagIndex tagIndex,
                @Nonnull final CommodityValueFetcher commodities,
                @Nonnull final TypeSpecificInfo typeSpecificInfo) {
            super(tagIndex, commodities);
            VirtualMachineInfo vmInfo = typeSpecificInfo.getVirtualMachine();
            connectedNetworks = vmInfo.getConnectedNetworksList();
            guesOs = vmInfo.getGuestOsInfo().getGuestOsName();
            numCpus = vmInfo.getNumCpus();
            coresPerSocket = vmInfo.getCoresPerSocketRatio();
            vendorToolsVersion = vmInfo.getVendorToolsVersion();
            isVdi = vmInfo.hasIsVdi() && vmInfo.getIsVdi();

        }

        @Nonnull
        @Override
        public Collection<String> getConnectedNetworkNames() {
            return connectedNetworks;
        }

        @Nonnull
        @Override
        public String getGuestOsName() {
            return guesOs;
        }

        @Override
        public int getNumCpus() {
            return numCpus;
        }

        @Override
        public int getCoresPerSocket() {
            return coresPerSocket;
        }

        @Override
        public String getVendorToolsVersion() {
            if (!StringUtils.isEmpty(vendorToolsVersion)) {
                return vendorToolsVersion;
            }
            return null;
        }

        @Override
        public boolean isVdi() {
            return isVdi;
        }
    }

    /**
     * Virtual volume properties.
     */
    public static class ThinVolumeProps extends ThinSearchableProps implements VolumeProps {
        private final AttachmentState attachmentState;
        private final boolean encrypted;
        private final boolean ephemeral;
        private final boolean deletable;
        private final Integer daysUnattached;

        private ThinVolumeProps(@Nonnull final TagIndex tagIndex,
                @Nonnull final CommodityValueFetcher commodities,
                @Nonnull final TopologyEntityDTO entity) {
            super(tagIndex, commodities);
            VirtualVolumeInfo vvInfo = entity.getTypeSpecificInfo().getVirtualVolume();
            this.attachmentState = vvInfo.getAttachmentState();
            this.daysUnattached = vvInfo.hasDaysUnattached()
                    ? vvInfo.getDaysUnattached() : null;
            this.encrypted = vvInfo.getEncryption();
            this.ephemeral = vvInfo.getIsEphemeral();
            this.deletable = entity.getAnalysisSettings().getDeletable();
        }

        @Nonnull
        @Override
        public AttachmentState attachmentState() {
            return attachmentState;
        }

        @Override
        public boolean isEncrypted() {
            return encrypted;
        }

        @Override
        public boolean isEphemeral() {
            return ephemeral;
        }

        @Override
        public boolean isDeletable() {
            return deletable;
        }

        @Override
        @Nonnull
        public Optional<Integer> daysUnattached() {
            return Optional.ofNullable(daysUnattached);
        }
    }

    /**
     * Service properties.
     */
    public static class ThinServiceProps extends ThinSearchableProps implements ServiceProps {
        private final TypeSpecificInfo typeSpecificInfo;

        private ThinServiceProps(@Nonnull final TagIndex tagIndex,
                                 @Nonnull final CommodityValueFetcher commodityValueFetcher,
                                 @Nonnull final TypeSpecificInfo typeSpecificInfo) {
            super(tagIndex, commodityValueFetcher);
            this.typeSpecificInfo = typeSpecificInfo;
        }

        @Override
        @Nullable
        public String getKubernetesServiceType() {
            return Optional.of(typeSpecificInfo)
                    .filter(TypeSpecificInfo::hasService)
                    .map(TypeSpecificInfo::getService)
                    .filter(ServiceInfo::hasKubernetesServiceData)
                    .map(ServiceInfo::getKubernetesServiceData)
                    .filter(KubernetesServiceData::hasServiceType)
                    .map(KubernetesServiceData::getServiceType)
                    .map(Enum::name)
                    .orElse(null);
        }
    }

    /**
     * Workload controller properties.
     */
    public static class ThinWorkloadControllerProps extends ThinSearchableProps implements WorkloadControllerProps {
        private final String controllerType;
        private final boolean isCustom;

        private ThinWorkloadControllerProps(@Nonnull final TagIndex tagIndex,
                @Nonnull final CommodityValueFetcher commodities,
                @Nonnull final TypeSpecificInfo typeSpecificInfo) {
            super(tagIndex, commodities);
            WorkloadControllerInfo wcInfo = typeSpecificInfo.getWorkloadController();
            // If wcInfo has custom controller info, then use the customControllerType
            // for searching.
            if (wcInfo.hasCustomControllerInfo()) {
                controllerType = wcInfo.getCustomControllerInfo().getCustomControllerType();
                isCustom = true;
            } else {
                controllerType = wcInfo.getControllerTypeCase().name();
                isCustom = false;
            }
        }

        @Nonnull
        @Override
        public String getControllerType() {
            return controllerType;
        }

        @Override
        public boolean isCustom() {
            return isCustom;
        }
    }

    /**
     * Business account properties.
     */
    public static class ThinBusinessAccountProps extends ThinSearchableProps implements BusinessAccountProps {
        private final boolean hasAssociatedTargetId;
        private final String accountId;

        private ThinBusinessAccountProps(@Nonnull final TagIndex tagIndex,
                @Nonnull final CommodityValueFetcher commodities,
                @Nonnull final TypeSpecificInfo typeSpecificInfo) {
            super(tagIndex, commodities);
            this.hasAssociatedTargetId = typeSpecificInfo.getBusinessAccount().hasAssociatedTargetId();
            this.accountId = typeSpecificInfo.getBusinessAccount().getAccountId();
        }

        @Override
        public boolean hasAssociatedTargetId() {
            return hasAssociatedTargetId;
        }

        @Nonnull
        @Override
        public String getAccountId() {
            return accountId;
        }
    }

    /**
     * Database server properties.
     */
    public static class ThinDatabaseServerProps extends ThinSearchableProps implements DatabaseServerProps {
        private final DatabaseEngine databaseEngine;
        private final String databaseEdition;
        private final String databaseVersion;
        private final String storageEncryption;
        private final String storageAutoscaling;
        private final String performanceInsights;
        private final String clusterRole;
        private final String storageTier;


        private ThinDatabaseServerProps(@Nonnull final TagIndex tagIndex,
                                         @Nonnull final CommodityValueFetcher commodities,
                                         @Nonnull final TopologyEntityDTO entityDTO) {
            super(tagIndex, commodities);
            final TypeSpecificInfo typeSpecificInfo = entityDTO.getTypeSpecificInfo();
            final boolean hasDatabaseEngine = typeSpecificInfo.getDatabase().hasEngine();
            final boolean hasDatabaseEdition = typeSpecificInfo.getDatabase().hasEdition()
                    && typeSpecificInfo.getDatabase().getEdition() != DatabaseEdition.NONE;
            final boolean hasRawEdition = typeSpecificInfo.getDatabase().hasRawEdition();
            final boolean hasDatabaseVersion = typeSpecificInfo.getDatabase().hasVersion();
            databaseEngine = hasDatabaseEngine ? typeSpecificInfo.getDatabase().getEngine()
                    : DatabaseEngine.UNKNOWN;
            if (hasDatabaseEdition) {
                databaseEdition = typeSpecificInfo.getDatabase().getEdition().name();
            } else if (hasRawEdition) {
                databaseEdition = typeSpecificInfo.getDatabase().getRawEdition();
            } else {
                databaseEdition = DatabaseEdition.NONE.name();
            }
            databaseVersion = hasDatabaseVersion ? typeSpecificInfo.getDatabase().getVersion()
                    : UNKNOWN;
            storageEncryption = entityDTO.getEntityPropertyMapOrDefault(
                    StringConstants.STORAGE_ENCRYPTION, null);
            storageAutoscaling = entityDTO.getEntityPropertyMapOrDefault(
                    StringConstants.STORAGE_AUTOSCALING, null);
            performanceInsights = entityDTO.getEntityPropertyMapOrDefault(
                    StringConstants.AWS_PERFORMANCE_INSIGHTS, null);
            clusterRole = entityDTO.getEntityPropertyMapOrDefault(StringConstants.CLUSTER_ROLE,
                    null);
            storageTier = entityDTO.getEntityPropertyMapOrDefault(StringConstants.DBS_STORAGE_TIER,
                    null);
        }

        @Nonnull
        @Override
        public DatabaseEngine getDatabaseEngine() {
            return databaseEngine;
        }

        @Nonnull
        @Override
        public String getDatabaseEdition() {
            return databaseEdition;
        }

        @Nonnull
        @Override
        public String getDatabaseVersion() {
            return databaseVersion;
        }

        @Override
        public String getStorageEncryption() {
            return storageEncryption;
        }

        @Override
        public String getStorageAutoscaling() {
            return storageAutoscaling;
        }

        @Override
        public String getPerformanceInsights() {
            return performanceInsights;
        }

        @Override
        public String getClusterRole() {
            return clusterRole;
        }

        @Override
        public String getStorageTier() {
            return storageTier;
        }
    }

    /**
     * Database properties.
     */
    public static class ThinDatabaseProps extends ThinSearchableProps implements DatabaseProps {
        private final String replicationRole;
        private final String pricingModel;
        private final String serviceTier;

        private ThinDatabaseProps(@Nonnull final TagIndex tagIndex,
                                  @Nonnull final CommodityValueFetcher commodities,
                                  @Nonnull final TopologyEntityDTO entityDTO) {
            super(tagIndex, commodities);
            replicationRole = entityDTO.getEntityPropertyMapOrDefault(StringConstants.DB_REPLICATION_ROLE,
                    null);
            pricingModel = entityDTO.getEntityPropertyMapOrDefault(StringConstants.DB_PRICING_MODEL,
                    null);
            serviceTier = entityDTO.getEntityPropertyMapOrDefault(StringConstants.DB_SERVICE_TIER,
                    null);
        }

        @Override
        public String getReplicationRole() {
            return replicationRole;
        }

        @Override
        public String getPricingModel() {
            return pricingModel;
        }

        @Override
        public String getServiceTier() {
            return serviceTier;
        }
    }

    /**
     * VmSpec properties.
     */
    public static class ThinVirtualMachineSpecProps extends ThinSearchableProps implements VirtualMachineSpecProps {
        private final String tier;
        private final Integer appCount;
        private final Integer daysEmpty;

        private ThinVirtualMachineSpecProps(@Nonnull final TagIndex tagIndex,
                @Nonnull final CommodityValueFetcher commodities,
                @Nonnull final TopologyEntityDTO entityDTO) {
            super(tagIndex, commodities);
            final TypeSpecificInfo typeSpecificInfo = entityDTO.getTypeSpecificInfo();
            final ApplicationServiceInfo applicationServiceInfo = typeSpecificInfo.getApplicationService();
            final boolean hasTier = applicationServiceInfo.hasTier();
            final boolean hasAppCount = applicationServiceInfo.hasAppCount();
            tier = hasTier ? applicationServiceInfo.getTier().name() : UNKNOWN;
            appCount = hasAppCount ? applicationServiceInfo.getAppCount() : -1;
            this.daysEmpty = applicationServiceInfo.hasDaysEmpty()
                    ? applicationServiceInfo.getDaysEmpty() : null;
        }

        @Override
        public String getTier() {
            return tier;
        }

        @Override
        public Integer getAppCount() {
            return appCount;
        }

        @Override
        @Nonnull
        public Optional<Integer> getDaysEmpty() {
            return Optional.ofNullable(daysEmpty);
        }
    }

    /**
     * Searchable ComputeTier properties.
     */
    public static class ThinComputeTierProps extends ThinSearchableProps implements ComputeTierProps {
        private final Set<EntityType> consumerEntityTypes;

        /**
         * Creates a new ThinComputeTierProps for a given topology entity.
         *
         * @param tagIndex The tag index
         * @param commodities The commodity value fetcher
         * @param entity The topology entity
         */
        public ThinComputeTierProps(@Nonnull final TagIndex tagIndex,
                @Nonnull final CommodityValueFetcher commodities,
                @Nonnull final TopologyEntityDTO entity) {
            super(tagIndex, commodities);
            consumerEntityTypes = getConsumerEntityTypes(entity);
        }

        @Override
        public Set<EntityType> getConsumerEntityTypes() {
            return consumerEntityTypes;
        }

        private Set<EntityType> getConsumerEntityTypes(TopologyEntityDTO entity) {
            return entity.getOrigin()
                    .getDiscoveryOrigin()
                    .getDiscoveredTargetDataMap()
                    .values()
                    .stream()
                    .map(e -> ComputeTierProps.getConsumerEntityType(e.getVendorId()))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
        }
    }
}
