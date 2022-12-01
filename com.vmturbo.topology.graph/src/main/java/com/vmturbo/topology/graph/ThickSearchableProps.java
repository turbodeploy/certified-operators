package com.vmturbo.topology.graph;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl.HotResizeInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.ApplicationServiceInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.ServiceInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.VirtualMachineInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.VirtualVolumeInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.WorkloadControllerInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoView;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonPOJO.EntityImpl.KubernetesServiceDataView;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;
/**
 * An implementation of {@link SearchableProps} backed by a full
 * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}.
 *
 * <p/>This avoids duplicating lots of references for graph entities that contain a full
 * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}.
 */

public class ThickSearchableProps implements SearchableProps {
    protected final TopologyEntityView entity;

    private static final String UNKNOWN = "UNKNOWN";

    private ThickSearchableProps(TopologyEntityView entity) {
        this.entity = entity;
    }

    /**
     * Create a new instance.
     *
     * @param entity The {@link TopologyEntityView}.
     * @return The {@link SearchableProps} for the entity, backed by the entity itself.
     */
    @Nonnull
    public static SearchableProps newProps(@Nonnull final TopologyEntityView entity) {
        switch (ApiEntityType.fromType(entity.getEntityType())) {
            case PHYSICAL_MACHINE:
                return new ThickPmProps(entity);
            case STORAGE:
                return new ThickStorageProps(entity);
            case VIRTUAL_MACHINE:
                return new ThickVmProps(entity);
            case VIRTUAL_VOLUME:
                return new ThickVolumeProps(entity);
            case WORKLOAD_CONTROLLER:
                return new ThickWorkloadControllerProps(entity);
            case BUSINESS_ACCOUNT:
                return new ThickBusinessAccountProps(entity);
            case DATABASE_SERVER:
                return new ThickDatabaseServerProps(entity);
            case DATABASE:
                return new ThickDatabaseProps(entity);
            case VIRTUAL_MACHINE_SPEC:
                return new ThickVirtualMachineSpecProps(entity);
            case APPLICATION_COMPONENT_SPEC:
                return new ThickAppComponentSpecProps(entity);
            case SERVICE:
                return new ThickServiceProps(entity);
            case COMPUTE_TIER:
                return new ThickComputeTierProps(entity);
            default:
                return new ThickSearchableProps(entity);
        }
    }

    @Override
    @Nonnull
    public TagIndex getTagIndex() {
        // Note - this is a little bit heavyweight,
        // but we want to guard against tag changes during stitching,
        // so we return a "fresh" index every time.
        return DefaultTagIndex.singleEntity(entity.getOid(), entity.getTags());
    }

    @Override
    public boolean hasBoughtCommodity(@Nonnull CommodityType commodityType,
                    @Nullable EntityType providerType) {
        return entity.getCommoditiesBoughtFromProvidersList().stream().filter(cbfp ->
                                        providerType == null
                                                        || providerType.getNumber() == cbfp.getProviderEntityType())
                        .flatMap(cbfp -> cbfp.getCommodityBoughtList().stream()
                                        .map(cb -> cb.getCommodityType().getType())
                                        .filter(SEARCHABLE_COMM_TYPES::contains))
                        .anyMatch(type -> type == commodityType.getNumber());
    }

    @Override
    public float getCommodityCapacity(final int type) {
        return (float)entity.getCommoditySoldListList().stream()
            .filter(comm -> comm.getCommodityType().getType() == type)
            .filter(CommoditySoldView::hasCapacity)
            .mapToDouble(CommoditySoldView::getCapacity)
            .findFirst().orElse(-1);
    }

    @Override
    public float getCommodityUsed(final int type) {
        return (float)entity.getCommoditySoldListList().stream()
            .filter(comm -> comm.getCommodityType().getType() == type)
            .filter(CommoditySoldView::hasUsed)
            .mapToDouble(CommoditySoldView::getUsed)
            .findFirst().orElse(-1);
    }

    @Override
    public boolean isHotAddSupported(int commodityType) {
        return isHotChangeSupported(commodityType, HotResizeInfoView::getHotAddSupported);
    }

    @Override
    public boolean isHotRemoveSupported(int commodityType) {
        return isHotChangeSupported(commodityType, HotResizeInfoView::getHotRemoveSupported);
    }

    private boolean isHotChangeSupported(int commodityType, Predicate<HotResizeInfoView> predicate) {
        return entity.getCommoditySoldListList()
                .stream()
                .filter(c -> c.getCommodityType().getType() == commodityType)
                .filter(CommoditySoldView::hasHotResizeInfo)
                .map(CommoditySoldView::getHotResizeInfo)
                .anyMatch(predicate);
    }

    /**
     * Physical machine properties.
     */
    public static class ThickPmProps extends ThickSearchableProps implements PmProps {
        private ThickPmProps(TopologyEntityView entity) {
            super(entity);
        }

        @Override
        public int getNumCpus() {
            return entity.getTypeSpecificInfo().getPhysicalMachine().getNumCpus();
        }

        @Override
        @Nonnull
        public String getVendor() {
            return entity.getTypeSpecificInfo().getPhysicalMachine().getVendor();
        }

        @Override
        @Nonnull
        public String getCpuModel() {
            return entity.getTypeSpecificInfo().getPhysicalMachine().getCpuModel();
        }

        @Override
        @Nonnull
        public String getModel() {
            return entity.getTypeSpecificInfo().getPhysicalMachine().getModel();
        }

        @Override
        @Nonnull
        public String getTimezone() {
            return entity.getTypeSpecificInfo().getPhysicalMachine().getTimezone();
        }
    }

    /**
     * Storage properties.
     */
    public static class ThickStorageProps extends ThickSearchableProps implements StorageProps {
        private ThickStorageProps(TopologyEntityView entity) {
            super(entity);
        }

        @Override
        public boolean isLocal() {
            return entity.getTypeSpecificInfo().getStorage().getIsLocal();
        }
    }

    /**
     * Virtual machine properties.
     */
    public static class ThickVmProps extends ThickSearchableProps implements VmProps {
        private ThickVmProps(TopologyEntityView entity) {
            super(entity);
        }

        @Override
        @Nonnull
        public Collection<String> getConnectedNetworkNames() {
            return entity.getTypeSpecificInfo().getVirtualMachine().getConnectedNetworksList();
        }

        @Override
        @Nonnull
        public String getGuestOsName() {
            return entity.getTypeSpecificInfo().getVirtualMachine().getGuestOsInfo().getGuestOsName();
        }

        @Override
        public int getNumCpus() {
            return entity.getTypeSpecificInfo().getVirtualMachine().getNumCpus();
        }

        @Override
        public int getCoresPerSocket() {
            return entity.getTypeSpecificInfo().getVirtualMachine().getCoresPerSocketRatio();
        }

        @Override
        public String getVendorToolsVersion() {
            final String vendorToolsVersion = entity.getTypeSpecificInfo().getVirtualMachine().getVendorToolsVersion();
            if (!StringUtils.isEmpty(vendorToolsVersion)) {
                return vendorToolsVersion;
            }
            return null;
        }

        @Override
        public boolean isVdi() {
            final VirtualMachineInfoView vmInfo = entity.getTypeSpecificInfo().getVirtualMachine();
            return vmInfo.hasIsVdi() && vmInfo.getIsVdi();
        }
    }

    /**
     * Volume properties.
     */
    public static class ThickVolumeProps extends ThickSearchableProps implements VolumeProps {
        private ThickVolumeProps(TopologyEntityView entity) {
            super(entity);
        }

        @Override
        @Nonnull
        public AttachmentState attachmentState() {
            return entity.getTypeSpecificInfo().getVirtualVolume().getAttachmentState();
        }

        @Override
        public boolean isEncrypted() {
            return entity.getTypeSpecificInfo().getVirtualVolume().getEncryption();
        }

        @Override
        public boolean isEphemeral() {
            return entity.getTypeSpecificInfo().getVirtualVolume().getIsEphemeral();
        }

        @Override
        public boolean isDeletable() {
            return entity.getAnalysisSettings().getDeletable();
        }

        @Override
        @Nonnull
        public Optional<Integer> daysUnattached() {
            VirtualVolumeInfoView volumeInfoView = entity.getTypeSpecificInfo().getVirtualVolume();
            return volumeInfoView.hasDaysUnattached()
                    ? Optional.of(volumeInfoView.getDaysUnattached()) : Optional.empty();
        }
    }

    /**
     * Service properties.
     */
    public static class ThickServiceProps extends ThickSearchableProps implements ServiceProps {
        private ThickServiceProps(TopologyEntityView entity) {
            super(entity);
        }

        @Override
        @Nullable
        public String getKubernetesServiceType() {
            return Optional.of(entity.getTypeSpecificInfo())
                    .filter(TypeSpecificInfoView::hasService)
                    .map(TypeSpecificInfoView::getService)
                    .filter(ServiceInfoView::hasKubernetesServiceData)
                    .map(ServiceInfoView::getKubernetesServiceData)
                    .filter(KubernetesServiceDataView::hasServiceType)
                    .map(KubernetesServiceDataView::getServiceType)
                    .map(Enum::name)
                    .orElse(null);
        }
    }

    /**
     * Workload controller properties.
     */
    public static class ThickWorkloadControllerProps extends ThickSearchableProps implements WorkloadControllerProps {
        private ThickWorkloadControllerProps(TopologyEntityView entity) {
            super(entity);
        }

        @Override
        @Nonnull
        public String getControllerType() {
            WorkloadControllerInfoView wcInfo = entity.getTypeSpecificInfo().getWorkloadController();
            if (wcInfo.hasCustomControllerInfo()) {
                return wcInfo.getCustomControllerInfo().getCustomControllerType();
            } else {
                return wcInfo.getControllerTypeCase().name();
            }
        }

        @Override
        public boolean isCustom() {
            return entity.getTypeSpecificInfo().getWorkloadController().hasCustomControllerInfo();
        }
    }

    /**
     * Business account properties.
     */
    public static class ThickBusinessAccountProps extends ThickSearchableProps implements BusinessAccountProps {

        private ThickBusinessAccountProps(TopologyEntityView entity) {
            super(entity);
        }

        @Override
        public boolean hasAssociatedTargetId() {
            return entity.getTypeSpecificInfo().getBusinessAccount().hasAssociatedTargetId();
        }

        @Override
        @Nonnull
        public String getAccountId() {
            return entity.getTypeSpecificInfo().getBusinessAccount().getAccountId();
        }
    }

    /**
     * Database server properties.
     */
    public static class ThickDatabaseServerProps extends ThickSearchableProps implements DatabaseServerProps {

        private ThickDatabaseServerProps(TopologyEntityView entity) {
            super(entity);
        }

        private boolean hasDatabaseEngine() {
            return entity.getTypeSpecificInfo().getDatabase().hasEngine();
        }

        private boolean hasDatabaseEdition() {
            return entity.getTypeSpecificInfo().getDatabase().hasEdition();
        }

        private boolean hasDatabaseRawEdition() {
            return entity.getTypeSpecificInfo().getDatabase().hasRawEdition();
        }

        private boolean hasDatabaseVersion() {
            return entity.getTypeSpecificInfo().getDatabase().hasVersion();
        }

        @Override
        @Nonnull
        public DatabaseEngine getDatabaseEngine() {
            if (hasDatabaseEngine()) {
                return entity.getTypeSpecificInfo().getDatabase().getEngine();
            }
            return DatabaseEngine.UNKNOWN;
        }

        @Override
        @Nonnull
        public String getDatabaseEdition() {
            if (hasDatabaseEdition()) {
                return entity.getTypeSpecificInfo().getDatabase().getEdition().name();
            } else if (hasDatabaseRawEdition()) {
                return entity.getTypeSpecificInfo().getDatabase().getRawEdition();
            }
            return DatabaseEdition.NONE.name();
        }

        @Override
        @Nonnull
        public String getDatabaseVersion() {
            if (hasDatabaseVersion()) {
                return entity.getTypeSpecificInfo().getDatabase().getVersion();
            }
            return UNKNOWN;
        }

        @Override
        public String getStorageEncryption() {
            return entity.getEntityPropertyMapOrDefault(StringConstants.STORAGE_ENCRYPTION,
                    null);
        }

        @Override
        public String getStorageAutoscaling() {
            return entity.getEntityPropertyMapOrDefault(StringConstants.STORAGE_AUTOSCALING,
                    null);
        }

        @Override
        public String getPerformanceInsights() {
            return entity.getEntityPropertyMapOrDefault(
                    StringConstants.AWS_PERFORMANCE_INSIGHTS, null);
        }

        @Override
        public String getClusterRole() {
            return entity.getEntityPropertyMapOrDefault(StringConstants.CLUSTER_ROLE, null);
        }

        @Override
        public String getStorageTier() {
            return entity.getEntityPropertyMapOrDefault(StringConstants.DBS_STORAGE_TIER, null);
        }
    }

    /**
     * Database properties.
     */
    public static class ThickDatabaseProps extends ThickSearchableProps implements DatabaseProps {

        private ThickDatabaseProps(@Nonnull final TopologyEntityView entity) {
            super(entity);
        }

        @Override
        public String getReplicationRole() {
            return entity.getEntityPropertyMapOrDefault(StringConstants.DB_REPLICATION_ROLE, null);
        }

        @Override
        public String getPricingModel() {
            return entity.getEntityPropertyMapOrDefault(StringConstants.DB_PRICING_MODEL, null);
        }

        @Override
        public String getServiceTier() {
            return entity.getEntityPropertyMapOrDefault(StringConstants.DB_SERVICE_TIER, null);
        }
    }

    /**
     * VmSpec properties.
     */
    public static class ThickVirtualMachineSpecProps extends ThickSearchableProps implements VirtualMachineSpecProps {

        private ThickVirtualMachineSpecProps(@Nonnull final TopologyEntityView entity) {
            super(entity);
        }

        @Override
        public String getTier() {
            return entity.getTypeSpecificInfo().getApplicationService().getTier().name();
        }

        @Override
        public Integer getAppCount() {
            return entity.getTypeSpecificInfo().getApplicationService().getAppCount();
        }

        @Override
        @Nonnull
        public Optional<Integer> getDaysEmpty() {
            ApplicationServiceInfoView appSvcView = entity.getTypeSpecificInfo().getApplicationService();
            return appSvcView.hasDaysEmpty()
                    ? Optional.of(appSvcView.getDaysEmpty()) : Optional.empty();
        }
    }

    /**
     * App/Application Component Spec Properties.
     */
    public static class ThickAppComponentSpecProps extends ThickSearchableProps implements AppComponentSpecProps {

        private ThickAppComponentSpecProps(@Nonnull final TopologyEntityView entity) {
            super(entity);
        }

        @Override
        public Integer getHybridConnectionCount() {
            if (entity.hasTypeSpecificInfo() & entity.getTypeSpecificInfo().hasCloudApplication()) {
                return entity.getTypeSpecificInfo()
                        .getCloudApplication()
                        .getHybridConnectionCount();
            } else {
                return 0;
            }
        }

        @Override
        public Integer getDeploymentSlotCount() {
            if (entity.hasTypeSpecificInfo() && entity.getTypeSpecificInfo()
                    .hasCloudApplication()) {
                return entity.getTypeSpecificInfo().getCloudApplication().getDeploymentSlotCount();
            } else {
                return 0;
            }
        }
    }

    /**
     * Searchable ComputeTier properties.
     */
    public static class ThickComputeTierProps extends ThickSearchableProps implements ComputeTierProps {
        private final Set<EntityType> consumerEntityTypes;

        private ThickComputeTierProps(@Nonnull final TopologyEntityView entity) {
            super(entity);
            consumerEntityTypes = getConsumerEntityTypes(entity);
        }

        @Override
        public Set<EntityType> getConsumerEntityTypes() {
            return consumerEntityTypes;
        }

        private Set<EntityType> getConsumerEntityTypes(TopologyEntityView entity) {
            return entity.getOrigin()
                    .getDiscoveryOrigin()
                    .getDiscoveredTargetDataMap()
                    .values()
                    .stream()
                    .map(e -> ComputeTierProps.getConsumerEntityType(e.getVendorId()))
                    .collect(Collectors.toSet());
        }
    }

}
