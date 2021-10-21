package com.vmturbo.topology.graph;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ServiceInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.KubernetesServiceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
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
    protected final TopologyEntityDTOOrBuilder entityOrBldr;

    private static final String UNKNOWN = "UNKNOWN";

    private ThickSearchableProps(TopologyEntityDTOOrBuilder entityOrBldr) {
        this.entityOrBldr = entityOrBldr;
    }

    /**
     * Create a new instance.
     *
     * @param entityOrBldr The {@link TopologyEntityDTOOrBuilder}.
     * @return The {@link SearchableProps} for the entity, backed by the entity itself.
     */
    @Nonnull
    public static SearchableProps newProps(@Nonnull final TopologyEntityDTOOrBuilder entityOrBldr) {
        switch (ApiEntityType.fromType(entityOrBldr.getEntityType())) {
            case PHYSICAL_MACHINE:
                return new ThickPmProps(entityOrBldr);
            case STORAGE:
                return new ThickStorageProps(entityOrBldr);
            case VIRTUAL_MACHINE:
                return new ThickVmProps(entityOrBldr);
            case VIRTUAL_VOLUME:
                return new ThickVolumeProps(entityOrBldr);
            case WORKLOAD_CONTROLLER:
                return new ThickWorkloadControllerProps(entityOrBldr);
            case BUSINESS_ACCOUNT:
                return new ThickBusinessAccountProps(entityOrBldr);
            case DATABASE_SERVER:
                return new ThickDatabaseServerProps(entityOrBldr);
            case SERVICE:
                return new ThickServiceProps(entityOrBldr);
            default:
                return new ThickSearchableProps(entityOrBldr);
        }
    }

    @Override
    @Nonnull
    public TagIndex getTagIndex() {
        // Note - this is a little bit heavyweight,
        // but we want to guard against tag changes during stitching,
        // so we return a "fresh" index every time.
        return DefaultTagIndex.singleEntity(entityOrBldr.getOid(), entityOrBldr.getTags());
    }

    @Override
    public float getCommodityCapacity(final int type) {
        return (float)entityOrBldr.getCommoditySoldListList().stream()
            .filter(comm -> comm.getCommodityType().getType() == type)
            .filter(CommoditySoldDTO::hasCapacity)
            .mapToDouble(CommoditySoldDTO::getCapacity)
            .findFirst().orElse(-1);
    }

    @Override
    public float getCommodityUsed(final int type) {
        return (float)entityOrBldr.getCommoditySoldListList().stream()
            .filter(comm -> comm.getCommodityType().getType() == type)
            .filter(CommoditySoldDTO::hasUsed)
            .mapToDouble(CommoditySoldDTO::getUsed)
            .findFirst().orElse(-1);
    }

    @Override
    public boolean isHotAddSupported(int commodityType) {
        return isHotChangeSupported(commodityType, HotResizeInfo::getHotAddSupported);
    }

    @Override
    public boolean isHotRemoveSupported(int commodityType) {
        return isHotChangeSupported(commodityType, HotResizeInfo::getHotRemoveSupported);
    }

    private boolean isHotChangeSupported(int commodityType, Predicate<HotResizeInfo> predicate) {
        return entityOrBldr.getCommoditySoldListList()
                .stream()
                .filter(c -> c.getCommodityType().getType() == commodityType)
                .filter(CommoditySoldDTO::hasHotResizeInfo)
                .map(CommoditySoldDTO::getHotResizeInfo)
                .anyMatch(predicate);
    }

    /**
     * Physical machine properties.
     */
    public static class ThickPmProps extends ThickSearchableProps implements PmProps {
        private ThickPmProps(TopologyEntityDTOOrBuilder entityOrBldr) {
            super(entityOrBldr);
        }

        @Override
        public int getNumCpus() {
            return entityOrBldr.getTypeSpecificInfo().getPhysicalMachine().getNumCpus();
        }

        @Override
        @Nonnull
        public String getVendor() {
            return entityOrBldr.getTypeSpecificInfo().getPhysicalMachine().getVendor();
        }

        @Override
        @Nonnull
        public String getCpuModel() {
            return entityOrBldr.getTypeSpecificInfo().getPhysicalMachine().getCpuModel();
        }

        @Override
        @Nonnull
        public String getModel() {
            return entityOrBldr.getTypeSpecificInfo().getPhysicalMachine().getModel();
        }

        @Override
        @Nonnull
        public String getTimezone() {
            return entityOrBldr.getTypeSpecificInfo().getPhysicalMachine().getTimezone();
        }
    }

    /**
     * Storage properties.
     */
    public static class ThickStorageProps extends ThickSearchableProps implements StorageProps {
        private ThickStorageProps(TopologyEntityDTOOrBuilder entityOrBldr) {
            super(entityOrBldr);
        }

        @Override
        public boolean isLocal() {
            return entityOrBldr.getTypeSpecificInfo().getStorage().getIsLocal();
        }
    }

    /**
     * Virtual machine properties.
     */
    public static class ThickVmProps extends ThickSearchableProps implements VmProps {
        private ThickVmProps(TopologyEntityDTOOrBuilder entityOrBldr) {
            super(entityOrBldr);
        }

        @Override
        @Nonnull
        public Collection<String> getConnectedNetworkNames() {
            return entityOrBldr.getTypeSpecificInfo().getVirtualMachine().getConnectedNetworksList();
        }

        @Override
        @Nonnull
        public String getGuestOsName() {
            return entityOrBldr.getTypeSpecificInfo().getVirtualMachine().getGuestOsInfo().getGuestOsName();
        }

        @Override
        public int getNumCpus() {
            return entityOrBldr.getTypeSpecificInfo().getVirtualMachine().getNumCpus();
        }

        @Override
        public int getCoresPerSocket() {
            return entityOrBldr.getTypeSpecificInfo().getVirtualMachine().getCoresPerSocketRatio();
        }
    }

    /**
     * Volume properties.
     */
    public static class ThickVolumeProps extends ThickSearchableProps implements VolumeProps {
        private ThickVolumeProps(TopologyEntityDTOOrBuilder entityOrBldr) {
            super(entityOrBldr);
        }

        @Override
        @Nonnull
        public AttachmentState attachmentState() {
            return entityOrBldr.getTypeSpecificInfo().getVirtualVolume().getAttachmentState();
        }

        @Override
        public boolean isEncrypted() {
            return entityOrBldr.getTypeSpecificInfo().getVirtualVolume().getEncryption();
        }

        @Override
        public boolean isEphemeral() {
            return entityOrBldr.getTypeSpecificInfo().getVirtualVolume().getIsEphemeral();
        }

        @Override
        public boolean isDeletable() {
            return entityOrBldr.getAnalysisSettings().getDeletable();
        }
    }

    /**
     * Service properties.
     */
    public static class ThickServiceProps extends ThickSearchableProps implements ServiceProps {
        private ThickServiceProps(TopologyEntityDTOOrBuilder entityDTOOrBuilder) {
            super(entityDTOOrBuilder);
        }

        @Override
        @Nullable
        public String getKubernetesServiceType() {
            return Optional.of(entityOrBldr.getTypeSpecificInfo())
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
    public static class ThickWorkloadControllerProps extends ThickSearchableProps implements WorkloadControllerProps {
        private ThickWorkloadControllerProps(TopologyEntityDTOOrBuilder entityOrBldr) {
            super(entityOrBldr);
        }

        @Override
        @Nonnull
        public String getControllerType() {
            WorkloadControllerInfo wcInfo = entityOrBldr.getTypeSpecificInfo().getWorkloadController();
            if (wcInfo.hasCustomControllerInfo()) {
                return wcInfo.getCustomControllerInfo().getCustomControllerType();
            } else {
                return wcInfo.getControllerTypeCase().name();
            }
        }

        @Override
        public boolean isCustom() {
            return entityOrBldr.getTypeSpecificInfo().getWorkloadController().hasCustomControllerInfo();
        }
    }

    /**
     * Business account properties.
     */
    public static class ThickBusinessAccountProps extends ThickSearchableProps implements BusinessAccountProps {

        private ThickBusinessAccountProps(TopologyEntityDTOOrBuilder entityOrBldr) {
            super(entityOrBldr);
        }

        @Override
        public boolean hasAssociatedTargetId() {
            return entityOrBldr.getTypeSpecificInfo().getBusinessAccount().hasAssociatedTargetId();
        }

        @Override
        @Nonnull
        public String getAccountId() {
            return entityOrBldr.getTypeSpecificInfo().getBusinessAccount().getAccountId();
        }
    }

    /**
     * Database server properties.
     */
    public static class ThickDatabaseServerProps extends ThickSearchableProps implements DatabaseServerProps {

        private ThickDatabaseServerProps(TopologyEntityDTOOrBuilder entityOrBldr) {
            super(entityOrBldr);
        }

        private boolean hasDatabaseEngine() {
            return entityOrBldr.getTypeSpecificInfo().getDatabase().hasEngine();
        }

        private boolean hasDatabaseEdition() {
            return entityOrBldr.getTypeSpecificInfo().getDatabase().hasEdition();
        }

        private boolean hasDatabaseRawEdition() {
            return entityOrBldr.getTypeSpecificInfo().getDatabase().hasRawEdition();
        }

        private boolean hasDatabaseVersion() {
            return entityOrBldr.getTypeSpecificInfo().getDatabase().hasVersion();
        }

        @Override
        @Nonnull
        public DatabaseEngine getDatabaseEngine() {
            if (hasDatabaseEngine()) {
                return entityOrBldr.getTypeSpecificInfo().getDatabase().getEngine();
            }
            return DatabaseEngine.UNKNOWN;
        }

        @Override
        @Nonnull
        public String getDatabaseEdition() {
            if (hasDatabaseEdition()) {
                return entityOrBldr.getTypeSpecificInfo().getDatabase().getEdition().name();
            } else if (hasDatabaseRawEdition()) {
                return entityOrBldr.getTypeSpecificInfo().getDatabase().getRawEdition();
            }
            return DatabaseEdition.NONE.name();
        }

        @Override
        @Nonnull
        public String getDatabaseVersion() {
            if (hasDatabaseVersion()) {
                return entityOrBldr.getTypeSpecificInfo().getDatabase().getVersion();
            }
            return UNKNOWN;
        }

        @Override
        public String getStorageEncryption() {
            return entityOrBldr.getEntityPropertyMapOrDefault(StringConstants.STORAGE_ENCRYPTION,
                    null);
        }

        @Override
        public String getStorageAutoscaling() {
            return entityOrBldr.getEntityPropertyMapOrDefault(StringConstants.STORAGE_AUTOSCALING,
                    null);
        }

        @Override
        public String getPerformanceInsights() {
            return entityOrBldr.getEntityPropertyMapOrDefault(
                    StringConstants.AWS_PERFORMANCE_INSIGHTS, null);
        }

        @Override
        public String getClusterRole() {
            return entityOrBldr.getEntityPropertyMapOrDefault(StringConstants.CLUSTER_ROLE, null);
        }

        @Override
        public String getStorageTier() {
            return entityOrBldr.getEntityPropertyMapOrDefault(StringConstants.DBS_STORAGE_TIER, null);
        }
    }

}
