package com.vmturbo.search.metadata;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedActionFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedGroupFieldApiDTO;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.GroupType;

/**
 * Enumeration for search db column mappings, which is used by both ingestion and query.
 */
public enum SearchEntityMetadata {

    /**
     * Mappings for different entity types.
     */
    APPLICATION(EntityType.APPLICATION, getApplicationMetadata()),
    BUSINESS_ACCOUNT(EntityType.BUSINESS_ACCOUNT, getBusinessAccountMetadata()),
    CONTAINER_POD(EntityType.CONTAINER_POD, getContainerPodMetadata()),
    DATA_CENTER(EntityType.DATACENTER, getDataCenterMetadata()),
    DB_SERVER(EntityType.DATABASE_SERVER, getDBServerMetaData()),
    DISK_ARRAY(EntityType.DISKARRAY, getDiskArrayMetadata()),
    PHYSICAL_MACHINE(EntityType.PHYSICAL_MACHINE, getPhysicalMachineMetadata()),
    REGION(EntityType.REGION, getRegionMetadata()),
    STORAGE(EntityType.STORAGE, getStorageMetadata()),
    VIRTUAL_MACHINE(EntityType.VIRTUAL_MACHINE, getVirtualMachineMetadata()),
    VIRTUAL_VOLUME(EntityType.VIRTUAL_VOLUME, getVirtualVolumeMetadata());

    private final EntityType entityType;

    private final Map<FieldApiDTO, SearchMetadataMapping> metadataMappingMap;

    /**
     * Create a SearchEntityMetadata, containing column mappings for reading/writing to searchDB.
     *
     * @param entityType entityType which mappings belong to
     * @param metadataMappingMap mappings for the entityType
     */
    SearchEntityMetadata(@Nonnull EntityType entityType,
            @Nonnull Map<FieldApiDTO, SearchMetadataMapping> metadataMappingMap) {
        this.entityType = entityType;
        this.metadataMappingMap = metadataMappingMap;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public Map<FieldApiDTO, SearchMetadataMapping> getMetadataMappingMap() {
        return metadataMappingMap;
    }

    /**
     * Returns all relevant column mappings for Virtual Machine.
     *
     * @return Virtual Machine mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getVirtualMachineMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // type specific fields
                .put(PrimitiveFieldApiDTO.primitive("guestOsType"), SearchMetadataMapping.PRIMITIVE_GUEST_OS_TYPE)
                .put(PrimitiveFieldApiDTO.primitive("numCpus"), SearchMetadataMapping.PRIMITIVE_VM_NUM_CPUS)
                // commodities
                .put(CommodityFieldApiDTO.capacity(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VSTORAGE), SearchMetadataMapping.COMMODITY_VSTORAGE_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VSTORAGE), SearchMetadataMapping.COMMODITY_VSTORAGE_UTILIZATION)
                // percentile TODO: uncomment and add more if needed
                // .put(CommodityFieldApiDTO.percentile(CommodityType.VCPU), SearchEntityMetadataMapping.COMMODITY_VCPU_PERCENTILE_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.APPLICATION), SearchMetadataMapping.RELATED_APPLICATION)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_ACCOUNT), SearchMetadataMapping.RELATED_ACCOUNT)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.DATACENTER), SearchMetadataMapping.RELATED_DATA_CENTER)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.DISKARRAY), SearchMetadataMapping.RELATED_DISKARRAY)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.PHYSICAL_MACHINE), SearchMetadataMapping.RELATED_HOST)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.REGION), SearchMetadataMapping.RELATED_REGION)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.STORAGE), SearchMetadataMapping.RELATED_STORAGE)
                // related groups
                .put(RelatedGroupFieldApiDTO.groupNames(GroupType.COMPUTE_HOST_CLUSTER), SearchMetadataMapping.RELATED_COMPUTE_HOST_CLUSTER_NAME)
                .build();
    }

    /**
     * Returns all relevant column mappings for Physical Machine.
     *
     * @return Physical Machine mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getPhysicalMachineMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // type specific fields
                .put(PrimitiveFieldApiDTO.primitive("connectedNetworks"), SearchMetadataMapping.PRIMITIVE_CONNECTED_NETWORKS)
                .put(PrimitiveFieldApiDTO.primitive("cpuModel"), SearchMetadataMapping.PRIMITIVE_CPU_MODEL)
                .put(PrimitiveFieldApiDTO.primitive("model"), SearchMetadataMapping.PRIMITIVE_MODEL)
                .put(PrimitiveFieldApiDTO.primitive("timezone"), SearchMetadataMapping.PRIMITIVE_TIMEZONE)
                // commodities
                .put(CommodityFieldApiDTO.percentile(CommodityType.BALLOONING), SearchMetadataMapping.COMMODITY_BALLOONING_PERCENTILE)
                .put(CommodityFieldApiDTO.utilization(CommodityType.BALLOONING), SearchMetadataMapping.COMMODITY_BALLOONING_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.CPU), SearchMetadataMapping.COMMODITY_CPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.CPU), SearchMetadataMapping.COMMODITY_CPU_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.IO_THROUGHPUT), SearchMetadataMapping.COMMODITY_IO_THROUGHPUT_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.IO_THROUGHPUT), SearchMetadataMapping.COMMODITY_IO_THROUGHPUT_UTILIZATION)
                .put(CommodityFieldApiDTO.capacity(CommodityType.MEM), SearchMetadataMapping.COMMODITY_MEM_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.MEM), SearchMetadataMapping.COMMODITY_MEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.MEM), SearchMetadataMapping.COMMODITY_MEM_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.NET_THROUGHPUT), SearchMetadataMapping.COMMODITY_NET_THROUGHPUT_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.NET_THROUGHPUT), SearchMetadataMapping.COMMODITY_NET_THROUGHPUT_UTILIZATION)
                .put(CommodityFieldApiDTO.percentile(CommodityType.SWAPPING), SearchMetadataMapping.COMMODITY_SWAPPING_PERCENTILE)
                .put(CommodityFieldApiDTO.utilization(CommodityType.SWAPPING), SearchMetadataMapping.COMMODITY_SWAPPING_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.DATACENTER), SearchMetadataMapping.RELATED_DATA_CENTER)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.SWITCH), SearchMetadataMapping.RELATED_SWITCH)
                .put(RelatedEntityFieldApiDTO.entityCount(EntityType.VIRTUAL_MACHINE), SearchMetadataMapping.NUM_VMS)
                // related groups
                .put(RelatedGroupFieldApiDTO.groupNames(GroupType.COMPUTE_HOST_CLUSTER), SearchMetadataMapping.RELATED_COMPUTE_HOST_CLUSTER_NAME)
                .build();
    }

    /**
     * Returns all relevant column mappings for Application.
     *
     * @return Application mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getApplicationMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // todo: type specific fields
                // todo: commodities
                // todo: related entities
                .build();
    }

    /**
     * Returns all relevant column mappings for Virtual Volume.
     *
     * @return Virtual Volume column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getVirtualVolumeMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // type specific fields
                .put(PrimitiveFieldApiDTO.primitive("attachmentState"), SearchMetadataMapping.PRIMITIVE_ATTACHMENT_STATE)
                // commodities
                .put(CommodityFieldApiDTO.capacity(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_CAPACITY)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_ACCOUNT), SearchMetadataMapping.RELATED_ACCOUNT)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.REGION), SearchMetadataMapping.RELATED_REGION)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.STORAGE), SearchMetadataMapping.RELATED_STORAGE)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.STORAGE_TIER), SearchMetadataMapping.RELATED_STORAGE_TIER)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.VIRTUAL_MACHINE), SearchMetadataMapping.RELATED_VM)
                .build();
    }

    /**
     * Returns all relevant column mappings for Storage.
     *
     * @return Storage column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getStorageMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // todo: type specific fields
                // todo: commodities
                // todo: related entities
                // related groups
                .put(RelatedGroupFieldApiDTO.groupNames(GroupType.COMPUTE_HOST_CLUSTER), SearchMetadataMapping.RELATED_COMPUTE_HOST_CLUSTER_NAME)
                .put(RelatedGroupFieldApiDTO.groupNames(GroupType.STORAGE_CLUSTER), SearchMetadataMapping.RELATED_STORAGE_CLUSTER_NAME)
                .build();
    }

    /**
     * Returns all relevant column mappings for Disk Array.
     *
     * @return Disk Array column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getDiskArrayMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // todo: type specific fields
                // todo: commodities
                // todo: related entities
                .build();
    }

    /**
     * Returns all relevant column mappings for Data Center.
     *
     * @return Data Center column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getDataCenterMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // todo: type specific fields
                // todo: commodities
                // todo: related entities
                .build();
    }

    /**
     * Returns all relevant column mappings for Container Pod.
     *
     * @return Container Pod column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getContainerPodMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // todo: type specific fields
                // todo: commodities
                // todo: related entities
                .build();
    }

    /**
     * Returns all relevant column mappings for Account.
     *
     * @return Account column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getBusinessAccountMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // todo: type specific fields
                // todo: commodities
                // todo: related entities
                .build();
    }

    /**
     * Returns all relevant column mappings for Region.
     *
     * @return Region column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getRegionMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // todo: type specific fields
                // todo: commodities
                // todo: related entities
                .build();
    }

    /**
     * Returns all relevant column mappings for DB Server.
     *
     * @return DB Server mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getDBServerMetaData() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VSTORAGE), SearchMetadataMapping.COMMODITY_VSTORAGE_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VSTORAGE), SearchMetadataMapping.COMMODITY_VSTORAGE_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_ACCOUNT), SearchMetadataMapping.RELATED_ACCOUNT)
                .build();
    }

    /**
     * Put static fields inside a nested class rather than inside the enum class, since enum
     * constructor is called BEFORE the static fields have all been initialized.
     */
    private static class Constants {
        /**
         * Common fields available to all entities.
         */
        static final Map<FieldApiDTO, SearchMetadataMapping> ENTITY_COMMON_FIELDS =
                ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                        // PRIMITIVES
                        .put(PrimitiveFieldApiDTO.oid(), SearchMetadataMapping.PRIMITIVE_OID)
                        .put(PrimitiveFieldApiDTO.entityType(), SearchMetadataMapping.PRIMITIVE_ENTITY_TYPE)
                        .put(PrimitiveFieldApiDTO.name(), SearchMetadataMapping.PRIMITIVE_NAME)
                        .put(PrimitiveFieldApiDTO.entitySeverity(), SearchMetadataMapping.PRIMITIVE_SEVERITY)
                        .put(PrimitiveFieldApiDTO.entityState(), SearchMetadataMapping.PRIMITIVE_STATE)
                        .put(PrimitiveFieldApiDTO.environmentType(), SearchMetadataMapping.PRIMITIVE_ENVIRONMENT_TYPE)
                        // RELATED ACTION
                        .put(RelatedActionFieldApiDTO.actionCount(), SearchMetadataMapping.RELATED_ACTION_COUNT)
                        .build();
    }
}
