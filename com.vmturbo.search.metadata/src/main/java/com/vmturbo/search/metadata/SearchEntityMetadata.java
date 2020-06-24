package com.vmturbo.search.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedActionFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntityType;

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

    private final Map<FieldApiDTO, SearchEntityMetadataMapping> metadataMappingMap;

    /**
     * Create a SearchEntityMetadata, containing column mappings for reading/writing to searchDB.
     *
     * @param entityType entityType which mappings belong to
     * @param metadataMappingMap mappings for the entityType
     */
    SearchEntityMetadata(@Nonnull EntityType entityType,
            @Nonnull Map<FieldApiDTO, SearchEntityMetadataMapping> metadataMappingMap) {
        this.entityType = entityType;
        this.metadataMappingMap = metadataMappingMap;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public Map<FieldApiDTO, SearchEntityMetadataMapping> getMetadataMappingMap() {
        return metadataMappingMap;
    }

    /**
     * List of search entity metadata by entity type and field type. We use integer entity type
     * as key to match the int entity type in
     * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO}, so we don't
     * need to convert the int entity type back to api {@link EntityType} for every
     * TopologyEntityDTO during ingestion.
     */
    private static final Table<Integer, FieldType, List<SearchEntityMetadataMapping>>
            METADATA_BY_ENTITY_TYPE_AND_FIELD_TYPE;
    static {
        final Table<Integer, FieldType, List<SearchEntityMetadataMapping>> table = HashBasedTable.create();
        for (SearchEntityMetadata searchEntityMetadata : SearchEntityMetadata.values()) {
            int entityType = EntityTypeMapper.fromApiEntityTypeToProto(
                    searchEntityMetadata.getEntityType()).getNumber();
            searchEntityMetadata.getMetadataMappingMap().forEach((FieldApiDTO, metadata) -> {
                List<SearchEntityMetadataMapping> metadataList =
                        table.get(entityType, FieldApiDTO.getFieldType());
                if (metadataList == null) {
                    metadataList = new ArrayList<>();
                    table.put(entityType, FieldApiDTO.getFieldType(), metadataList);
                }
                metadataList.add(metadata);
            });
        }
        METADATA_BY_ENTITY_TYPE_AND_FIELD_TYPE = Tables.unmodifiableTable(table);
    }

    /**
     * Get list of defined metadata for the given entity type and field type.
     *
     * @param entityType type of the entity
     * @param fieldType type of the field as defined in {@link FieldType}
     * @return list of {@link SearchEntityMetadataMapping}
     */
    @Nonnull
    public static List<SearchEntityMetadataMapping> getMetadata(int entityType, FieldType fieldType) {
        List<SearchEntityMetadataMapping> metadataMappingList =
                METADATA_BY_ENTITY_TYPE_AND_FIELD_TYPE.get(entityType, fieldType);
        return metadataMappingList != null ? metadataMappingList : Collections.emptyList();
    }

    /**
     * Check if there is metadata defined for the given entity type.
     *
     * @param entityType type of the entity
     * @return true if metadata is defined for the entity, otherwise false
     */
    public static boolean hasMetadata(int entityType) {
        Map<FieldType, List<SearchEntityMetadataMapping>> metadata =
                METADATA_BY_ENTITY_TYPE_AND_FIELD_TYPE.row(entityType);
        return metadata != null && !metadata.isEmpty();
    }

    /**
     * Returns all relevant column mappings for Virtual Machine.
     *
     * @return Virtual Machine mappings
     */
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getVirtualMachineMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // type specific fields
                .put(PrimitiveFieldApiDTO.primitive("guestOsType"), SearchEntityMetadataMapping.PRIMITIVE_GUEST_OS_TYPE)
                .put(PrimitiveFieldApiDTO.primitive("numCpus"), SearchEntityMetadataMapping.PRIMITIVE_VM_NUM_CPUS)
                // commodities
                .put(CommodityFieldApiDTO.capacity(CommodityType.VMEM), SearchEntityMetadataMapping.COMMODITY_VMEM_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.VCPU), SearchEntityMetadataMapping.COMMODITY_VCPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VCPU), SearchEntityMetadataMapping.COMMODITY_VCPU_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VMEM), SearchEntityMetadataMapping.COMMODITY_VMEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VMEM), SearchEntityMetadataMapping.COMMODITY_VMEM_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VSTORAGE), SearchEntityMetadataMapping.COMMODITY_VSTORAGE_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VSTORAGE), SearchEntityMetadataMapping.COMMODITY_VSTORAGE_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.APPLICATION), SearchEntityMetadataMapping.RELATED_APPLICATION)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_ACCOUNT), SearchEntityMetadataMapping.RELATED_ACCOUNT)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.DATACENTER), SearchEntityMetadataMapping.RELATED_DATA_CENTER)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.DISKARRAY), SearchEntityMetadataMapping.RELATED_DISKARRAY)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.PHYSICAL_MACHINE), SearchEntityMetadataMapping.RELATED_HOST)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.REGION), SearchEntityMetadataMapping.RELATED_REGION)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.STORAGE), SearchEntityMetadataMapping.RELATED_STORAGE)
                .build();
    }

    /**
     * Returns all relevant column mappings for Physical Machine.
     *
     * @return Physical Machine mappings
     */
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getPhysicalMachineMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // type specific fields
                .put(PrimitiveFieldApiDTO.primitive("connectedNetworks"), SearchEntityMetadataMapping.PRIMITIVE_CONNECTED_NETWORKS)
                .put(PrimitiveFieldApiDTO.primitive("cpuModel"), SearchEntityMetadataMapping.PRIMITIVE_CPU_MODEL)
                .put(PrimitiveFieldApiDTO.primitive("model"), SearchEntityMetadataMapping.PRIMITIVE_MODEL)
                .put(PrimitiveFieldApiDTO.primitive("timezone"), SearchEntityMetadataMapping.PRIMITIVE_TIMEZONE)
                // commodities
                .put(CommodityFieldApiDTO.percentile(CommodityType.BALLOONING), SearchEntityMetadataMapping.COMMODITY_BALLOONING_PERCENTILE)
                .put(CommodityFieldApiDTO.utilization(CommodityType.BALLOONING), SearchEntityMetadataMapping.COMMODITY_BALLOONING_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.CPU), SearchEntityMetadataMapping.COMMODITY_CPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.CPU), SearchEntityMetadataMapping.COMMODITY_CPU_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.IO_THROUGHPUT), SearchEntityMetadataMapping.COMMODITY_IO_THROUGHPUT_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.IO_THROUGHPUT), SearchEntityMetadataMapping.COMMODITY_IO_THROUGHPUT_UTILIZATION)
                .put(CommodityFieldApiDTO.capacity(CommodityType.MEM), SearchEntityMetadataMapping.COMMODITY_MEM_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.MEM), SearchEntityMetadataMapping.COMMODITY_MEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.MEM), SearchEntityMetadataMapping.COMMODITY_MEM_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.NET_THROUGHPUT), SearchEntityMetadataMapping.COMMODITY_NET_THROUGHPUT_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.NET_THROUGHPUT), SearchEntityMetadataMapping.COMMODITY_NET_THROUGHPUT_UTILIZATION)
                .put(CommodityFieldApiDTO.percentile(CommodityType.SWAPPING), SearchEntityMetadataMapping.COMMODITY_SWAPPING_PERCENTILE)
                .put(CommodityFieldApiDTO.utilization(CommodityType.SWAPPING), SearchEntityMetadataMapping.COMMODITY_SWAPPING_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.DATACENTER), SearchEntityMetadataMapping.RELATED_DATA_CENTER)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.SWITCH), SearchEntityMetadataMapping.RELATED_SWITCH)
                .put(RelatedEntityFieldApiDTO.entityCount(EntityType.VIRTUAL_MACHINE), SearchEntityMetadataMapping.NUM_VMS)
                .build();
    }

    /**
     * Returns all relevant column mappings for Application.
     *
     * @return Application mappings
     */
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getApplicationMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
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
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getVirtualVolumeMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // type specific fields
                .put(PrimitiveFieldApiDTO.primitive("attachmentState"), SearchEntityMetadataMapping.PRIMITIVE_ATTACHMENT_STATE)
                // commodities
                .put(CommodityFieldApiDTO.capacity(CommodityType.STORAGE_AMOUNT), SearchEntityMetadataMapping.COMMODITY_STORAGE_AMOUNT_CAPACITY)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_ACCOUNT), SearchEntityMetadataMapping.RELATED_ACCOUNT)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.REGION), SearchEntityMetadataMapping.RELATED_REGION)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.STORAGE), SearchEntityMetadataMapping.RELATED_STORAGE)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.STORAGE_TIER), SearchEntityMetadataMapping.RELATED_STORAGE_TIER)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.VIRTUAL_MACHINE), SearchEntityMetadataMapping.RELATED_VM)
                .build();
    }

    /**
     * Returns all relevant column mappings for Storage.
     *
     * @return Storage column mappings
     */
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getStorageMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // todo: type specific fields
                // todo: commodities
                // todo: related entities
                .build();
    }

    /**
     * Returns all relevant column mappings for Disk Array.
     *
     * @return Disk Array column mappings
     */
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getDiskArrayMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
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
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getDataCenterMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
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
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getContainerPodMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
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
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getBusinessAccountMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
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
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getRegionMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
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
    private static Map<FieldApiDTO, SearchEntityMetadataMapping> getDBServerMetaData() {
        return ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.VCPU), SearchEntityMetadataMapping.COMMODITY_VCPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VCPU), SearchEntityMetadataMapping.COMMODITY_VCPU_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VMEM), SearchEntityMetadataMapping.COMMODITY_VMEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VMEM), SearchEntityMetadataMapping.COMMODITY_VMEM_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VSTORAGE), SearchEntityMetadataMapping.COMMODITY_VSTORAGE_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VSTORAGE), SearchEntityMetadataMapping.COMMODITY_VSTORAGE_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_ACCOUNT), SearchEntityMetadataMapping.RELATED_ACCOUNT)
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
        static final Map<FieldApiDTO, SearchEntityMetadataMapping> ENTITY_COMMON_FIELDS =
                ImmutableMap.<FieldApiDTO, SearchEntityMetadataMapping>builder()
                        // PRIMITIVES
                        .put(PrimitiveFieldApiDTO.oid(), SearchEntityMetadataMapping.PRIMITIVE_OID)
                        .put(PrimitiveFieldApiDTO.entityType(), SearchEntityMetadataMapping.PRIMITIVE_ENTITY_TYPE)
                        .put(PrimitiveFieldApiDTO.name(), SearchEntityMetadataMapping.PRIMITIVE_NAME)
                        .put(PrimitiveFieldApiDTO.entitySeverity(), SearchEntityMetadataMapping.PRIMITIVE_SEVERITY)
                        .put(PrimitiveFieldApiDTO.entityState(), SearchEntityMetadataMapping.PRIMITIVE_STATE)
                        .put(PrimitiveFieldApiDTO.environmentType(), SearchEntityMetadataMapping.PRIMITIVE_ENVIRONMENT_TYPE)
                        // RELATED ACTION
                        .put(RelatedActionFieldApiDTO.actionCount(), SearchEntityMetadataMapping.RELATED_ACTION)
                        .build();
    }
}
