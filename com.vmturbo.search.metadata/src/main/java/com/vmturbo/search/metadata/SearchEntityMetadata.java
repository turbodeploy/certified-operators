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
    APPLICATION_COMPONENT(EntityType.APPLICATION_COMPONENT, getApplicationComponentMetadata()),
    BUSINESS_ACCOUNT(EntityType.BUSINESS_ACCOUNT, getBusinessAccountMetadata()),
    BUSINESS_APPLICATION(EntityType.BUSINESS_APPLICATION, getBusinessApplicationMetadata()),
    BUSINESS_TRANSACTION(EntityType.BUSINESS_TRANSACTION, getBusinessTransactionMetadata()),
    BUSINESS_USER(EntityType.BUSINESS_USER, getBusinessUserMetadata()),
    CHASSIS(EntityType.CHASSIS, getChassisMetadata()),
    CONTAINER(EntityType.CONTAINER, getContainerMetadata()),
    CONTAINER_POD(EntityType.CONTAINER_POD, getContainerPodMetadata()),
    DATABASE(EntityType.DATABASE, getDBMetaData()),
    DATABASE_SERVER(EntityType.DATABASE_SERVER, getDBServerMetaData()),
    DATACENTER(EntityType.DATACENTER, getDataCenterMetadata()),
    DESKTOP_POOL(EntityType.DESKTOP_POOL, getDesktopPoolMetaData()),
    DISKARRAY(EntityType.DISKARRAY, getDiskArrayMetadata()),
    IOMODULE(EntityType.IOMODULE, getIOModuleMetadata()),
    NETWORK(EntityType.NETWORK, getNetworkMetadata()),
    PHYSICAL_MACHINE(EntityType.PHYSICAL_MACHINE, getPhysicalMachineMetadata()),
    REGION(EntityType.REGION, getRegionMetadata()),
    SERVICE(EntityType.SERVICE, getServiceMetadata()),
    STORAGE(EntityType.STORAGE, getStorageMetadata()),
    STORAGECONTROLLER(EntityType.STORAGECONTROLLER, getStorageControllerMetadata()),
    SWITCH(EntityType.SWITCH, getSwitchMetadata()),
    VIEW_POD(EntityType.VIEW_POD, getViewPodMetadata()),
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
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.APPLICATION), SearchMetadataMapping.RELATED_APPLICATION)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_ACCOUNT), SearchMetadataMapping.RELATED_ACCOUNT)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.DATACENTER), SearchMetadataMapping.RELATED_DATA_CENTER)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.DISKARRAY), SearchMetadataMapping.RELATED_DISKARRAY)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.PHYSICAL_MACHINE), SearchMetadataMapping.RELATED_HOST)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.REGION), SearchMetadataMapping.RELATED_REGION)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.STORAGE), SearchMetadataMapping.RELATED_STORAGE)
                .putAll(Constants.BASIC_APPLICATION_FIELDS)
                // related groups
                .put(RelatedGroupFieldApiDTO.groupNames(GroupType.RESOURCE), SearchMetadataMapping.RELATED_RESOURCE_GROUP_NAME_FOR_VM)
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
    private static Map<FieldApiDTO, SearchMetadataMapping> getApplicationComponentMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.RESPONSE_TIME), SearchMetadataMapping.COMMODITY_RESPONSE_TIME_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.RESPONSE_TIME), SearchMetadataMapping.COMMODITY_RESPONSE_TIME_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.TRANSACTION), SearchMetadataMapping.COMMODITY_TRANSACTION_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.TRANSACTION), SearchMetadataMapping.COMMODITY_TRANSACTION_UTILIZATION)
                // related entities
                .putAll(Constants.BASIC_APPLICATION_FIELDS)
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
                // related groups
                .put(RelatedGroupFieldApiDTO.groupNames(GroupType.RESOURCE), SearchMetadataMapping.RELATED_RESOURCE_GROUP_NAME_FOR_VV)
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
                // type specific fields
                .put(PrimitiveFieldApiDTO.primitive("isLocal"), SearchMetadataMapping.PRIMITIVE_IS_LOCAL)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_ACCESS), SearchMetadataMapping.COMMODITY_STORAGE_ACCESS_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_ACCESS), SearchMetadataMapping.COMMODITY_STORAGE_ACCESS_UTILIZATION)
                .put(CommodityFieldApiDTO.capacity(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_LATENCY), SearchMetadataMapping.COMMODITY_STORAGE_LATENCY_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_LATENCY), SearchMetadataMapping.COMMODITY_STORAGE_LATENCY_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_PROVISIONED), SearchMetadataMapping.COMMODITY_STORAGE_PROVISIONED_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_PROVISIONED), SearchMetadataMapping.COMMODITY_STORAGE_PROVISIONED_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.DATACENTER), SearchMetadataMapping.RELATED_DATA_CENTER)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.VIRTUAL_MACHINE), SearchMetadataMapping.RELATED_VM)
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
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_ACCESS), SearchMetadataMapping.COMMODITY_STORAGE_ACCESS_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_ACCESS), SearchMetadataMapping.COMMODITY_STORAGE_ACCESS_UTILIZATION)
                .put(CommodityFieldApiDTO.capacity(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_LATENCY), SearchMetadataMapping.COMMODITY_STORAGE_LATENCY_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_LATENCY), SearchMetadataMapping.COMMODITY_STORAGE_LATENCY_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_PROVISIONED), SearchMetadataMapping.COMMODITY_STORAGE_PROVISIONED_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_PROVISIONED), SearchMetadataMapping.COMMODITY_STORAGE_PROVISIONED_UTILIZATION)
                .build();
    }

    /**
     * Returns all relevant column mappings for Storage Controller.
     *
     * @return Storage Controller column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getStorageControllerMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_ACCESS), SearchMetadataMapping.COMMODITY_STORAGE_ACCESS_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_ACCESS), SearchMetadataMapping.COMMODITY_STORAGE_ACCESS_UTILIZATION)
                .put(CommodityFieldApiDTO.capacity(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_AMOUNT), SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.STORAGE_LATENCY), SearchMetadataMapping.COMMODITY_STORAGE_LATENCY_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.STORAGE_LATENCY), SearchMetadataMapping.COMMODITY_STORAGE_LATENCY_UTILIZATION)
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
                .build();
    }

    /**
     * Returns all relevant column mappings for Switch.
     *
     * @return Switch column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getSwitchMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.NET_THROUGHPUT), SearchMetadataMapping.COMMODITY_NET_THROUGHPUT_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.NET_THROUGHPUT), SearchMetadataMapping.COMMODITY_NET_THROUGHPUT_UTILIZATION)
                .put(CommodityFieldApiDTO.utilization(CommodityType.PORT_CHANNEL), SearchMetadataMapping.COMMODITY_PORT_CHANNEL_UTILIZATION)
                .build();
    }

    /**
     * Returns all relevant column mappings for IO Module.
     *
     * @return IO Module column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getIOModuleMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.utilization(CommodityType.NET_THROUGHPUT), SearchMetadataMapping.COMMODITY_NET_THROUGHPUT_UTILIZATION)
                .build();
    }

    /**
     * Returns all relevant column mappings for Network.
     *
     * @return Network column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getNetworkMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.utilization(CommodityType.NET_THROUGHPUT), SearchMetadataMapping.COMMODITY_NET_THROUGHPUT_UTILIZATION)
                .build();
    }

    /**
     * Returns all relevant column mappings for Chassis.
     *
     * @return Chassis column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getChassisMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.utilization(CommodityType.COOLING), SearchMetadataMapping.COMMODITY_COOLING_UTILIZATION)
                .put(CommodityFieldApiDTO.utilization(CommodityType.POWER), SearchMetadataMapping.COMMODITY_POWER_UTILIZATION)
                .put(CommodityFieldApiDTO.utilization(CommodityType.SPACE), SearchMetadataMapping.COMMODITY_SPACE_UTILIZATION)
                .build();
    }

    /**
     * Returns all relevant column mappings for Business User.
     *
     * @return Data Center column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getBusinessUserMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.IMAGE_CPU), SearchMetadataMapping.COMMODITY_IMAGE_CPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.IMAGE_CPU), SearchMetadataMapping.COMMODITY_IMAGE_CPU_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.IMAGE_MEM), SearchMetadataMapping.COMMODITY_IMAGE_MEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.IMAGE_MEM), SearchMetadataMapping.COMMODITY_IMAGE_MEM_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.IMAGE_STORAGE), SearchMetadataMapping.COMMODITY_IMAGE_STORAGE_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.IMAGE_STORAGE), SearchMetadataMapping.COMMODITY_IMAGE_STORAGE_UTILIZATION)
                .build();
    }

    /**
     * Returns all relevant column mappings for View Pod.
     *
     * @return View Pod column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getViewPodMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.ACTIVE_SESSIONS), SearchMetadataMapping.COMMODITY_ACTIVE_SESSIONS_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.ACTIVE_SESSIONS), SearchMetadataMapping.COMMODITY_ACTIVE_SESSIONS_UTILIZATION)
                .build();
    }

    /**
     * Returns all relevant column mappings for Desktop Pool.
     *
     * @return Desktop Pool column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getDesktopPoolMetaData() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.utilization(CommodityType.ACTIVE_SESSIONS), SearchMetadataMapping.COMMODITY_ACTIVE_SESSIONS_UTILIZATION)
                .put(CommodityFieldApiDTO.utilization(CommodityType.POOL_CPU), SearchMetadataMapping.COMMODITY_POOL_CPU_UTILIZATION)
                .put(CommodityFieldApiDTO.utilization(CommodityType.POOL_MEM), SearchMetadataMapping.COMMODITY_POOL_MEM_UTILIZATION)
                .put(CommodityFieldApiDTO.utilization(CommodityType.POOL_STORAGE), SearchMetadataMapping.COMMODITY_POOL_STORAGE_UTILIZATION)
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
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_UTILIZATION)
                .put(CommodityFieldApiDTO.capacity(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.VIRTUAL_MACHINE), SearchMetadataMapping.RELATED_VM)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.NAMESPACE), SearchMetadataMapping.RELATED_NAMESPACE)
                .putAll(Constants.BASIC_APPLICATION_FIELDS)
                .build();
    }

    /**
     * Returns all relevant column mappings for Container.
     *
     * @return Container column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getContainerMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_UTILIZATION)
                .put(CommodityFieldApiDTO.capacity(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.CONTAINER_POD), SearchMetadataMapping.RELATED_CONTAINER_POD)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.NAMESPACE), SearchMetadataMapping.RELATED_NAMESPACE)
                .putAll(Constants.BASIC_APPLICATION_FIELDS)
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
                // related groups
                .put(RelatedGroupFieldApiDTO.groupNames(GroupType.BILLING_FAMILY), SearchMetadataMapping.RELATED_BILLING_FAMILY_NAME)
                .build();
    }

    /**
     * Returns all relevant column mappings for Business Application.
     *
     * @return Business Application column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getBusinessApplicationMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.RESPONSE_TIME), SearchMetadataMapping.COMMODITY_RESPONSE_TIME_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.RESPONSE_TIME), SearchMetadataMapping.COMMODITY_RESPONSE_TIME_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.TRANSACTION), SearchMetadataMapping.COMMODITY_TRANSACTION_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.TRANSACTION), SearchMetadataMapping.COMMODITY_TRANSACTION_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_TRANSACTION), SearchMetadataMapping.RELATED_BUSINESS_TRANSACTION)
                .build();
    }


    /**
     * Returns all relevant column mappings for Business Transaction.
     *
     * @return Business Application column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getBusinessTransactionMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.RESPONSE_TIME), SearchMetadataMapping.COMMODITY_RESPONSE_TIME_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.RESPONSE_TIME), SearchMetadataMapping.COMMODITY_RESPONSE_TIME_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.TRANSACTION), SearchMetadataMapping.COMMODITY_TRANSACTION_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.TRANSACTION), SearchMetadataMapping.COMMODITY_TRANSACTION_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_APPLICATION), SearchMetadataMapping.RELATED_BUSINESS_APPLICATION)
                .build();
    }


    /**
     * Returns all relevant column mappings for Service.
     *
     * @return Service column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getServiceMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.RESPONSE_TIME), SearchMetadataMapping.COMMODITY_RESPONSE_TIME_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.RESPONSE_TIME), SearchMetadataMapping.COMMODITY_RESPONSE_TIME_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_APPLICATION), SearchMetadataMapping.RELATED_BUSINESS_APPLICATION)
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_TRANSACTION), SearchMetadataMapping.RELATED_BUSINESS_TRANSACTION)
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
                .put(CommodityFieldApiDTO.capacity(CommodityType.DB_MEM), SearchMetadataMapping.COMMODITY_DB_MEM_CAPACITY)
                .put(CommodityFieldApiDTO.used(CommodityType.DB_MEM), SearchMetadataMapping.COMMODITY_DB_MEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.DB_MEM), SearchMetadataMapping.COMMODITY_DB_MEM_UTILIZATION).put(CommodityFieldApiDTO.used(CommodityType.DB_CACHE_HIT_RATE), SearchMetadataMapping.COMMODITY_DB_HIT_RATE_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.DB_CACHE_HIT_RATE), SearchMetadataMapping.COMMODITY_DB_HIT_RATE_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VCPU), SearchMetadataMapping.COMMODITY_VCPU_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VMEM), SearchMetadataMapping.COMMODITY_VMEM_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VSTORAGE), SearchMetadataMapping.COMMODITY_VSTORAGE_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VSTORAGE), SearchMetadataMapping.COMMODITY_VSTORAGE_UTILIZATION)
                 // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_ACCOUNT), SearchMetadataMapping.RELATED_ACCOUNT)
                .putAll(Constants.BASIC_APPLICATION_FIELDS)
                .build();
    }

    /**
     * Returns all relevant column mappings for DB Server.
     *
     * @return DB Server mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getDBMetaData() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                // commodities
                .put(CommodityFieldApiDTO.used(CommodityType.TRANSACTION), SearchMetadataMapping.COMMODITY_TRANSACTION_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.TRANSACTION), SearchMetadataMapping.COMMODITY_TRANSACTION_UTILIZATION)
                .put(CommodityFieldApiDTO.used(CommodityType.VSTORAGE), SearchMetadataMapping.COMMODITY_VSTORAGE_USED)
                .put(CommodityFieldApiDTO.utilization(CommodityType.VSTORAGE), SearchMetadataMapping.COMMODITY_VSTORAGE_UTILIZATION)
                // related entities
                .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_ACCOUNT), SearchMetadataMapping.RELATED_ACCOUNT)
                .putAll(Constants.BASIC_APPLICATION_FIELDS)
                // related groups
                .put(RelatedGroupFieldApiDTO.groupNames(GroupType.RESOURCE), SearchMetadataMapping.RELATED_RESOURCE_GROUP_NAME_FOR_DB)
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
                        .put(PrimitiveFieldApiDTO.severity(), SearchMetadataMapping.PRIMITIVE_SEVERITY)
                        .put(PrimitiveFieldApiDTO.entityState(), SearchMetadataMapping.PRIMITIVE_STATE)
                        .put(PrimitiveFieldApiDTO.environmentType(), SearchMetadataMapping.PRIMITIVE_ENVIRONMENT_TYPE)
                        // RELATED ACTION
                        .put(RelatedActionFieldApiDTO.actionCount(), SearchMetadataMapping.RELATED_ACTION_COUNT)
                        .build();

        /**
         * Fields available to entities that can be associated with business applications, transactions, and services.
         */
        static final Map<FieldApiDTO, SearchMetadataMapping> BASIC_APPLICATION_FIELDS =
                ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                        // RELATED ENTITIES
                        .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_APPLICATION), SearchMetadataMapping.RELATED_BUSINESS_APPLICATION)
                        .put(RelatedEntityFieldApiDTO.entityNames(EntityType.BUSINESS_TRANSACTION), SearchMetadataMapping.RELATED_BUSINESS_TRANSACTION)
                        .put(RelatedEntityFieldApiDTO.entityNames(EntityType.SERVICE), SearchMetadataMapping.RELATED_SERVICE)
                        .build();
    }
}
