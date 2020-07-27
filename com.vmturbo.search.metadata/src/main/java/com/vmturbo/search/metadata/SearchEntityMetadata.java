package com.vmturbo.search.metadata;

import static com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO.capacity;
import static com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO.percentileHistoricalUtilization;
import static com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO.used;
import static com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO.weightedHistoricalUtilization;
import static com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO.primitive;
import static com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO.entityCount;
import static com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO.entityNames;
import static com.vmturbo.api.dto.searchquery.RelatedGroupFieldApiDTO.groupNames;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_ACTIVE_SESSIONS_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_ACTIVE_SESSIONS_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_BALLOONING_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_CONNECTION_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_CONNECTION_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_COOLING_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_CPU_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_CPU_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_DB_HIT_RATE_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_DB_HIT_RATE_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_DB_MEM_CAPACITY;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_DB_MEM_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_DB_MEM_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_IMAGE_CPU_PERCENTILE_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_IMAGE_CPU_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_IMAGE_MEM_PERCENTILE_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_IMAGE_MEM_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_IMAGE_STORAGE_PERCENTILE_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_IMAGE_STORAGE_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_IO_THROUGHPUT_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_IO_THROUGHPUT_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_MEM_CAPACITY;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_MEM_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_MEM_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_NET_THROUGHPUT_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_NET_THROUGHPUT_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_POOL_CPU_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_POOL_MEM_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_POOL_STORAGE_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_PORT_CHANNEL_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_POWER_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_RESPONSE_TIME_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_SPACE_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_STORAGE_ACCESS_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_STORAGE_ACCESS_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_CAPACITY;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_STORAGE_AMOUNT_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_STORAGE_LATENCY_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_STORAGE_LATENCY_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_STORAGE_PROVISIONED_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_STORAGE_PROVISIONED_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_SWAPPING_HISTORICAL_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_TRANSACTION_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_VCPU_PERCENTILE_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_VCPU_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_VMEM_CAPACITY;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_VMEM_PERCENTILE_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_VMEM_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_VSTORAGE_PERCENTILE_UTILIZATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.COMMODITY_VSTORAGE_USED;
import static com.vmturbo.search.metadata.SearchMetadataMapping.NUM_VMS;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_ATTACHMENT_STATE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_CONNECTED_NETWORKS;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_CPU_MODEL;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_ENTITY_TYPE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_ENVIRONMENT_TYPE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_GUEST_OS_TYPE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_IS_LOCAL;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_MODEL;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_NAME;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_OID;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_PM_NUM_CPUS;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_SEVERITY;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_STATE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_TIMEZONE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.PRIMITIVE_VM_NUM_CPUS;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_ACCOUNT;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_ACTION_COUNT;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_APPLICATION_COMPONENT;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_BILLING_FAMILY_NAME;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_BUSINESS_APPLICATION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_BUSINESS_TRANSACTION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_COMPUTE_HOST_CLUSTER_NAME;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_CONTAINER_POD;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_DATA_CENTER;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_DISKARRAY;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_HOST;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_NAMESPACE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_REGION;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_RESOURCE_GROUP_NAME_FOR_DB;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_RESOURCE_GROUP_NAME_FOR_VM;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_RESOURCE_GROUP_NAME_FOR_VV;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_SERVICE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_STORAGE;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_STORAGE_CLUSTER_NAME;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_STORAGE_TIER;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_SWITCH;
import static com.vmturbo.search.metadata.SearchMetadataMapping.RELATED_VM;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.dto.searchquery.FieldApiDTO;
import com.vmturbo.api.dto.searchquery.PrimitiveFieldApiDTO;
import com.vmturbo.api.dto.searchquery.RelatedActionFieldApiDTO;
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
    ApplicationComponent(EntityType.ApplicationComponent, getApplicationComponentMetadata()),
    BusinessAccount(EntityType.BusinessAccount, getBusinessAccountMetadata()),
    BusinessApplication(EntityType.BusinessApplication, getBusinessApplicationMetadata()),
    BusinessTransaction(EntityType.BusinessTransaction, getBusinessTransactionMetadata()),
    BusinessUser(EntityType.BusinessUser, getBusinessUserMetadata()),
    Chassis(EntityType.Chassis, getChassisMetadata()),
    Container(EntityType.Container, getContainerMetadata()),
    ContainerPod(EntityType.ContainerPod, getContainerPodMetadata()),
    ContainerSpec(EntityType.ContainerSpec, getContainerPodMetadata()),
    Database(EntityType.Database, getDBMetaData()),
    DatabaseServer(EntityType.DatabaseServer, getDBServerMetaData()),
    DataCenter(EntityType.DataCenter, getDataCenterMetadata()),
    DesktopPool(EntityType.DesktopPool, getDesktopPoolMetaData()),
    DiskArray(EntityType.DiskArray, getDiskArrayMetadata()),
    IOModule(EntityType.IOModule, getIOModuleMetadata()),
    Namespace(EntityType.Namespace, getNamespaceMetadata()),
    Network(EntityType.Network, getNetworkMetadata()),
    PhysicalMachine(EntityType.PhysicalMachine, getPhysicalMachineMetadata()),
    Region(EntityType.Region, getRegionMetadata()),
    Service(EntityType.Service, getServiceMetadata()),
    Storage(EntityType.Storage, getStorageMetadata()),
    StorageController(EntityType.StorageController, getStorageControllerMetadata()),
    Switch(EntityType.Switch, getSwitchMetadata()),
    ViewPod(EntityType.ViewPod, getViewPodMetadata()),
    VirtualMachine(EntityType.VirtualMachine, getVirtualMachineMetadata()),
    VirtualVolume(EntityType.VirtualVolume, getVirtualVolumeMetadata()),
    VirtualDataCenter(EntityType.VirtualDataCenter, getVirtualDataCenterMetadata()),
    WorkloadController(EntityType.WorkloadController, getWorkloadControllerMetadata());

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

    /**
     * Gets an immutable map representing the mappings for fields specific to this entity type.
     *
     * @return an immutable map representing the mappings for fields specific to this entity type
     */
    public Map<FieldApiDTO, SearchMetadataMapping> getMetadataMappingMap() {
        return metadataMappingMap;
    }

    /**
     * Gets an immutable map representing the mappings for fields common to all entities.
     *
     * @return an immutable map representing the mappings for fields common to all entities
     */
    public static Map<FieldApiDTO, SearchMetadataMapping> getEntityCommonFieldsMappingMap() {
        return Constants.ENTITY_COMMON_FIELDS;
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
            .put(primitive("connectedNetworks"), PRIMITIVE_CONNECTED_NETWORKS)
            .put(primitive("guestOsType"), PRIMITIVE_GUEST_OS_TYPE)
            .put(primitive("numCpus"), PRIMITIVE_VM_NUM_CPUS)
            // commodities
            .put(capacity(CommodityType.VMEM), COMMODITY_VMEM_CAPACITY)
            .put(used(CommodityType.VCPU), COMMODITY_VCPU_USED)
            .put(percentileHistoricalUtilization(CommodityType.VCPU), COMMODITY_VCPU_PERCENTILE_UTILIZATION)
            .put(used(CommodityType.VMEM), COMMODITY_VMEM_USED)
            .put(percentileHistoricalUtilization(CommodityType.VMEM), COMMODITY_VMEM_PERCENTILE_UTILIZATION)
            .put(used(CommodityType.VSTORAGE), COMMODITY_VSTORAGE_USED)
            .put(percentileHistoricalUtilization(CommodityType.VSTORAGE), COMMODITY_VSTORAGE_PERCENTILE_UTILIZATION)
            // related entities
            .put(entityNames(EntityType.ApplicationComponent), RELATED_APPLICATION_COMPONENT)
            .put(entityNames(EntityType.BusinessAccount), RELATED_ACCOUNT)
            .put(entityNames(EntityType.DataCenter), RELATED_DATA_CENTER)
            .put(entityNames(EntityType.DiskArray), RELATED_DISKARRAY)
            .put(entityNames(EntityType.PhysicalMachine), RELATED_HOST)
            .put(entityNames(EntityType.Region), RELATED_REGION)
            .put(entityNames(EntityType.Storage), RELATED_STORAGE)
            .putAll(Constants.BASIC_APPLICATION_FIELDS)
            // related groups
            .put(groupNames(GroupType.Resource), RELATED_RESOURCE_GROUP_NAME_FOR_VM)
            .put(groupNames(GroupType.Cluster), RELATED_COMPUTE_HOST_CLUSTER_NAME)
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
            .put(primitive("cpuModel"), PRIMITIVE_CPU_MODEL)
            .put(primitive("model"), PRIMITIVE_MODEL)
            .put(primitive("numCpus"), PRIMITIVE_PM_NUM_CPUS)
            .put(primitive("timezone"), PRIMITIVE_TIMEZONE)
            // commodities
            .put(weightedHistoricalUtilization(CommodityType.BALLOONING), COMMODITY_BALLOONING_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.CPU), COMMODITY_CPU_USED)
            .put(weightedHistoricalUtilization(CommodityType.CPU), COMMODITY_CPU_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.IO_THROUGHPUT), COMMODITY_IO_THROUGHPUT_USED)
            .put(weightedHistoricalUtilization(CommodityType.IO_THROUGHPUT), COMMODITY_IO_THROUGHPUT_HISTORICAL_UTILIZATION)
            .put(capacity(CommodityType.MEM), COMMODITY_MEM_CAPACITY)
            .put(used(CommodityType.MEM), COMMODITY_MEM_USED)
            .put(weightedHistoricalUtilization(CommodityType.MEM), COMMODITY_MEM_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.NET_THROUGHPUT), COMMODITY_NET_THROUGHPUT_USED)
            .put(weightedHistoricalUtilization(CommodityType.NET_THROUGHPUT), COMMODITY_NET_THROUGHPUT_HISTORICAL_UTILIZATION)
            .put(weightedHistoricalUtilization(CommodityType.SWAPPING), COMMODITY_SWAPPING_HISTORICAL_UTILIZATION)
            // related entities
            .put(entityNames(EntityType.DataCenter), RELATED_DATA_CENTER)
            .put(entityNames(EntityType.Storage), RELATED_STORAGE)
            .put(entityNames(EntityType.Switch), RELATED_SWITCH)
            .put(entityCount(EntityType.VirtualMachine), NUM_VMS)
            // related groups
            .put(groupNames(GroupType.Cluster), RELATED_COMPUTE_HOST_CLUSTER_NAME)
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
            .put(used(CommodityType.RESPONSE_TIME), COMMODITY_RESPONSE_TIME_USED)
            .put(used(CommodityType.TRANSACTION), COMMODITY_TRANSACTION_USED)
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
            .put(primitive("attachmentState"), PRIMITIVE_ATTACHMENT_STATE)
            // commodities
            .put(capacity(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_CAPACITY)
            // related entities
            .put(entityNames(EntityType.BusinessAccount), RELATED_ACCOUNT)
            .put(entityNames(EntityType.Region), RELATED_REGION)
            .put(entityNames(EntityType.Storage), RELATED_STORAGE)
            .put(entityNames(EntityType.StorageTier), RELATED_STORAGE_TIER)
            .put(entityNames(EntityType.VirtualMachine), RELATED_VM)
            // related groups
            .put(groupNames(GroupType.Resource), RELATED_RESOURCE_GROUP_NAME_FOR_VV)
            .build();
    }

    /**
     * Returns all relevant column mappings for Virtual Datacenter.
     *
     * @return Virtual Datacenter column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getVirtualDataCenterMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
                .build();
    }

    /**
     * Returns all relevant column mappings for Workload Controller.
     *
     * @return Workload Controller column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getWorkloadControllerMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
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
            .put(primitive("isLocal"), PRIMITIVE_IS_LOCAL)
            // commodities
            .put(used(CommodityType.STORAGE_ACCESS), COMMODITY_STORAGE_ACCESS_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_ACCESS), COMMODITY_STORAGE_ACCESS_HISTORICAL_UTILIZATION)
            .put(capacity(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_CAPACITY)
            .put(used(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.STORAGE_LATENCY), COMMODITY_STORAGE_LATENCY_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_LATENCY), COMMODITY_STORAGE_LATENCY_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.STORAGE_PROVISIONED), COMMODITY_STORAGE_PROVISIONED_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_PROVISIONED), COMMODITY_STORAGE_PROVISIONED_HISTORICAL_UTILIZATION)
            // related entities
            .put(entityNames(EntityType.DataCenter), RELATED_DATA_CENTER)
            .put(entityNames(EntityType.VirtualMachine), RELATED_VM)
            // related groups
            .put(groupNames(GroupType.Cluster), RELATED_COMPUTE_HOST_CLUSTER_NAME)
            .put(groupNames(GroupType.StorageCluster), RELATED_STORAGE_CLUSTER_NAME)
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
            .put(used(CommodityType.STORAGE_ACCESS), COMMODITY_STORAGE_ACCESS_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_ACCESS), COMMODITY_STORAGE_ACCESS_HISTORICAL_UTILIZATION)
            .put(capacity(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_CAPACITY)
            .put(used(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.STORAGE_LATENCY), COMMODITY_STORAGE_LATENCY_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_LATENCY), COMMODITY_STORAGE_LATENCY_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.STORAGE_PROVISIONED), COMMODITY_STORAGE_PROVISIONED_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_PROVISIONED), COMMODITY_STORAGE_PROVISIONED_HISTORICAL_UTILIZATION)
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
            .put(used(CommodityType.STORAGE_ACCESS), COMMODITY_STORAGE_ACCESS_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_ACCESS), COMMODITY_STORAGE_ACCESS_HISTORICAL_UTILIZATION)
            .put(capacity(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_CAPACITY)
            .put(used(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_AMOUNT), COMMODITY_STORAGE_AMOUNT_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.STORAGE_LATENCY), COMMODITY_STORAGE_LATENCY_USED)
            .put(weightedHistoricalUtilization(CommodityType.STORAGE_LATENCY), COMMODITY_STORAGE_LATENCY_HISTORICAL_UTILIZATION)
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
            .put(used(CommodityType.NET_THROUGHPUT), COMMODITY_NET_THROUGHPUT_USED)
            .put(weightedHistoricalUtilization(CommodityType.NET_THROUGHPUT), COMMODITY_NET_THROUGHPUT_HISTORICAL_UTILIZATION)
            .put(weightedHistoricalUtilization(CommodityType.PORT_CHANNEL), COMMODITY_PORT_CHANNEL_HISTORICAL_UTILIZATION)
            .build();
    }

    /**
     * Returns all relevant column mappings for Namespace.
     *
     * @return Namespace column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getNamespaceMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
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
            .put(weightedHistoricalUtilization(CommodityType.NET_THROUGHPUT), COMMODITY_NET_THROUGHPUT_HISTORICAL_UTILIZATION)
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
            .put(weightedHistoricalUtilization(CommodityType.NET_THROUGHPUT), COMMODITY_NET_THROUGHPUT_HISTORICAL_UTILIZATION)
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
            .put(weightedHistoricalUtilization(CommodityType.COOLING), COMMODITY_COOLING_HISTORICAL_UTILIZATION)
            .put(weightedHistoricalUtilization(CommodityType.POWER), COMMODITY_POWER_HISTORICAL_UTILIZATION)
            .put(weightedHistoricalUtilization(CommodityType.SPACE), COMMODITY_SPACE_HISTORICAL_UTILIZATION)
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
            .put(used(CommodityType.IMAGE_CPU), COMMODITY_IMAGE_CPU_USED)
            .put(percentileHistoricalUtilization(CommodityType.IMAGE_CPU), COMMODITY_IMAGE_CPU_PERCENTILE_UTILIZATION)
            .put(used(CommodityType.IMAGE_MEM), COMMODITY_IMAGE_MEM_USED)
            .put(percentileHistoricalUtilization(CommodityType.IMAGE_MEM), COMMODITY_IMAGE_MEM_PERCENTILE_UTILIZATION)
            .put(used(CommodityType.IMAGE_STORAGE), COMMODITY_IMAGE_STORAGE_USED)
            .put(percentileHistoricalUtilization(CommodityType.IMAGE_STORAGE), COMMODITY_IMAGE_STORAGE_PERCENTILE_UTILIZATION)
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
            .put(used(CommodityType.ACTIVE_SESSIONS), COMMODITY_ACTIVE_SESSIONS_USED)
            .put(weightedHistoricalUtilization(CommodityType.ACTIVE_SESSIONS), COMMODITY_ACTIVE_SESSIONS_HISTORICAL_UTILIZATION)
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
            .put(weightedHistoricalUtilization(CommodityType.ACTIVE_SESSIONS), COMMODITY_ACTIVE_SESSIONS_HISTORICAL_UTILIZATION)
            .put(weightedHistoricalUtilization(CommodityType.POOL_CPU), COMMODITY_POOL_CPU_HISTORICAL_UTILIZATION)
            .put(weightedHistoricalUtilization(CommodityType.POOL_MEM), COMMODITY_POOL_MEM_HISTORICAL_UTILIZATION)
            .put(weightedHistoricalUtilization(CommodityType.POOL_STORAGE), COMMODITY_POOL_STORAGE_HISTORICAL_UTILIZATION)
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
            .put(used(CommodityType.VCPU), COMMODITY_VCPU_USED)
            .put(percentileHistoricalUtilization(CommodityType.VCPU), COMMODITY_VCPU_PERCENTILE_UTILIZATION)
            .put(capacity(CommodityType.VMEM), COMMODITY_VMEM_CAPACITY)
            .put(used(CommodityType.VMEM), COMMODITY_VMEM_USED)
            .put(percentileHistoricalUtilization(CommodityType.VMEM), COMMODITY_VMEM_PERCENTILE_UTILIZATION)
            // related entities
            .put(entityNames(EntityType.VirtualMachine), RELATED_VM)
            .put(entityNames(EntityType.Namespace), RELATED_NAMESPACE)
            .putAll(Constants.BASIC_APPLICATION_FIELDS)
            .build();
    }

    /**
     * Returns all relevant column mappings for Container Spec.
     *
     * @return Container Spec column mappings
     */
    private static Map<FieldApiDTO, SearchMetadataMapping> getContainerSpecMetadata() {
        return ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // common fields
                .putAll(Constants.ENTITY_COMMON_FIELDS)
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
            .put(used(CommodityType.VCPU), COMMODITY_VCPU_USED)
            .put(percentileHistoricalUtilization(CommodityType.VCPU), COMMODITY_VCPU_PERCENTILE_UTILIZATION)
            .put(capacity(CommodityType.VMEM), COMMODITY_VMEM_CAPACITY)
            .put(used(CommodityType.VMEM), COMMODITY_VMEM_USED)
            .put(percentileHistoricalUtilization(CommodityType.VMEM), COMMODITY_VMEM_PERCENTILE_UTILIZATION)
            // related entities
            .put(entityNames(EntityType.ContainerPod), RELATED_CONTAINER_POD)
            .put(entityNames(EntityType.Namespace), RELATED_NAMESPACE)
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
            .put(groupNames(GroupType.BillingFamily), RELATED_BILLING_FAMILY_NAME)
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
            .put(used(CommodityType.RESPONSE_TIME), COMMODITY_RESPONSE_TIME_USED)
            .put(used(CommodityType.TRANSACTION), COMMODITY_TRANSACTION_USED)
            // related entities
            .put(entityNames(EntityType.BusinessTransaction), RELATED_BUSINESS_TRANSACTION)
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
            .put(used(CommodityType.RESPONSE_TIME), COMMODITY_RESPONSE_TIME_USED)
            .put(used(CommodityType.TRANSACTION), COMMODITY_TRANSACTION_USED)
            // related entities
            .put(entityNames(EntityType.BusinessApplication), RELATED_BUSINESS_APPLICATION)
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
            .put(used(CommodityType.RESPONSE_TIME), COMMODITY_RESPONSE_TIME_USED)
            .put(used(CommodityType.TRANSACTION), COMMODITY_TRANSACTION_USED)
            // related entities
            .put(entityNames(EntityType.BusinessApplication), RELATED_BUSINESS_APPLICATION)
            .put(entityNames(EntityType.BusinessTransaction), RELATED_BUSINESS_TRANSACTION)
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
            .put(used(CommodityType.CONNECTION), COMMODITY_CONNECTION_USED)
            .put(weightedHistoricalUtilization(CommodityType.CONNECTION), COMMODITY_CONNECTION_HISTORICAL_UTILIZATION)
            .put(capacity(CommodityType.DB_MEM), COMMODITY_DB_MEM_CAPACITY)
            .put(used(CommodityType.DB_MEM), COMMODITY_DB_MEM_USED)
            .put(weightedHistoricalUtilization(CommodityType.DB_MEM), COMMODITY_DB_MEM_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.DB_CACHE_HIT_RATE), COMMODITY_DB_HIT_RATE_USED)
            .put(weightedHistoricalUtilization(CommodityType.DB_CACHE_HIT_RATE), COMMODITY_DB_HIT_RATE_HISTORICAL_UTILIZATION)
            .put(used(CommodityType.VCPU), COMMODITY_VCPU_USED)
            .put(percentileHistoricalUtilization(CommodityType.VCPU), COMMODITY_VCPU_PERCENTILE_UTILIZATION)
            .put(used(CommodityType.VMEM), COMMODITY_VMEM_USED)
            .put(percentileHistoricalUtilization(CommodityType.VMEM), COMMODITY_VMEM_PERCENTILE_UTILIZATION)
            .put(used(CommodityType.VSTORAGE), COMMODITY_VSTORAGE_USED)
            .put(percentileHistoricalUtilization(CommodityType.VSTORAGE), COMMODITY_VSTORAGE_PERCENTILE_UTILIZATION)
            // related entities
            .put(entityNames(EntityType.BusinessAccount), RELATED_ACCOUNT)
            .put(entityNames(EntityType.Region), RELATED_REGION)
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
            .put(used(CommodityType.TRANSACTION), COMMODITY_TRANSACTION_USED)
            .put(used(CommodityType.VSTORAGE), COMMODITY_VSTORAGE_USED)
            .put(percentileHistoricalUtilization(CommodityType.VSTORAGE), COMMODITY_VSTORAGE_PERCENTILE_UTILIZATION)
            // related entities
            .put(entityNames(EntityType.BusinessAccount), RELATED_ACCOUNT)
            .put(entityNames(EntityType.Region), RELATED_REGION)
            .putAll(Constants.BASIC_APPLICATION_FIELDS)
            // related groups
            .put(groupNames(GroupType.Resource), RELATED_RESOURCE_GROUP_NAME_FOR_DB)
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
                .put(PrimitiveFieldApiDTO.oid(), PRIMITIVE_OID)
                .put(PrimitiveFieldApiDTO.entityType(), PRIMITIVE_ENTITY_TYPE)
                .put(PrimitiveFieldApiDTO.name(), PRIMITIVE_NAME)
                .put(PrimitiveFieldApiDTO.severity(), PRIMITIVE_SEVERITY)
                .put(PrimitiveFieldApiDTO.entityState(), PRIMITIVE_STATE)
                .put(PrimitiveFieldApiDTO.environmentType(), PRIMITIVE_ENVIRONMENT_TYPE)
                // RELATED ACTION
                .put(RelatedActionFieldApiDTO.actionCount(), RELATED_ACTION_COUNT)
                .build();

        /**
         * Fields available to entities that can be associated with business applications, transactions, and services.
         */
        static final Map<FieldApiDTO, SearchMetadataMapping> BASIC_APPLICATION_FIELDS =
            ImmutableMap.<FieldApiDTO, SearchMetadataMapping>builder()
                // RELATED ENTITIES
                .put(entityNames(EntityType.BusinessApplication), RELATED_BUSINESS_APPLICATION)
                .put(entityNames(EntityType.BusinessTransaction), RELATED_BUSINESS_TRANSACTION)
                .put(entityNames(EntityType.Service), RELATED_SERVICE)
                .build();
    }
}
