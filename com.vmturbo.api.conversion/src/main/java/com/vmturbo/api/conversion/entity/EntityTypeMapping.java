package com.vmturbo.api.conversion.entity;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * This class provides mapping between SDK entity types and API entity types.
 */
public class EntityTypeMapping {

    private EntityTypeMapping() {
    }

    /**
     * The map from SDK type to API Entity type.
     */
    public static final Map<EntityDTO.EntityType, EntityType> ENTITY_TYPE_TO_API_STRING =
        ImmutableMap.<EntityDTO.EntityType, EntityType>builder()
            .put(EntityDTO.EntityType.APPLICATION_COMPONENT, EntityType.ApplicationComponent)
            .put(EntityDTO.EntityType.SERVICE, EntityType.Service)
            .put(EntityDTO.EntityType.APPLICATION, EntityType.Application)
            .put(EntityDTO.EntityType.APPLICATION_SERVER, EntityType.ApplicationServer)
            .put(EntityDTO.EntityType.AVAILABILITY_ZONE, EntityType.AvailabilityZone)
            .put(EntityDTO.EntityType.BUSINESS_ACCOUNT, EntityType.BusinessAccount)
            .put(EntityDTO.EntityType.BUSINESS_APPLICATION, EntityType.BusinessApplication)
            .put(EntityDTO.EntityType.BUSINESS_TRANSACTION, EntityType.BusinessTransaction)
            .put(EntityDTO.EntityType.BUSINESS_USER, EntityType.BusinessUser)
            .put(EntityDTO.EntityType.CHASSIS, EntityType.Chassis)
            .put(EntityDTO.EntityType.CLOUD_SERVICE, EntityType.CloudService)
            .put(EntityDTO.EntityType.COMPUTE_TIER, EntityType.ComputeTier)
            .put(EntityDTO.EntityType.CONTAINER, EntityType.Container)
            .put(EntityDTO.EntityType.CONTAINER_POD, EntityType.ContainerPod)
            .put(EntityDTO.EntityType.DATABASE, EntityType.Database)
            .put(EntityDTO.EntityType.DATABASE_SERVER, EntityType.DatabaseServer)
            .put(EntityDTO.EntityType.DATABASE_SERVER_TIER, EntityType.DatabaseServerTier)
            .put(EntityDTO.EntityType.DATABASE_TIER, EntityType.DatabaseTier)
            .put(EntityDTO.EntityType.DATACENTER, EntityType.DataCenter)
            .put(EntityDTO.EntityType.DESKTOP_POOL, EntityType.DesktopPool)
            .put(EntityDTO.EntityType.DISK_ARRAY, EntityType.DiskArray)
            .put(EntityDTO.EntityType.DPOD, EntityType.DPod)
            .put(EntityDTO.EntityType.HYPERVISOR_SERVER, EntityType.HypervisorServer)
            .put(EntityDTO.EntityType.INTERNET, EntityType.Internet)
            .put(EntityDTO.EntityType.IO_MODULE, EntityType.IOModule)
            .put(EntityDTO.EntityType.LOAD_BALANCER, EntityType.LoadBalancer)
            .put(EntityDTO.EntityType.LOGICAL_POOL, EntityType.LogicalPool)
            .put(EntityDTO.EntityType.NETWORK, EntityType.Network)
            .put(EntityDTO.EntityType.PHYSICAL_MACHINE, EntityType.PhysicalMachine)
            .put(EntityDTO.EntityType.HCI_PHYSICAL_MACHINE, EntityType.HCIPhysicalMachine)
            .put(EntityDTO.EntityType.PROCESSOR_POOL, EntityType.ProcessorPool)
            .put(EntityDTO.EntityType.REGION, EntityType.Region)
            .put(EntityDTO.EntityType.RESERVED_INSTANCE, EntityType.ReservedInstance)
            .put(EntityDTO.EntityType.SERVICE_PROVIDER, EntityType.ServiceProvider)
            .put(EntityDTO.EntityType.STORAGE_CONTROLLER, EntityType.StorageController)
            .put(EntityDTO.EntityType.STORAGE, EntityType.Storage)
            .put(EntityDTO.EntityType.STORAGE_TIER, EntityType.StorageTier)
            .put(EntityDTO.EntityType.SWITCH, EntityType.Switch)
            .put(EntityDTO.EntityType.UNKNOWN, EntityType.Unknown)
            .put(EntityDTO.EntityType.VIEW_POD, EntityType.ViewPod)
            .put(EntityDTO.EntityType.VIRTUAL_APPLICATION, EntityType.VirtualApplication)
            .put(EntityDTO.EntityType.VIRTUAL_DATACENTER, EntityType.VirtualDataCenter)
            .put(EntityDTO.EntityType.VIRTUAL_MACHINE, EntityType.VirtualMachine)
            .put(EntityDTO.EntityType.VIRTUAL_VOLUME, EntityType.VirtualVolume)
            .put(EntityDTO.EntityType.VPOD, EntityType.VPod)
            .put(EntityDTO.EntityType.CONTAINER_PLATFORM_CLUSTER, EntityType.ContainerPlatformCluster)
            .put(EntityDTO.EntityType.NAMESPACE, EntityType.Namespace)
            .put(EntityDTO.EntityType.WORKLOAD_CONTROLLER, EntityType.WorkloadController)
            .put(EntityDTO.EntityType.CONTAINER_SPEC, EntityType.ContainerSpec)
            .put(EntityDTO.EntityType.VM_SPEC, EntityType.VMSpec)
            .put(EntityDTO.EntityType.CLOUD_COMMITMENT, EntityType.CloudCommitment)
            .build();

    /**
     * Gets the API entity type based on sdk entity type.
     *
     * @param entityType the sdk api type.
     * @return the api type.
     */
    public static EntityType getApiEntityType(EntityDTO.EntityType entityType) {
        return ENTITY_TYPE_TO_API_STRING.getOrDefault(entityType, EntityType.Unknown);
    }
}
