package com.vmturbo.extractor.topology.mapper;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility class to support mappings of entity type enums.
 */
public class EntityTypeMappers {

    private EntityTypeMappers() {
    }

    /**
     * Mapping between {@link com.vmturbo.api.enums.EntityType} and {@link EntityDTO.EntityType}. It
     * contains all the supported entity types mappings which are needed for ingestion.
     */
    public static final BiMap<com.vmturbo.api.enums.EntityType, EntityDTO.EntityType> SUPPORTED_ENTITY_TYPE_MAPPING =
            new ImmutableBiMap.Builder<com.vmturbo.api.enums.EntityType, EntityDTO.EntityType>()
                    .put(com.vmturbo.api.enums.EntityType.Application, EntityDTO.EntityType.APPLICATION)
                    .put(com.vmturbo.api.enums.EntityType.ApplicationComponent, EntityDTO.EntityType.APPLICATION_COMPONENT)
                    .put(com.vmturbo.api.enums.EntityType.BusinessAccount, EntityDTO.EntityType.BUSINESS_ACCOUNT)
                    .put(com.vmturbo.api.enums.EntityType.BusinessApplication, EntityDTO.EntityType.BUSINESS_APPLICATION)
                    .put(com.vmturbo.api.enums.EntityType.BusinessTransaction, EntityDTO.EntityType.BUSINESS_TRANSACTION)
                    .put(com.vmturbo.api.enums.EntityType.BusinessUser, EntityDTO.EntityType.BUSINESS_USER)
                    .put(com.vmturbo.api.enums.EntityType.Chassis, EntityDTO.EntityType.CHASSIS)
                    .put(com.vmturbo.api.enums.EntityType.Container, EntityDTO.EntityType.CONTAINER)
                    .put(com.vmturbo.api.enums.EntityType.ContainerPod, EntityDTO.EntityType.CONTAINER_POD)
                    .put(com.vmturbo.api.enums.EntityType.ContainerSpec, EntityDTO.EntityType.CONTAINER_SPEC)
                    .put(com.vmturbo.api.enums.EntityType.Database, EntityDTO.EntityType.DATABASE)
                    .put(com.vmturbo.api.enums.EntityType.DatabaseServer, EntityDTO.EntityType.DATABASE_SERVER)
                    .put(com.vmturbo.api.enums.EntityType.DataCenter, EntityDTO.EntityType.DATACENTER)
                    .put(com.vmturbo.api.enums.EntityType.DesktopPool, EntityDTO.EntityType.DESKTOP_POOL)
                    .put(com.vmturbo.api.enums.EntityType.DiskArray, EntityDTO.EntityType.DISK_ARRAY)
                    .put(com.vmturbo.api.enums.EntityType.IOModule, EntityDTO.EntityType.IO_MODULE)
                    .put(com.vmturbo.api.enums.EntityType.Namespace, EntityDTO.EntityType.NAMESPACE)
                    .put(com.vmturbo.api.enums.EntityType.Network, EntityDTO.EntityType.NETWORK)
                    .put(com.vmturbo.api.enums.EntityType.Region, EntityDTO.EntityType.REGION)
                    .put(com.vmturbo.api.enums.EntityType.PhysicalMachine, EntityDTO.EntityType.PHYSICAL_MACHINE)
                    .put(com.vmturbo.api.enums.EntityType.Service, EntityDTO.EntityType.SERVICE)
                    .put(com.vmturbo.api.enums.EntityType.ServiceProvider, EntityType.SERVICE_PROVIDER)
                    .put(com.vmturbo.api.enums.EntityType.Storage, EntityDTO.EntityType.STORAGE)
                    .put(com.vmturbo.api.enums.EntityType.StorageController, EntityDTO.EntityType.STORAGE_CONTROLLER)
                    .put(com.vmturbo.api.enums.EntityType.StorageTier, EntityDTO.EntityType.STORAGE_TIER)
                    .put(com.vmturbo.api.enums.EntityType.Switch, EntityDTO.EntityType.SWITCH)
                    .put(com.vmturbo.api.enums.EntityType.ViewPod, EntityDTO.EntityType.VIEW_POD)
                    .put(com.vmturbo.api.enums.EntityType.VirtualDataCenter, EntityDTO.EntityType.VIRTUAL_DATACENTER)
                    .put(com.vmturbo.api.enums.EntityType.VirtualMachine, EntityDTO.EntityType.VIRTUAL_MACHINE)
                    .put(com.vmturbo.api.enums.EntityType.VirtualVolume, EntityDTO.EntityType.VIRTUAL_VOLUME)
                    .put(com.vmturbo.api.enums.EntityType.WorkloadController, EntityDTO.EntityType.WORKLOAD_CONTROLLER)
                    .build();
}
