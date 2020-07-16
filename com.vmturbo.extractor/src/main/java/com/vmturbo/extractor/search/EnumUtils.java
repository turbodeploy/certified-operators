package com.vmturbo.extractor.search;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Enum mapping between sdk type, api type and db type.
 */
public class EnumUtils {

    /**
     * Mapping between {@link com.vmturbo.api.enums.EntityType} and
     * {@link EntityDTO.EntityType}.
     * It contains all the supported entity types mappings which are needed for ingestion.
     */
    private static final BiMap<com.vmturbo.api.enums.EntityType, EntityDTO.EntityType> SUPPORTED_ENTITY_TYPE_MAPPING =
            new ImmutableBiMap.Builder<com.vmturbo.api.enums.EntityType, EntityDTO.EntityType>()
                    .put(com.vmturbo.api.enums.EntityType.APPLICATION, EntityDTO.EntityType.APPLICATION)
                    .put(com.vmturbo.api.enums.EntityType.APPLICATION_COMPONENT, EntityDTO.EntityType.APPLICATION_COMPONENT)
                    .put(com.vmturbo.api.enums.EntityType.BUSINESS_ACCOUNT, EntityDTO.EntityType.BUSINESS_ACCOUNT)
                    .put(com.vmturbo.api.enums.EntityType.BUSINESS_APPLICATION, EntityDTO.EntityType.BUSINESS_APPLICATION)
                    .put(com.vmturbo.api.enums.EntityType.BUSINESS_TRANSACTION, EntityDTO.EntityType.BUSINESS_TRANSACTION)
                    .put(com.vmturbo.api.enums.EntityType.BUSINESS_USER, EntityDTO.EntityType.BUSINESS_USER)
                    .put(com.vmturbo.api.enums.EntityType.CHASSIS, EntityDTO.EntityType.CHASSIS)
                    .put(com.vmturbo.api.enums.EntityType.CONTAINER, EntityDTO.EntityType.CONTAINER)
                    .put(com.vmturbo.api.enums.EntityType.CONTAINER_POD, EntityDTO.EntityType.CONTAINER_POD)
                    .put(com.vmturbo.api.enums.EntityType.CONTAINER_SPEC, EntityDTO.EntityType.CONTAINER_SPEC)
                    .put(com.vmturbo.api.enums.EntityType.DATABASE, EntityDTO.EntityType.DATABASE)
                    .put(com.vmturbo.api.enums.EntityType.DATABASE_SERVER, EntityDTO.EntityType.DATABASE_SERVER)
                    .put(com.vmturbo.api.enums.EntityType.DATACENTER, EntityDTO.EntityType.DATACENTER)
                    .put(com.vmturbo.api.enums.EntityType.DESKTOP_POOL, EntityDTO.EntityType.DESKTOP_POOL)
                    .put(com.vmturbo.api.enums.EntityType.DISKARRAY, EntityDTO.EntityType.DISK_ARRAY)
                    .put(com.vmturbo.api.enums.EntityType.IOMODULE, EntityDTO.EntityType.IO_MODULE)
                    .put(com.vmturbo.api.enums.EntityType.NAMESPACE, EntityDTO.EntityType.NAMESPACE)
                    .put(com.vmturbo.api.enums.EntityType.NETWORK, EntityDTO.EntityType.NETWORK)
                    .put(com.vmturbo.api.enums.EntityType.REGION, EntityDTO.EntityType.REGION)
                    .put(com.vmturbo.api.enums.EntityType.PHYSICAL_MACHINE, EntityDTO.EntityType.PHYSICAL_MACHINE)
                    .put(com.vmturbo.api.enums.EntityType.SERVICE, EntityDTO.EntityType.SERVICE)
                    .put(com.vmturbo.api.enums.EntityType.STORAGE, EntityDTO.EntityType.STORAGE)
                    .put(com.vmturbo.api.enums.EntityType.STORAGECONTROLLER, EntityDTO.EntityType.STORAGE_CONTROLLER)
                    .put(com.vmturbo.api.enums.EntityType.STORAGE_TIER, EntityDTO.EntityType.STORAGE_TIER)
                    .put(com.vmturbo.api.enums.EntityType.SWITCH, EntityDTO.EntityType.SWITCH)
                    .put(com.vmturbo.api.enums.EntityType.VIEW_POD, EntityDTO.EntityType.VIEW_POD)
                    .put(com.vmturbo.api.enums.EntityType.VIRTUAL_DATACENTER, EntityDTO.EntityType.VIRTUAL_DATACENTER)
                    .put(com.vmturbo.api.enums.EntityType.VIRTUAL_MACHINE, EntityDTO.EntityType.VIRTUAL_MACHINE)
                    .put(com.vmturbo.api.enums.EntityType.VIRTUAL_VOLUME, EntityDTO.EntityType.VIRTUAL_VOLUME)
                    .put(com.vmturbo.api.enums.EntityType.WORKLOAD_CONTROLLER, EntityDTO.EntityType.WORKLOAD_CONTROLLER)
                    .build();

    /**
     * Mapping between {@link com.vmturbo.api.enums.GroupType} and
     * {@link GroupType}.
     */
    private static final BiMap<com.vmturbo.api.enums.GroupType, GroupType> GROUP_TYPE_MAPPING =
            new ImmutableBiMap.Builder<com.vmturbo.api.enums.GroupType, GroupType>()
                    .put(com.vmturbo.api.enums.GroupType.GROUP, GroupType.REGULAR)
                    .put(com.vmturbo.api.enums.GroupType.RESOURCE, GroupType.RESOURCE)
                    .put(com.vmturbo.api.enums.GroupType.COMPUTE_HOST_CLUSTER, GroupType.COMPUTE_HOST_CLUSTER)
                    .put(com.vmturbo.api.enums.GroupType.STORAGE_CLUSTER, GroupType.STORAGE_CLUSTER)
                    .put(com.vmturbo.api.enums.GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER, GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER)
                    .put(com.vmturbo.api.enums.GroupType.BILLING_FAMILY, GroupType.BILLING_FAMILY)
                    .build();

    /**
     * Private constructor.
     */
    private EnumUtils() {}

    /**
     * Convert from proto {@link GroupType} to db {@link EntityType}.
     *
     * @param groupType {@link GroupType}
     * @return db {@link EntityType}
     */
    public static EntityType groupTypeFromProtoToDb(GroupType groupType) {
        return EntityType.valueOf(groupType.name());
    }

    /**
     * Convert from proto {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType}
     * to db {@link EntityType}.
     *
     * @param protoIntEntityType integer value of proto {@link com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType}
     * @return db {@link EntityType}
     */
    public static EntityType entityTypeFromProtoIntToDb(int protoIntEntityType) {
        EntityDTO.EntityType entityType = EntityDTO.EntityType.forNumber(protoIntEntityType);
        if (entityType == null) {
            throw new IllegalArgumentException("Can not find matching db EntityType for " + protoIntEntityType);
        }
        return EntityType.valueOf(entityType.name());
    }

    /**
     * Convert from proto {@link TopologyDTO.EntityState} to db {@link EntityState}.
     *
     * @param entityState proto {@link TopologyDTO.EntityState}
     * @return db {@link EntityState}
     */
    public static EntityState entityStateFromProtoToDb(TopologyDTO.EntityState entityState) {
        return EntityState.valueOf(entityState.name());
    }

    /**
     * Convert from proto {@link EnvironmentTypeEnum.EnvironmentType} to db {@link EnvironmentType}.
     *
     * @param environmentType proto {@link EnvironmentTypeEnum.EnvironmentType}
     * @return db {@link EnvironmentType}
     */
    public static EnvironmentType environmentTypeFromProtoToDb(
            EnvironmentTypeEnum.EnvironmentType environmentType) {
        return EnvironmentType.valueOf(environmentType.name());
    }

    /**
     * Convert from api {@link CommodityType} to proto {@link CommodityDTO.CommodityType} integer value.
     *
     * @param apiCommodityType api {@link CommodityType}
     * @return proto {@link CommodityDTO.CommodityType} integer value
     */
    public static int commodityTypeFromApiToProtoInt(CommodityType apiCommodityType) {
        // port_chanel seems to be the only case whose name doesn't match
        if (apiCommodityType == CommodityType.PORT_CHANNEL) {
            return CommodityDTO.CommodityType.PORT_CHANEL.getNumber();
        }
        CommodityDTO.CommodityType commodityType =
                CommodityDTO.CommodityType.valueOf(apiCommodityType.name());
        return commodityType.getNumber();
    }

    /**
     * Convert from api {@link com.vmturbo.api.enums.GroupType} to proto {@link GroupType}.
     *
     * @param apiGroupType api {@link com.vmturbo.api.enums.GroupType}
     * @return proto {@link GroupType}
     */
    public static GroupType groupTypeFromApiToProto(com.vmturbo.api.enums.GroupType apiGroupType) {
        return GROUP_TYPE_MAPPING.get(apiGroupType);
    }

    /**
     * Convert an {@link com.vmturbo.api.enums.EntityType} to {@link EntityDTO.EntityType}.
     *
     * @param apiEntityType api {@link com.vmturbo.api.enums.EntityType}
     * @return {@link EntityDTO.EntityType}
     */
    public static EntityDTO.EntityType entityTypeFromApiToProto(
            com.vmturbo.api.enums.EntityType apiEntityType) {
        EntityDTO.EntityType protoEntityType = SUPPORTED_ENTITY_TYPE_MAPPING.get(apiEntityType);
        if (protoEntityType != null) {
            return protoEntityType;
        }
        throw new IllegalArgumentException("Unsupported api EntityType: " + apiEntityType);
    }
}
