package com.vmturbo.extractor.search;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.topology.mapper.GroupMappers;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;

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
    private static final BiMap<GroupType, GroupDTO.GroupType> GROUP_TYPE_MAPPING =
            new ImmutableBiMap.Builder<GroupType, GroupDTO.GroupType>()
                    .put(GroupType.GROUP, GroupDTO.GroupType.REGULAR)
                    .put(GroupType.RESOURCE, GroupDTO.GroupType.RESOURCE)
                    .put(GroupType.COMPUTE_HOST_CLUSTER, GroupDTO.GroupType.COMPUTE_HOST_CLUSTER)
                    .put(GroupType.STORAGE_CLUSTER, GroupDTO.GroupType.STORAGE_CLUSTER)
                    .put(GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER, GroupDTO.GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER)
                    .put(GroupType.BILLING_FAMILY, GroupDTO.GroupType.BILLING_FAMILY)
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
    public static EntityType groupTypeFromProtoToDb(GroupDTO.GroupType groupType) {
        return GroupMappers.mapGroupTypeToName(groupType);
    }

    /**
     * Convert from proto {@link EntityDTO.EntityType} to db {@link EntityType}.
     *
     * @param protoIntEntityType integer value of proto {@link EntityDTO.EntityType}
     * @return db {@link EntityType}
     * @throws IllegalArgumentException if the conversion fails
     */
    public static EntityType entityTypeFromProtoIntToDb(int protoIntEntityType) throws IllegalArgumentException {
        EntityType entityType = entityTypeFromProtoIntToDb(protoIntEntityType, null);
        if (entityType == null) {
            throw new IllegalArgumentException("Can not find matching db EntityType for " + protoIntEntityType);
        }
        return entityType;
    }

    /**
     * Convert from proto {@link EntityDTO.EntityType} numeric value to db {@link EntityType}, with
     * a default value to return if the conversion fails.
     *
     * @param protoIntEntityType the int value underlying an protobuf entity type
     * @param dflt               default value to return if conversion fails
     * @return db entity type, supplied default
     */
    public static EntityType entityTypeFromProtoIntToDb(int protoIntEntityType, EntityType dflt) {
        try {
            EntityDTO.EntityType entityProtoType = EntityDTO.EntityType.forNumber(protoIntEntityType);
            if (entityProtoType != null) {
                return EntityType.valueOf(entityProtoType.name());
            }
        } catch (IllegalArgumentException ignored) {
        }
        // either the provided int did not correspond to a proto entity type, or that entity type
        // was not present in the db enum
        return dflt;
    }

    /**
     * Convert from proto {@link TopologyDTO.EntityState} to db {@link EntityState}.
     *
     * @param entityState proto {@link TopologyDTO.EntityState}
     * @return db {@link EntityState}
     * @throws IllegalArgumentException if the conversion fails
     */
    public static EntityState entityStateFromProtoToDb(TopologyDTO.EntityState entityState)
            throws IllegalArgumentException {
        return EntityState.valueOf(entityState.name());
    }

    /**
     * Convert from proto {@link TopologyDTO.EntityState} to db {@link EntityState}, with a default
     * value for when conversion fails.
     *
     * @param entityState proto {@link TopologyDTO.EntityState}
     * @param dflt        value to return if conversion fails
     * @return db {@link EntityState}
     */
    public static EntityState entityStateFromProtoToDb(TopologyDTO.EntityState entityState, EntityState dflt) {
        try {
            return entityStateFromProtoToDb(entityState);
        } catch (IllegalArgumentException e) {
            return dflt;
        }
    }

    /**
     * Convert from proto {@link EnvironmentTypeEnum.EnvironmentType} to db {@link
     * EnvironmentType}.
     *
     * @param environmentType proto {@link EnvironmentTypeEnum.EnvironmentType}
     * @return db {@link EnvironmentType}
     * @throws IllegalArgumentException if the conversion fails
     */
    public static EnvironmentType environmentTypeFromProtoToDb(EnvironmentTypeEnum.EnvironmentType environmentType)
            throws IllegalArgumentException {
        return EnvironmentType.valueOf(environmentType.name());
    }

    /**
     * Convert from proto {@link EnvironmentTypeEnum.EnvironmentType} to db {@link EnvironmentType},
     * or a supplied default if conversion fails.
     *
     * @param environmentType proto {@link EnvironmentTypeEnum.EnvironmentType}
     * @param dflt            value to provide if conversion fails
     * @return db {@link EnvironmentType}
     */
    public static EnvironmentType environmentTypeFromProtoToDb(
            EnvironmentTypeEnum.EnvironmentType environmentType, EnvironmentType dflt) {
        try {
            return environmentTypeFromProtoToDb(environmentType);
        } catch (IllegalArgumentException e) {
            return dflt;
        }
    }

    /**
     * Convert from api {@link CommodityType} to proto {@link CommodityDTO.CommodityType} integer
     * value.
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
     * Convert a {@link CommodityDTO.CommodityType} numeric value to a db {@link MetricType} enum,
     * with a default value for when conversion fails.
     *
     * @param protoIntCommType int value of a {@link CommodityDTO.CommodityType} to be converted
     * @param dflt             default value if conversion fails
     * @return converted value
     */
    public static MetricType commodityTypeFromProtoIntToDb(int protoIntCommType, MetricType dflt) {
        try {
            CommodityDTO.CommodityType protoCommType =
                    CommodityDTO.CommodityType.forNumber(protoIntCommType);
            if (protoCommType != null) {
                return MetricType.valueOf(protoCommType.name());
            }
        } catch (IllegalArgumentException ignored) {
        }
        // here if either the number didn't correspond to a commodity type, or we had not
        // metric type for the commodity type
        return dflt;
    }

    /**
     * Convert from api {@link GroupType} to proto {@link GroupDTO.GroupType}.
     *
     * @param apiGroupType api {@link GroupType}
     * @return proto {@link GroupDTO.GroupType}
     */
    public static GroupDTO.GroupType groupTypeFromApiToProto(GroupType apiGroupType) {
        switch (apiGroupType) {
            case GROUP:
                return GroupDTO.GroupType.REGULAR;
            case RESOURCE:
                return GroupDTO.GroupType.RESOURCE;
            case COMPUTE_HOST_CLUSTER:
                return GroupDTO.GroupType.COMPUTE_HOST_CLUSTER;
            case COMPUTE_VIRTUAL_MACHINE_CLUSTER:
                return GroupDTO.GroupType.COMPUTE_VIRTUAL_MACHINE_CLUSTER;
            case STORAGE_CLUSTER:
                return GroupDTO.GroupType.STORAGE_CLUSTER;
            case BILLING_FAMILY:
                return GroupDTO.GroupType.BILLING_FAMILY;
            default:
                return GroupDTO.GroupType.REGULAR;
        }
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
