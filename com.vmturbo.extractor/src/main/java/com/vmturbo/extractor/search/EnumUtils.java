package com.vmturbo.extractor.search;

import static com.vmturbo.extractor.topology.mapper.EntityTypeMappers.SUPPORTED_ENTITY_TYPE_MAPPING;
import static com.vmturbo.extractor.topology.mapper.GroupMappers.GROUP_TYPE_MAPPING;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.topology.mapper.GroupMappers;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;

/**
 * Classes to perform conversions between various enum types.
 *
 * <p>In this class, the jOOQ generated "DB" enum types are always imported so that they can
 * appear unqualified. Any other types with the same name must be qualified.</p>
 *
 * <p>Conversion methods defined in inner types are all named with patterns that should be
 * evident and should be maintained as this class evolves. Where a conversion is not currently
 * needed in the code, the method is omitted.</p>
 */
public class EnumUtils {
    private EnumUtils() {
    }

    /**
     * Class to perform conversions among EntityType types, limited to entity types whitelisted for
     * search.
     */
    public static class SearchEntityTypeUtils {
        private SearchEntityTypeUtils() {
        }

        /**
         * Convert an API entity type to a Protobuf entity type.
         *
         * @param apiEntityType the API entity type
         * @param def           default value if conversion fails
         * @return the Protobuf entity type, or default if the conversion fails
         */
        public static EntityDTO.EntityType apiToProto(
                com.vmturbo.api.enums.EntityType apiEntityType, EntityDTO.EntityType def) {
            EntityDTO.EntityType protoEntityType = SUPPORTED_ENTITY_TYPE_MAPPING.get(apiEntityType);
            return protoEntityType != null ? protoEntityType : def;
        }

        /**
         * Convert an API entity type to a Protobuf entity type.
         *
         * @param apiEntityType the API entity type
         * @return the Protobuf entity type, or null if the conversion fails
         */
        public static EntityDTO.EntityType apiToProto(com.vmturbo.api.enums.EntityType apiEntityType) {
            return apiToProto(apiEntityType, null);
        }

        /**
         * Convert a Protobuf entity type numeric value to a DB entity type.
         *
         * @param protoEntityTypeInt Protobuf entity type numeric value
         * @param def                default value if conversion fails
         * @return DB entity type, or default if conversion fails
         */
        public static EntityType protoIntToDb(int protoEntityTypeInt, EntityType def) {
            try {
                EntityDTO.EntityType entityProtoType = EntityDTO.EntityType.forNumber(protoEntityTypeInt);
                if (entityProtoType != null) {
                    return EntityType.valueOf(entityProtoType.name());
                }
            } catch (IllegalArgumentException ignored) {
            }
            // either the provided int did not correspond to a proto entity type, or that entity type
            // was not present in the db enum
            return def;
        }

        /**
         * Convert a Protobuf entity type numeric value to a DB entity type.
         *
         * @param protoEntityTypeInt Protobuf entity type numeric value
         * @return DB entity type, or null if conversion fails
         */
        public static EntityType protoIntToDb(int protoEntityTypeInt) {
            return protoIntToDb(protoEntityTypeInt, null);
        }
    }

    /**
     * Class to perform conversions among EntityType types, without limitation to the search
     * entity-type whitelist.
     */
    public static class EntityTypeUtils {
        private EntityTypeUtils() {
        }

        /**
         * Convert a Protobuf entity type numeric value to a DB entity type.
         *
         * @param protoEntityTypeInt Protobuf entity type numeric value
         * @param def                default value if conversion fails
         * @return DB entity type, or default if conversion fails
         */
        public static EntityType protoIntToDb(final int protoEntityTypeInt, final EntityType def) {
            EntityDTO.EntityType protoEntityType = EntityDTO.EntityType.forNumber(protoEntityTypeInt);
            if (protoEntityType != null) {
                try {
                    return EntityType.valueOf(protoEntityType.name());
                } catch (IllegalArgumentException ignored) {
                }
            }
            return def;
        }
    }

    /**
     * Class to perform conversions among GroupType types.
     */
    public static class GroupTypeUtils {

        private GroupTypeUtils() {
        }

        /**
         * Convert an API group type to a Protobuf group type.
         *
         * @param apiGroupType API group type
         * @param def          default value if conversion fails
         * @return Protobuf group type, or default if conversion fails
         */
        public static GroupDTO.GroupType apiToProto(
                com.vmturbo.api.enums.GroupType apiGroupType, GroupDTO.GroupType def) {
            final GroupDTO.GroupType result = GROUP_TYPE_MAPPING.get(apiGroupType);
            return result != null ? result : def;
        }

        /**
         * Convert an API group type to a Protobuf group type.
         *
         * @param apiGroupType API group type
         * @return Protobuf group type, or null if conversion fails
         */
        public static GroupDTO.GroupType apiToProto(com.vmturbo.api.enums.GroupType apiGroupType) {
            return apiToProto(apiGroupType, null);
        }

        /**
         * Convert a Protobuf group type to a DB entity type.
         *
         * @param protoGroupType Protobuf group type
         * @param def            default value if conversion fails
         * @return entity type, or default if conversion fails
         */
        public static EntityType protoToDb(GroupDTO.GroupType protoGroupType, EntityType def) {
            final EntityType result = GroupMappers.mapGroupTypeToEntityType(protoGroupType);
            return result != null ? result : def;
        }

        /**
         * Convert a Protobuf group type to a DB entity type.
         *
         * @param protoGroupType Protobuf group type
         * @return entity type, or null if conversion fails
         */
        public static EntityType protoToDb(GroupDTO.GroupType protoGroupType) {
            return protoToDb(protoGroupType, null);
        }

        /**
         * Convert from Protobuf group type numeric value to DB entity type.
         *
         * @param protoGroupTypeInt Protobuf group type numeric value
         * @param def               default if conversion fails
         * @return DB entity type, or default if conversion fails
         */
        public static EntityType protoIntToDb(int protoGroupTypeInt, EntityType def) {
            GroupDTO.GroupType groupType = GroupDTO.GroupType.forNumber(protoGroupTypeInt);
            EntityType result = null;
            if (groupType != null) {
                result = GroupMappers.mapGroupTypeToEntityType(groupType);
            }
            return result != null ? result : def;
        }

        /**
         * Convert from Protobuf group type numeric value to DB entity type.
         *
         * @param protoGroupTypeInt Protobuf group type numeric value
         * @return DB entity type, or null if conversion fails
         */
        public static EntityType protoIntToDb(int protoGroupTypeInt) {
            return protoIntToDb(protoGroupTypeInt, null);
        }
    }

    /**
     * Class to perform conversions among EnvironmentType types.
     */
    public static class EnvironmentTypeUtils {
        private EnvironmentTypeUtils() {
        }

        /**
         * Convert a Protobuf environment type to a DB environment type.
         *
         * @param protoEnvironmentType Protobuf environment type
         * @param def                  default value if conversion fails
         * @return DB environment type, or default if conversion fails
         */
        public static EnvironmentType protoToDb(
                EnvironmentTypeEnum.EnvironmentType protoEnvironmentType, EnvironmentType def) {
            try {
                return EnvironmentType.valueOf(protoEnvironmentType.name());
            } catch (IllegalArgumentException e) {
                return def;
            }
        }

        /**
         * Convert a Protobuf environment type to a DB environment type.
         *
         * @param protoEnvironmentType Protobuf environment type
         * @return DB environment type, or null if conversion fails
         */
        public static EnvironmentType protoToDb(
                EnvironmentTypeEnum.EnvironmentType protoEnvironmentType) {
            return protoToDb(protoEnvironmentType, null);
        }
    }

    /**
     * Class to perform conversions among EntityState types.
     */
    public static class EntityStateUtils {
        private EntityStateUtils() {
        }

        /**
         * Convert a Protobuf entity state to a DB entity state.
         *
         * @param protoEntityState Protobuf entity type
         * @param def              default of conversion fails
         * @return DB entity type, or default if conversion fails
         */
        public static EntityState protoToDb(TopologyDTO.EntityState protoEntityState, EntityState def) {
            try {
                return EntityState.valueOf(protoEntityState.name());
            } catch (IllegalArgumentException e) {
                return def;
            }
        }

        /**
         * Convert a Protobuf entity state to a DB entity state.
         *
         * @param protoEntityState Protobuf entity type
         * @return DB entity type, or null if conversion fails
         */
        public static EntityState protoToDb(TopologyDTO.EntityState protoEntityState) {
            return protoToDb(protoEntityState, null);
        }

        /**
         * Convert a Protobuf entity state numeric value to a DB entity state.
         *
         * @param protoEntityStateInt Protobuf entity state numeric value
         * @param def                 default if conversion fails
         * @return DB entity state, or default if conversion fails
         */
        public static EntityState protoIntToDb(int protoEntityStateInt, EntityState def) {
            EntityState result = null;
            TopologyDTO.EntityState protoEntityState =
                    TopologyDTO.EntityState.forNumber(protoEntityStateInt);
            if (protoEntityState != null) {
                result = protoToDb(protoEntityState, null);
            }
            return result != null ? result : def;
        }

        /**
         * Convert a Protobuf entity state numeric value to a DB entity state.
         *
         * @param protoEntityStateInt Protobuf entity state numeric value
         * @return DB entity state, or null if conversion fails
         */
        public static EntityState protoIntToDb(int protoEntityStateInt) {
            return protoIntToDb(protoEntityStateInt, null);
        }
    }

    /**
     * Class to perform conversions among CommodityType types.
     */
    public static class CommodityTypeUtils {
        private CommodityTypeUtils() {
        }

        /**
         * Convert an API commodity type to a Protobuf commodity type.
         *
         * @param apiCommodityType API commodity type
         * @param def              default value if conversion fails
         * @return Protobuf commodity type, or default if conversion fails
         */
        public static CommodityDTO.CommodityType apiToProto(
                com.vmturbo.api.enums.CommodityType apiCommodityType, CommodityDTO.CommodityType def) {
            // port_channel seems to be the only case whose name doesn't match
            if (apiCommodityType == com.vmturbo.api.enums.CommodityType.PORT_CHANNEL) {
                return CommodityDTO.CommodityType.PORT_CHANEL;
            }
            try {
                return CommodityType.valueOf(apiCommodityType.name());
            } catch (IllegalArgumentException e) {
                return def;
            }
        }

        /**
         * Convert an API commodity type to a Protobuf commodity type.
         *
         * @param apiCommodityType API commodity type
         * @return Protobuf commodity type, or null if conversion fails
         */
        public static CommodityDTO.CommodityType apiToProto(
                com.vmturbo.api.enums.CommodityType apiCommodityType) {
            return apiToProto(apiCommodityType, null);
        }

        /**
         * Convert a Protobuf commodity type numeric value to a DB commodity type.
         *
         * @param protoCommodityTypeInt Protobuf commodity type numeric value
         * @param def                   default if conversion fails
         * @return DB commodity type, or default if conversion fails
         */
        public static MetricType protoIntToDb(int protoCommodityTypeInt, MetricType def) {
            MetricType result = null;
            try {
                CommodityDTO.CommodityType protoCommType =
                        CommodityDTO.CommodityType.forNumber(protoCommodityTypeInt);
                if (protoCommType != null) {
                    result = MetricType.valueOf(protoCommType.name());
                }
            } catch (IllegalArgumentException ignored) {
            }
            return result != null ? result : def;
        }

        /**
         * Convert a Protobuf commodity type numeric value to a DB commodity type.
         *
         * @param protoCommodityTypeInt Protobuf commodity type numeric value
         * @return DB commodity type, or null if conversion fails
         */
        public static MetricType protoIntToDb(int protoCommodityTypeInt) {
            return protoIntToDb(protoCommodityTypeInt, null);
        }
    }
}
