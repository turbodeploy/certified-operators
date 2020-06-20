package com.vmturbo.extractor.search;

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
 * Enum mapping from sdk type to db type.
 * todo: move this into {@link com.vmturbo.search.metadata.SearchEntityMetadataMapping}, and
 * put together with with Viktor's mappings.
 */
public class EnumUtils {

    /**
     * Private constructor.
     */
    private EnumUtils() {}

    public static EntityType protoGroupTypeToDbType(GroupType groupType) {
        return EntityType.valueOf(groupType.name());
    }

    public static EntityType protoIntEntityTypeToDbType(int protoIntEntityType) {
        EntityDTO.EntityType entityType = EntityDTO.EntityType.forNumber(protoIntEntityType);
        if (entityType == null) {
            throw new IllegalArgumentException("Can not find matching db EntityType for " + protoIntEntityType);
        }
        return EntityType.valueOf(entityType.name());
    }

    public static EntityState protoEntityStateToDbState(TopologyDTO.EntityState entityState) {
        return EntityState.valueOf(entityState.name());
    }

    public static EnvironmentType protoEnvironmentTypeToDbType(
            EnvironmentTypeEnum.EnvironmentType environmentType) {
        return EnvironmentType.valueOf(environmentType.name());
    }

    public static int apiCommodityTypeToProtoInt(CommodityType apiCommodityType) {
        // port_chanel seems to be the only case whose name doesn't match
        if (apiCommodityType == CommodityType.PORT_CHANNEL) {
            return CommodityDTO.CommodityType.PORT_CHANEL.getNumber();
        }
        CommodityDTO.CommodityType commodityType =
                CommodityDTO.CommodityType.valueOf(apiCommodityType.name());
        return commodityType.getNumber();
    }
}
