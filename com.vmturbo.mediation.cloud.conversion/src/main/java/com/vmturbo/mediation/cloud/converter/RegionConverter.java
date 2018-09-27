package com.vmturbo.mediation.cloud.converter;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * CloudDiscoveryConverter for Region. It discards any commodities bought and sold, since those commodities
 * don't have valid use cases. In new cloud model, there are only connected relationship
 * between region and other entities, but not consumes relationship. Owns relationship is set up
 * in other converters.
 */
public class RegionConverter implements IEntityConverter {

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        // discard any commodities bought and sold
        entity.clearCommoditiesBought();

        // discard any commodities sold
        entity.clearCommoditiesSold();

        // add DataCenterCommodity (used for merging two regions in TopologyProcessor and market side)
        entity.addCommoditiesSold(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.DATACENTER)
                .setKey("DataCenter::" + entity.getId())
                .build());
        return true;
    }
}
