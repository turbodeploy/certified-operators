package com.vmturbo.mediation.conversion.cloud.converter;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.util.ConverterUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * CloudDiscoveryConverter for Region. It discards any commodities bought and sold, since those commodities
 * don't have valid use cases. In new cloud model, there are only connected relationship
 * between region and other entities, but not consumes relationship. Owns relationship is set up
 * in other converters.
 */
public class RegionConverter implements IEntityConverter {

    private final SDKProbeType probeType;

    public RegionConverter(@Nonnull final SDKProbeType probeType) {
        this.probeType = probeType;
    }

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        if (probeType == SDKProbeType.AZURE) {
            // find the related AZ from the region
            String azId = converter.getAzIdFromRegionId(entity.getId());
            EntityDTO.Builder azEntity = converter.getNewEntityBuilder(azId);
            // add all entity properties (Azure core quota info) from az entity to region entity
            entity.addAllEntityProperties(azEntity.getEntityPropertiesList());
        }

        // discard any commodities bought and sold
        entity.clearCommoditiesBought();

        // discard any commodities sold
        entity.clearCommoditiesSold();

        // add DataCenterCommodity (used for merging two regions in TopologyProcessor and market side)
        entity.addCommoditiesSold(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.DATACENTER)
                .setKey(ConverterUtils.DATACENTER_ACCESS_COMMODITY_PREFIX + entity.getId())
                .build());

        return true;
    }
}
