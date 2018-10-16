package com.vmturbo.mediation.cloud.converter;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.mediation.cloud.util.ConverterUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * The AZ rewirer will remove all commodities bought and sold -- they are not relevant on AZ
 * in XL, because we expect all commodities to be bought and sold between VM/DB and Compute
 * Tiers/Storage Tiers. So we will shift all AZ buyers to the appropriate Tier entities.
 *
 * It will also need to be connected to the region(s) it is related to.
 */
public class AvailabilityZoneConverter implements IEntityConverter {

    private SDKProbeType probeType;

    public AvailabilityZoneConverter(@Nonnull SDKProbeType probeType) {
        this.probeType = probeType;
    }

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        // remove AZ for azure
        if (probeType == SDKProbeType.AZURE) {
            return false;
        }

        // find region and make it owns AZ
        String regionId = converter.getRegionIdFromAzId(entity.getId());
        EntityDTO.Builder regionEntity = converter.getNewEntityBuilder(regionId);
        regionEntity.addConsistsOf(entity.getId());

        // discard any commodities bought and sold
        entity.clearCommoditiesBought();
        entity.clearCommoditiesSold();

        // sell ZONE access commodity, whose key is az id
        entity.addCommoditiesSold(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.ZONE)
                .setKey(entity.getId())
                .setCapacity(ConverterUtils.DEFAULT_ACCESS_COMMODITY_CAPACITY)
                .build());

        return true;
    }
}
