package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Settings applicator, that requires multiple settings to be processed.
 */
@FunctionalInterface
interface SettingApplicator {

    /**
     * Applies settings to the specified entity.
     *
     * @param entity entity to apply settings to
     * @param settings settings to apply
     */
    void apply(@Nonnull TopologyEntityDTO.Builder entity,
            @Nonnull Map<EntitySettingSpecs, Setting> settings);

    /**
     * Utility method to retrieve entity sold commodities.
     *
     * @param entity entity
     * @param commodityType commodity type
     * @return sold commodities
     */
    @Nonnull
    static Collection<CommoditySoldDTO.Builder> getCommoditySoldBuilders(
            @Nonnull TopologyEntityDTO.Builder entity, CommodityType commodityType) {
        return entity.getCommoditySoldListBuilderList().stream().filter(
                commodity -> commodity.getCommodityType().getType() == commodityType.getNumber())
                .collect(Collectors.toList());
    }
}
