/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.topology.processor.group.settings.applicators;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * {@link ComputeTierInstanceStorePolicyApplicator} creates sold commodities for {@link
 * EntityType#COMPUTE_TIER} which has instance store storages.
 */
public class ComputeTierInstanceStorePolicyApplicator
                extends InstanceStorePolicyApplicator<CommoditySoldDTO.Builder> {

    /**
     * Creates {@link ComputeTierInstanceStorePolicyApplicator} instance.
     */
    public ComputeTierInstanceStorePolicyApplicator() {
        super(EntityType.COMPUTE_TIER, CommoditySoldDTO::newBuilder,
                        CommoditySoldDTO.Builder::setCommodityType,
                        (builder, number) -> builder.setCapacity(number.doubleValue()),
                        (builder) -> builder.setActive(true).setIsResizeable(false),
                        builder -> builder.getCommodityType().getType());
    }

    @Override
    protected void populateInstanceStoreCommodities(@Nonnull Builder entity) {
        addCommodities(Builder::getCommoditySoldListBuilderList, Builder::addCommoditySoldList,
                        entity, entity.getTypeSpecificInfo().getComputeTier());
    }

    @Override
    protected boolean isApplicable(@Nonnull Builder entity, @Nonnull Setting setting) {
        return super.isApplicable(entity, setting) && hasComputeTierInfo(entity);
    }

}
