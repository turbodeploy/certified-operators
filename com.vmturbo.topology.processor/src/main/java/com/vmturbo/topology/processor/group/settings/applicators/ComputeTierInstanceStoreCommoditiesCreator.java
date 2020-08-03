/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.topology.processor.group.settings.applicators;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * {@link ComputeTierInstanceStoreCommoditiesCreator} creates sold commodities for {@link
 * EntityType#COMPUTE_TIER} which has instance store storages.
 */
public class ComputeTierInstanceStoreCommoditiesCreator
                extends InstanceStoreCommoditiesCreator<CommoditySoldDTO.Builder> {

    /**
     * Creates {@link ComputeTierInstanceStoreCommoditiesCreator} instance.
     */
    public ComputeTierInstanceStoreCommoditiesCreator() {
        super(CommoditySoldDTO::newBuilder, CommoditySoldDTO.Builder::setCommodityType,
                        (builder, number) -> builder.setCapacity(number.doubleValue()),
                        (builder) -> builder.setActive(true).setIsResizeable(false),
                        builder -> builder.getCommodityType().getType());
    }
}
