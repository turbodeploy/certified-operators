/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.topology.processor.group.settings.applicators;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * {@link ComputeTierInstanceStoreCommoditiesCreator} creates sold commodities for {@link
 * EntityType#COMPUTE_TIER} which has instance store storages.
 */
public class ComputeTierInstanceStoreCommoditiesCreator
                extends InstanceStoreCommoditiesCreator<CommoditySoldImpl> {

    /**
     * Creates {@link ComputeTierInstanceStoreCommoditiesCreator} instance.
     */
    public ComputeTierInstanceStoreCommoditiesCreator() {
        super(CommoditySoldImpl::new, CommoditySoldImpl::setCommodityType,
                        (builder, number) -> builder.setCapacity(number.doubleValue()),
                        (builder) -> builder.setActive(true).setIsResizeable(false),
                        builder -> builder.getCommodityType().getType());
    }
}
