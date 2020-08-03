/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.topology.processor.group.settings.applicators;

import java.util.function.Function;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * {@link VmInstanceStoreCommoditiesCreator} creates bought commodity for {@link
 * EntityType#VIRTUAL_MACHINE} which is residing on the template which has instance store disks.
 */
public class VmInstanceStoreCommoditiesCreator
                extends InstanceStoreCommoditiesCreator<CommodityBoughtDTO.Builder> {

    /**
     * Creates {@link VmInstanceStoreCommoditiesCreator} instance.
     */
    public VmInstanceStoreCommoditiesCreator() {
        super(CommodityBoughtDTO::newBuilder, CommodityBoughtDTO.Builder::setCommodityType,
                        (builder, number) -> builder.setUsed(number.doubleValue()),
                        Function.identity(), builder -> builder.getCommodityType().getType());
    }

}
