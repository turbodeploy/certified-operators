/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.topology.processor.group.settings.applicators;

import java.util.function.Function;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * {@link VmInstanceStoreCommoditiesCreator} creates bought commodity for {@link
 * EntityType#VIRTUAL_MACHINE} which is residing on the template which has instance store disks.
 */
public class VmInstanceStoreCommoditiesCreator
                extends InstanceStoreCommoditiesCreator<CommodityBoughtImpl> {

    /**
     * Creates {@link VmInstanceStoreCommoditiesCreator} instance.
     */
    public VmInstanceStoreCommoditiesCreator() {
        super(CommodityBoughtImpl::new, CommodityBoughtImpl::setCommodityType,
                        (builder, number) -> builder.setUsed(number.doubleValue()),
                        Function.identity(), builder -> builder.getCommodityType().getType());
    }

}
