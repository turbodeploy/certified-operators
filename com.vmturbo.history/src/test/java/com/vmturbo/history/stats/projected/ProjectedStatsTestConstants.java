package com.vmturbo.history.stats.projected;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * Constants for this test package.
 */
public class ProjectedStatsTestConstants {
    final static CommodityType COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.MEM.getNumber())
            .build();

    final static String COMMODITY = "Mem";

    final static String COMMODITY_UNITS = "KB";
}
