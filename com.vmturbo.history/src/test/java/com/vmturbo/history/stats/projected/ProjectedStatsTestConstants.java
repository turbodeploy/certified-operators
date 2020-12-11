package com.vmturbo.history.stats.projected;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Constants for this test package.
 */
public class ProjectedStatsTestConstants {
    final static CommodityType COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.MEM_VALUE)
            .build();

    final static String COMMODITY = "Mem";
    final static CommodityType COMMODITY_TYPE_WITH_KEY = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.MEM_VALUE)
            .setKey(COMMODITY)
            .build();

    final static String COMMODITY_UNITS = "KB";
}
