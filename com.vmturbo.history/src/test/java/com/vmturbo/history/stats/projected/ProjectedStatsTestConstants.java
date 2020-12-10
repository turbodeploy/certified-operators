package com.vmturbo.history.stats.projected;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Constants for this test package.
 */
public class ProjectedStatsTestConstants {
    private ProjectedStatsTestConstants() {
    }

    static final CommodityType COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.MEM_VALUE)
            .build();

    static final String COMMODITY = "Mem";
    static final CommodityDTO.CommodityType COMMODITY_SDK_TYPE = CommodityDTO.CommodityType.MEM;
    static final CommodityType COMMODITY_TYPE_WITH_KEY = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.MEM_VALUE)
            .setKey(COMMODITY)
            .build();

    static final String COMMODITY_UNITS = "KB";
}
