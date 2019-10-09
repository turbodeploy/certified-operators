package com.vmturbo.market.topology.conversions;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Factory to create test entities for Cloud topologies.
 */
class CloudTestEntityFactory {

    static final long REGION_ID = 1L;
    static final long TIER_ID = 100L;
    static final long ZONE_ID = 4;
    static final String FAMILY_NAME = "compute_optimized";
    static final String TIER_NAME = "compute_optimized_medium";
    static final String REGION_NAME = "antarctica";

    static TopologyEntityDTO mockRegion() {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setDisplayName(REGION_NAME)
                .setOid(REGION_ID)
                .build();
    }

    static TopologyEntityDTO mockComputeTier() {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(TIER_ID)
                .setDisplayName(TIER_NAME)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setFamily(FAMILY_NAME)
                                .build()).build())
                .addAllCommoditySoldList(createComputeTierSoldCommodities())
                .addConnectedEntityList(ConnectedEntity.newBuilder().setConnectedEntityId(REGION_ID)
                        .setConnectedEntityType(EntityType.REGION_VALUE)
                        .build())
                .build();
    }

    private static List<CommoditySoldDTO> createComputeTierSoldCommodities() {
        return ImmutableList.of(createLicenseAccessCommoditySoldDTO("Linux"),
                createLicenseAccessCommoditySoldDTO("Windows"),
                createLicenseAccessCommoditySoldDTO("RHEL"));
    }

    private static CommoditySoldDTO createLicenseAccessCommoditySoldDTO(final String platform) {
        return CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setKey(platform)
                        .build())
                .build();
    }
}
