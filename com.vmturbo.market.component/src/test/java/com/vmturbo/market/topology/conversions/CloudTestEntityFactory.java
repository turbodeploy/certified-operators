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
    static final long REGION_ID_2 = 2L;
    static final long TIER_ID = 100L;
    static final long TIER_ID_2 = 101L;
    static final long ZONE_ID = 4;
    static final long RI_BOUGHT_ID = 1234L;
    static final long RI_BOUGHT_ID_2 = 2345L;
    static final String FAMILY_NAME = "compute_optimized";
    static final String TIER_NAME = "compute_optimized_medium";
    static final String REGION_NAME = "antarctica";
    static final String REGION_NAME_2 = "oceania";
    static final String TIER_NAME_2 = "compute_optimized_large";


    static TopologyEntityDTO mockRegion(long oid, String name) {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setDisplayName(name)
                .setOid(oid)
                .build();
    }

    static TopologyEntityDTO mockComputeTier() {
        return mockComputeTier(TIER_ID, true);
    }

    static TopologyEntityDTO mockComputeTier(long oid, boolean connectToRegion) {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(oid)
                .setDisplayName(TIER_NAME)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setFamily(FAMILY_NAME)
                                .build()).build())
                .addAllCommoditySoldList(createComputeTierSoldCommodities());
        if (connectToRegion) {
            builder.addConnectedEntityList(ConnectedEntity.newBuilder().setConnectedEntityId(REGION_ID)
                    .setConnectedEntityType(EntityType.REGION_VALUE)
                    .build());
        }
        return builder.build();
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
