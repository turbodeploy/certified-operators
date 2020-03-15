package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class tests methods in the ReservedInstanceAnalyzer class.
 */
public class ReservedInstanceAnalyzerTest {

    static final long MASTER_ID = 11111;

    static final long ZONE_ID = 33333;

    static final long REGION_ID = 1234;

    static final long COMPUTE_TIER_ID = 1234;

     /**
     * This method tests ReservedInstanceAnalyzer::getHourlyOnDemandCost method.
     **/
    @Ignore
    public void testGetHourlyOnDemandCost() {
        //TODO use new classes
        /*TopologyEntityDTO buyComputeTier = buildComputeTierDTO();
        TopologyEntityDTO region = buildRegionDTO();
        Map<TopologyEntityDTO, float[]> templateTypeHourlyDemand = new HashMap<>();
        float[] demand = new float[ReservedInstanceDataProcessor.WEEKLY_DEMAND_DATA_SIZE];
        Arrays.fill(demand, 4);
        templateTypeHourlyDemand.put(buyComputeTier, demand);
        ReservedInstanceRegionalContext regionalContext = new ReservedInstanceRegionalContext(MASTER_ID,
                OSType.LINUX, Tenancy.DEFAULT, buyComputeTier, region);
        ReservedInstanceAnalyzerRateAndRIs priceAndRIProvider = Mockito
                .mock(ReservedInstanceAnalyzerRateAndRIs.class);
        Mockito.when(priceAndRIProvider.lookupOnDemandRate(any(), any())).thenReturn(1f);
        ReservedInstanceAnalyzer analyzer = new ReservedInstanceAnalyzer();
        final float hourlyOnDemandCost = analyzer.getHourlyOnDemandCost(templateTypeHourlyDemand,
                        regionalContext, priceAndRIProvider, "RILT0000");
        assertEquals(1f, hourlyOnDemandCost, 0.0);
        */
    }

    /**
     * Returns a compute tier DTO.
     * @return returns a compute tier DTO.
     */
    private TopologyEntityDTO buildComputeTierDTO() {
        return TopologyEntityDTO.newBuilder()
                .setOid(COMPUTE_TIER_ID)
                .setDisplayName("computeTier")
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setFamily("familyA")
                                .setNumCoupons(4)))
                .build();
    }

    /**
     * Returns a region DTO.
     * @return returns a region DTO.
     */
    private TopologyEntityDTO buildRegionDTO() {
        return TopologyEntityDTO.newBuilder()
                .setOid(REGION_ID)
                .setDisplayName("region")
                .setEntityType(EntityType.REGION_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(ZONE_ID)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setConnectionType(ConnectionType.OWNS_CONNECTION))
                .build();
    }
}
