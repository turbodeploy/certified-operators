package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class tests methods in the ReservedInstanceDataProcessor class.
 */
public class ReservedInstanceDataProcessorTest {

    static final long MASTER_ID = 11111;

    static final long ZONE_ID = 33333;

    static final long ZONE_ID_1 = 77777;

    static final long REGION_ID = 1234;

    static final long COMPUTE_TIER_ID = 1234;

    /**
     * This method tests ReservedInstanceDataProcessor::applyZonalRIs method.
     */
    @Test
    public void testApplyZonalRIs() {
        TopologyEntityDTO buyComputeTier = buildComputeTierDTO();
        ReservedInstanceZonalContext context = new ReservedInstanceZonalContext(MASTER_ID,
                OSType.LINUX, Tenancy.DEFAULT, buyComputeTier, ZONE_ID);

        // This RI is applicable for the context and hence the resultant demand would be reduced
        // based on the number of coupons this RI has. The number of coupons contributed by this RI
        // is 1. So all values in demand array would also be reduced by 1.
        final ReservedInstanceSpecInfo specInfo1 = ReservedInstanceSpecInfo.newBuilder()
                .setRegionId(REGION_ID).setOs(OSType.LINUX).setTierId(COMPUTE_TIER_ID).build();
        final ReservedInstanceSpec riSpec1 = ReservedInstanceSpec.newBuilder().setId(1)
                .setReservedInstanceSpecInfo(specInfo1).build();

        final ReservedInstanceBoughtInfo applicableRI = ReservedInstanceBoughtInfo.newBuilder()
                .setAvailabilityZoneId(ZONE_ID).setBusinessAccountId(MASTER_ID)
                .setReservedInstanceSpec(riSpec1.getId())
                .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                        .setNumberOfCoupons(1).build()).setNumBought(1).build();

        // This RI is not applicable for the context and hence the resultant demand would NOT be
        // reduced because of this RI
        final ReservedInstanceSpecInfo specInfo2 = ReservedInstanceSpecInfo.newBuilder()
                .setRegionId(REGION_ID).setOs(OSType.WINDOWS).setTierId(COMPUTE_TIER_ID).build();
        final ReservedInstanceSpec riSpec2 = ReservedInstanceSpec.newBuilder().setId(2l)
                .setReservedInstanceSpecInfo(specInfo2).build();

        final ReservedInstanceBoughtInfo notApplicableRI = ReservedInstanceBoughtInfo.newBuilder()
                .setAvailabilityZoneId(ZONE_ID).setBusinessAccountId(MASTER_ID)
                .setReservedInstanceSpec(riSpec2.getId())
                .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                        .setNumberOfCoupons(1).build()).setNumBought(1).build();


        List<ReservedInstanceBoughtInfo> riBoughts = Lists.newArrayList(applicableRI, notApplicableRI);
        ReservedInstanceAnalyzerRateAndRIs priceAndRIProvider = Mockito
                .mock(ReservedInstanceAnalyzerRateAndRIs.class);
        Mockito.when(priceAndRIProvider
                .lookupReservedInstanceBoughtInfos(anyLong(), anyLong(), anyString())).thenReturn(riBoughts);
        Mockito.when(priceAndRIProvider
                .lookupReservedInstanceSpecWithId(applicableRI.getReservedInstanceSpec()))
                .thenReturn(riSpec1);
        Mockito.when(priceAndRIProvider
                .lookupReservedInstanceSpecWithId(notApplicableRI.getReservedInstanceSpec()))
                .thenReturn(riSpec2);


        float[] demand = new float[168];
        Arrays.fill(demand, 1f);
        demand[0] = 10;
        demand[1] = 9;
        demand[2] = 8;
        ReservedInstanceDataProcessor processor = new ReservedInstanceDataProcessor();
        processor.applyZonalRIs(demand, context, buyComputeTier, priceAndRIProvider);
        assertEquals(9, demand[0], 0.0);
        assertEquals(8, demand[1], 0.0);
        assertEquals(7, demand[2], 0.0);
    }

    /**
     * This method tests ReservedInstanceDataProcessor::applyRegionalRIs method.
     */
    @Test
    public void testApplyRegionalRIs() {
        TopologyEntityDTO buyComputeTier = buildComputeTierDTO();
        TopologyEntityDTO region = buildRegionDTO();
        final ReservedInstanceSpecInfo specInfo1 = ReservedInstanceSpecInfo.newBuilder()
                .setRegionId(REGION_ID).setOs(OSType.LINUX).setTierId(COMPUTE_TIER_ID).build();
        final ReservedInstanceSpec riSpec1 = ReservedInstanceSpec.newBuilder().setId(1)
                .setReservedInstanceSpecInfo(specInfo1).build();

        // This RI is applicable for the context and hence the resultant demand would be reduced
        // based on the number of coupons this RI has. The number of coupons contributed by this RI
        // is 1. So all values in demand array would also be reduced by 1.
        final ReservedInstanceBoughtInfo applicableRI = ReservedInstanceBoughtInfo.newBuilder()
                .setAvailabilityZoneId(ZONE_ID).setBusinessAccountId(MASTER_ID)
                .setReservedInstanceSpec(riSpec1.getId())
                .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                        .setNumberOfCoupons(1).build()).setNumBought(1).build();

        ReservedInstanceRegionalContext regionalContext = new ReservedInstanceRegionalContext(MASTER_ID,
                OSType.LINUX, Tenancy.DEFAULT, buyComputeTier, region);

        ReservedInstanceAnalyzerRateAndRIs priceAndRIProvider = Mockito
                .mock(ReservedInstanceAnalyzerRateAndRIs.class);
        List<ReservedInstanceBoughtInfo> riBoughts = Lists.newArrayList(applicableRI);
        Mockito.when(priceAndRIProvider.lookupReservedInstancesBoughtInfos(any(), anyString()))
                .thenReturn(riBoughts);
        Mockito.when(priceAndRIProvider.lookupReservedInstanceSpecWithId(applicableRI.getReservedInstanceSpec()))
                .thenReturn(riSpec1);

        Map<ReservedInstanceZonalContext, float[]> zonalContexts = new HashMap<>();
        ReservedInstanceZonalContext context = new ReservedInstanceZonalContext(MASTER_ID,
                OSType.LINUX, Tenancy.DEFAULT, buyComputeTier, ZONE_ID);
        float[] demand = new float[168];
        Arrays.fill(demand, 1f);
        demand[0] = 21;
        demand[1] = 20;
        demand[2] = 19;
        zonalContexts.put(context, demand);

        Map<Long, TopologyEntityDTO> cloudEntities = new HashMap<>();
        cloudEntities.put(COMPUTE_TIER_ID, buyComputeTier);

        ReservedInstanceDataProcessor processor = new ReservedInstanceDataProcessor();
        processor.applyRegionalRIs(zonalContexts, regionalContext, priceAndRIProvider, cloudEntities);
        assertEquals(20, demand[0], 0.0);
        assertEquals(19, demand[1], 0.0);
        assertEquals(18, demand[2], 0.0);
    }

    /**
     * This method tests ReservedInstanceDataProcessor::getCouponDemandByTemplate method.
     */
    @Test
    public void testGetCouponDemandByTemplate() {
        final TopologyEntityDTO buyComputeTier = buildComputeTierDTO();
        final Map<ReservedInstanceZonalContext, float[]> zonalContexts = new HashMap<>();

        final ReservedInstanceZonalContext context1 = new ReservedInstanceZonalContext(MASTER_ID,
                OSType.LINUX, Tenancy.DEFAULT, buyComputeTier, ZONE_ID);
        final float[] demand1 = new float[168];
        Arrays.fill(demand1, 1f);

        final ReservedInstanceZonalContext context2 = new ReservedInstanceZonalContext(MASTER_ID,
                OSType.LINUX, Tenancy.DEFAULT, buyComputeTier, ZONE_ID_1);
        final float[] demand2 = new float[168];
        Arrays.fill(demand2, 2f);

        zonalContexts.put(context1, demand1);
        zonalContexts.put(context2, demand2);

        final ReservedInstanceDataProcessor processor = new ReservedInstanceDataProcessor();
        final Map<TopologyEntityDTO, float[]> couponDemandByTemplate = processor
                .getCouponDemandByTemplate(zonalContexts, 1);

        assertEquals(1, couponDemandByTemplate.size());
        final float[] result = couponDemandByTemplate.values().stream().findFirst().get();
        // The demand for both the contexts would collapse into a single demand since both of them
        // are using the same compute tier (template).
        assertEquals(3f, result[0], 0f);
        assertEquals(3f, result[167], 0f);
        assertEquals(3f, result[90], 0f);
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
                                .setNumCoupons(1)))
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
