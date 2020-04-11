package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Junit tests for ReservedInstanceContext, ReservedInstanceRegionalContext and ReservedInstanceZonalContext.
 */
public class ReservedInstanceContextTest {
    static final long MASTER_ID = 11111;
    static final long MASTER_ID_1 = 11112;
    static final long REGION_ID = 22222;
    static final long REGION_ID_1 = 22223;
    static final long ZONE_ID = 33333;
    static final long ZONE_ID_1 = 33334;

    private static final TopologyEntityDTO COMPUTE_TIER =  TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(7)
        .build();
    private static final TopologyEntityDTO COMPUTE_TIER_1 =  TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(17)
        .build();
    private static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.REGION_VALUE)
                    .setOid(REGION_ID)
                    .setDisplayName("REGION")
                    .build();
    private static final TopologyEntityDTO REGION_1 = TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.REGION_VALUE)
                    .setOid(REGION_ID_1)
                    .setDisplayName("REGION_1")
                    .build();
    private static final TopologyEntityDTO REGION_Z = TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.REGION_VALUE)
                    .setOid(ZONE_ID)
                    .setDisplayName("REGION_Z")
                    .build();

    /**
     * Test ReservedInstanceRegionalContext construction.
     */
    @Test
    public void testReservedInstanceRegionalContextConstructor() {
        ReservedInstanceRegionalContext context = new ReservedInstanceRegionalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER, REGION);
        Assert.assertTrue(context.getAccountId() == MASTER_ID);
        Assert.assertTrue(context.getPlatform() == OSType.LINUX);
        Assert.assertTrue(context.getPlatform() != OSType.WINDOWS);
        Assert.assertTrue(context.getTenancy() == Tenancy.DEFAULT);
        Assert.assertTrue(context.getTenancy() != Tenancy.DEDICATED);
        Assert.assertTrue(context.getTenancy() != Tenancy.HOST);
        Assert.assertTrue(context.getComputeTier().equals(COMPUTE_TIER));
        Assert.assertTrue(context.isInstanceSizeFlexible() == true);
        Assert.assertTrue(context.getRegionId() == REGION_ID);
        Assert.assertTrue("REGION".equals(context.getRegionDisplayName()));

        ReservedInstanceContext context1 = new ReservedInstanceRegionalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER, REGION);
        Assert.assertTrue(context1.isInstanceSizeFlexible() == true);
        Assert.assertTrue(context.equals(context1));
        Assert.assertTrue(context.hashCode() == context1.hashCode());

        ReservedInstanceContext context2 = new ReservedInstanceRegionalContext(MASTER_ID_1,
            OSType.WINDOWS, Tenancy.DEFAULT, COMPUTE_TIER, REGION_1);
        Assert.assertTrue(context2.isInstanceSizeFlexible() == false);
        Assert.assertFalse(context.equals(context2));
        Assert.assertFalse(context.hashCode() == context2.hashCode());

        ReservedInstanceContext context3 = new ReservedInstanceRegionalContext(MASTER_ID_1,
            OSType.LINUX, Tenancy.HOST, COMPUTE_TIER, REGION_1);
        Assert.assertTrue(context3.isInstanceSizeFlexible() == false);
        Assert.assertFalse(context.equals(context3));
        Assert.assertFalse(context.hashCode() == context3.hashCode());

        ReservedInstanceContext context4 = new ReservedInstanceRegionalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER_1, REGION);
        Assert.assertTrue(context4.isInstanceSizeFlexible() == true);
        Assert.assertFalse(context.equals(context4));
        Assert.assertFalse(context.hashCode() == context4.hashCode());

        ReservedInstanceContext context5 = new ReservedInstanceRegionalContext(MASTER_ID_1,
            OSType.WINDOWS, Tenancy.DEDICATED, COMPUTE_TIER, REGION_1);
        Assert.assertTrue(context5.isInstanceSizeFlexible() == false);
        Assert.assertFalse(context.equals(context5));
        Assert.assertFalse(context.hashCode() == context5.hashCode());
    }

    /**
     * Test ReservedInstanceZonalContext construction.
     */
    @Test
    public void testReservedInstanceZonalContextConstructor() {
        ReservedInstanceZonalContext context = new ReservedInstanceZonalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER, ZONE_ID);
        Assert.assertTrue(context.getAccountId() == MASTER_ID);
        Assert.assertTrue(context.getPlatform() == OSType.LINUX);
        Assert.assertTrue(context.getPlatform() != OSType.WINDOWS);
        Assert.assertTrue(context.getTenancy() == Tenancy.DEFAULT);
        Assert.assertTrue(context.getTenancy() != Tenancy.DEDICATED);
        Assert.assertTrue(context.getTenancy() != Tenancy.HOST);
        Assert.assertTrue(context.getComputeTier().equals(COMPUTE_TIER));
        Assert.assertTrue(context.isInstanceSizeFlexible() == false);
        Assert.assertTrue(context.getAvailabilityZoneId() == ZONE_ID);

        ReservedInstanceContext context1 = new ReservedInstanceZonalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER, ZONE_ID);
        Assert.assertTrue(context1.isInstanceSizeFlexible() == false);
        Assert.assertTrue(context.equals(context1));
        Assert.assertTrue(context.hashCode() == context1.hashCode());

        ReservedInstanceContext context2 = new ReservedInstanceZonalContext(MASTER_ID_1,
            OSType.WINDOWS, Tenancy.DEDICATED, COMPUTE_TIER, ZONE_ID_1);
        Assert.assertTrue(context2.isInstanceSizeFlexible() == false);
        Assert.assertFalse(context.equals(context2));
        Assert.assertFalse(context.hashCode() == context2.hashCode());

        ReservedInstanceContext context3 = new ReservedInstanceZonalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER_1, ZONE_ID);
        Assert.assertTrue(context3.isInstanceSizeFlexible() == false);
        Assert.assertFalse(context.equals(context3));
        Assert.assertFalse(context.hashCode() == context3.hashCode());
    }

    /**
     * Test ReservedInstanceContext construction.
     */
    @Test
    public void testReservedInstanceContextConstructor() {
        ReservedInstanceContext zonalContext = new ReservedInstanceZonalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER, ZONE_ID);
        Assert.assertTrue(zonalContext.getAccountId() == MASTER_ID);
        Assert.assertTrue(zonalContext.getPlatform() == OSType.LINUX);
        Assert.assertTrue(zonalContext.getPlatform() != OSType.WINDOWS);
        Assert.assertTrue(zonalContext.getTenancy() == Tenancy.DEFAULT);
        Assert.assertTrue(zonalContext.getTenancy() != Tenancy.DEDICATED);
        Assert.assertTrue(zonalContext.getTenancy() != Tenancy.HOST);
        Assert.assertTrue(zonalContext.getComputeTier().equals(COMPUTE_TIER));
        Assert.assertTrue(zonalContext.isInstanceSizeFlexible() == false);

        ReservedInstanceContext zonalContext1 = new ReservedInstanceZonalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER, ZONE_ID);
        Assert.assertTrue(zonalContext1.isInstanceSizeFlexible() == false);
        Assert.assertTrue(zonalContext.equals(zonalContext1));

        ReservedInstanceContext zonalContext2 = new ReservedInstanceZonalContext(MASTER_ID_1,
            OSType.WINDOWS, Tenancy.DEDICATED, COMPUTE_TIER, ZONE_ID_1);
        Assert.assertTrue(zonalContext2.isInstanceSizeFlexible() == false);
        Assert.assertFalse(zonalContext.equals(zonalContext2));

        ReservedInstanceContext zonalContext3 = new ReservedInstanceZonalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER_1, ZONE_ID);
        Assert.assertTrue(zonalContext3.isInstanceSizeFlexible() == false);
        Assert.assertFalse(zonalContext.equals(zonalContext3));

        // REGION_Z is a Region with ID == ZONE_ID
        ReservedInstanceContext regionalContext = new ReservedInstanceRegionalContext(MASTER_ID,
            OSType.LINUX, Tenancy.DEFAULT, COMPUTE_TIER_1, REGION_Z);
        Assert.assertTrue(regionalContext.isInstanceSizeFlexible() == true);
        Assert.assertFalse(zonalContext.equals(regionalContext));

        ReservedInstanceContext regionalContext1 = new ReservedInstanceRegionalContext(MASTER_ID,
            OSType.LINUX, Tenancy.HOST, COMPUTE_TIER_1, REGION_Z);
        Assert.assertTrue(regionalContext1.isInstanceSizeFlexible() == false);
        Assert.assertFalse(zonalContext.equals(regionalContext1));
        Assert.assertFalse(regionalContext.equals(regionalContext1));
    }
}
