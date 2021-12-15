package com.vmturbo.topology.processor.history;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.UtilizationData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for ICommodityFieldAccessor.
 */
public class ICommodityFieldAccessorTest extends BaseGraphRelatedTest {
    private static final int ENTITY_TYPE = 1;
    private static final long OID1 = 1;
    private static final long OID2 = 2;
    private static final long OID3 = 3;
    private static final long OID4 = 4;
    private static final CommodityType CT1 = CommodityType.newBuilder().setType(1).build();
    private static final CommodityType CT2 = CommodityType.newBuilder().setType(2).build();
    private static final CommodityType CT3 = CommodityType.newBuilder().setType(3).setKey("qqq").build();
    private static final double CAPACITY1 = 12d;
    private static final double CAPACITY2 = 11231d;
    private static final double CAPACITY3 = 43543d;
    private static final double USED1 = 2d;
    private static final double USED2 = 3d;
    private static final double USED3 = 4d;
    private static final double DELTA = 0.001;

    private TopologyEntity entity1;
    private TopologyEntity entity2;
    private TopologyEntity entity3;
    private TopologyGraph<TopologyEntity> graph;

    /**
     * Initialize the testing.
     */
    @Before
    public void setUp() {
        entity1 = mockEntity(ENTITY_TYPE, OID1, CT1, CAPACITY1, USED1, null, null, null, null, true);
        entity2 = mockEntity(ENTITY_TYPE, OID2, CT2, CAPACITY2, USED2, OID1, CT1, USED1, null, true);
        entity3 = mockEntity(ENTITY_TYPE, OID3, CT3, CAPACITY3, USED3, OID2, CT2, USED2,
                             UtilizationData.newBuilder().setLastPointTimestampMs(0)
                                             .setIntervalMs(1).addPoint(USED3).build(), true);
        final TopologyEntity entity4 = mockEntity(ENTITY_TYPE, OID4, CT1, CAPACITY1, null, null,
            null, null, null, true);
        graph = mockGraph(ImmutableSet.of(entity1, entity2, entity3, entity4));
    }

    /**
     * Test that getRealTimeValue returns usages with different parameters passed.
     */
    @Test
    public void testGetRealTimeValue() {
        ICommodityFieldAccessor accessor = new CommodityFieldAccessor(graph);
        // sold
        Double used = accessor.getRealTimeValue(new EntityCommodityFieldReference(OID1, CT1, CommodityField.USED));
        Assert.assertEquals(USED1, used, DELTA);
        Double peak = accessor.getRealTimeValue(new EntityCommodityFieldReference(OID1, CT1, CommodityField.PEAK));
        Assert.assertEquals(0.0, peak, DELTA);
        used = accessor.getRealTimeValue(new EntityCommodityFieldReference(OID2, CT1, CommodityField.USED));
        Assert.assertNull(used);
        // bought
        used = accessor.getRealTimeValue(new EntityCommodityFieldReference(OID2, CT1, OID1, CommodityField.USED));
        Assert.assertEquals(USED1, used, DELTA);
        peak = accessor.getRealTimeValue(new EntityCommodityFieldReference(OID2, CT2, OID1, CommodityField.PEAK));
        Assert.assertNull(peak);
        used = accessor.getRealTimeValue(new EntityCommodityFieldReference(OID2, CT3, OID1, CommodityField.USED));
        Assert.assertNull(used);
    }

    /**
     * Test that if CommoditySoldDTO has used value unset, then CommodityFieldAccessor returns
     * null as the used value.
     */
    @Test
    public void testGetUnsetRealTimeValue() {
        final ICommodityFieldAccessor accessor = new CommodityFieldAccessor(graph);
        final Double used = accessor.getRealTimeValue(new EntityCommodityFieldReference(OID4, CT1,
            CommodityField.USED));
        Assert.assertNull(used);
    }

    /**
     * Test that getCapacity returns capacities with different parameters passed.
     */
    @Test
    public void testGetCapacity() {
        ICommodityFieldAccessor accessor = new CommodityFieldAccessor(graph);
        // sold
        Double cap = accessor.getCapacity(new EntityCommodityReference(OID1, CT1, null));
        Assert.assertEquals(CAPACITY1, cap, DELTA);
        cap = accessor.getCapacity(new EntityCommodityReference(OID2, CT1, null));
        Assert.assertNull(cap);
        // bought
        cap = accessor.getCapacity(new EntityCommodityReference(OID2, CT1, OID1));
        Assert.assertEquals(CAPACITY1, cap, DELTA);
        cap = accessor.getCapacity(new EntityCommodityReference(OID2, CT2, OID1));
        Assert.assertNull(cap);
    }

    /**
     * Test that getUtilizationData returns utilization data with different parameters passed.
     */
    @Test
    public void testGetUtilizationData() {
        ICommodityFieldAccessor accessor = new CommodityFieldAccessor(graph);
        UtilizationData data = accessor.getUtilizationData(new EntityCommodityReference(OID1, CT1, null));
        Assert.assertNull(data);
        data = accessor.getUtilizationData(new EntityCommodityReference(OID3, CT3, null));
        Assert.assertNotNull(data);
        Assert.assertEquals(1, data.getPointCount());
        Assert.assertEquals(USED3, data.getPoint(0), DELTA);
    }

    /**
     * Test that updateHistoryValue sets the commodity field.
     */
    @Test
    public void testUpdateHistoryValue() {
        ICommodityFieldAccessor accessor = new CommodityFieldAccessor(graph);
        CommoditySoldDTO.Builder sold = entity1.getTopologyEntityDtoBuilder().getCommoditySoldListBuilder(0);
        Assert.assertFalse(sold.hasHistoricalUsed());
        Double percentile = 0.5d;
        accessor.updateHistoryValue(new EntityCommodityFieldReference(OID1, CT1,
                                                                      CommodityField.USED),
                                    hv -> hv.setPercentile(percentile), "");
        Assert.assertTrue(sold.hasHistoricalUsed());
        Assert.assertEquals(percentile, sold.getHistoricalUsed().getPercentile(), DELTA);
    }

    /**
     * Test applyInsufficientHistoricalDataPolicy when only VMEM does not have enough data
     * and when both VMEM and VCPU do not have enough data to pass min observation period check.
     */
    @Test
    public void testApplyInsufficientHistoricalDataPolicy() {
        final long vmOid = 1234567L;
        TopologyEntityDTO.Builder vmBuilder = TopologyEntityDTO.newBuilder().setOid(vmOid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(
                        CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE)).setIsResizeable(true))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(
                        CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE)).setIsResizeable(true));
        EntityCommodityFieldReference vmemRef = new EntityCommodityFieldReference(vmOid, CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VMEM_VALUE).build(),
                CommodityField.USED);
        ICommodityFieldAccessor accessor = new CommodityFieldAccessor(graph);
        when(graph.getEntity(eq(vmOid))).thenReturn(Optional.of(TopologyEntity.newBuilder(vmBuilder).build()));
        // Only perform applyInsufficientHistoricalDataPolicy on VMEM, vm's getCommoditiesBoughtFromProviders
        // are still scalable.
        accessor.applyInsufficientHistoricalDataPolicy(vmemRef);
        Assert.assertTrue(vmBuilder.getCommoditiesBoughtFromProvidersBuilderList().stream().allMatch(commBoughtGrp
                -> commBoughtGrp.getScalable() == true));
        // Now both VMEM and VCPU have performed applyInsufficientHistoricalDataPolicy, vm's
        // getCommoditiesBoughtFromProviders are no longer scalable.
        EntityCommodityFieldReference vcpuRef = new EntityCommodityFieldReference(vmOid, CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE).build(),
                CommodityField.USED);
        accessor.applyInsufficientHistoricalDataPolicy(vcpuRef);
        Assert.assertTrue(vmBuilder.getCommoditiesBoughtFromProvidersBuilderList().stream().noneMatch(commBoughtGrp
                -> commBoughtGrp.getScalable() == true));

    }
}
