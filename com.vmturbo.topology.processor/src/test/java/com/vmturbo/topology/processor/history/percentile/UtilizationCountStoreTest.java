package com.vmturbo.topology.processor.history.percentile;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Unit tests for UtilizationCountStore.
 */
public class UtilizationCountStoreTest {
    private static final double delta = 0.001;
    private UtilizationCountStore store;
    private EntityCommodityFieldReference ref;

    /**
     * Set up the test.
     *
     * @throws HistoryCalculationException when failed
     */
    @Before
    public void setUp() throws HistoryCalculationException {
        ref =
            new EntityCommodityFieldReference(134L,
                                              CommodityType.newBuilder().setKey("efds").setType(12).build(),
                                              4857L, CommodityBoughtDTO.newBuilder(),
                                              CommodityField.USED);
        store = new UtilizationCountStore(new PercentileBuckets(null), ref);
    }

    /**
     * Test the points added to the store produce proper percentile rank.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testAddPoints() throws HistoryCalculationException {
        store.addPoints(ImmutableList.of(0.1d, 0.1d, 0.1d, 0.1d, 0.1d), 100d, 100);
        Assert.assertEquals(10, store.getPercentile(90));
        // adding for the same time should have no effect
        store.addPoints(ImmutableList.of(0.2d, 0.2d, 0.2d, 0.2d, 0.2d), 100d, 100);
        Assert.assertEquals(10, store.getPercentile(90));
        store.addPoints(ImmutableList.of(0.2d, 0.2d, 0.2d, 0.2d, 0.2d), 100d, 200);
        Assert.assertEquals(20, store.getPercentile(80));
    }

    /**
     * Test the serialization/deserialization of latest record.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testLatestRecord() throws HistoryCalculationException {
        PercentileRecord.Builder builder = PercentileRecord.newBuilder()
                        .setEntityOid(ref.getEntityOid())
                        .setCommodityType(ref.getCommodityType().getType())
                        .setKey(ref.getCommodityType().getKey())
                        .setProviderOid(ref.getProviderOid()).setCapacity(0f);
        for (int i = 0; i <= 100; ++i) {
            builder.addUtilization(20);
        }
        PercentileRecord rec1 = builder.build();
        store.setLatestCountsRecord(rec1);
        PercentileRecord rec2 = store.getLatestCountsRecord().build();
        Assert.assertEquals(rec1.getEntityOid(), rec2.getEntityOid());
        Assert.assertEquals(rec1.getCommodityType(), rec2.getCommodityType());
        Assert.assertEquals(rec1.getCapacity(), rec2.getCapacity(), delta);
        Assert.assertEquals(rec1.getUtilizationCount(), rec2.getUtilizationCount());
    }

    /**
     * Test the full record checkpoint behavior.
     *
     * @throws HistoryCalculationException when failed
     */
    @Test
    public void testCheckpoint() throws HistoryCalculationException {
        final float capacity = 100f;
        PercentileRecord.Builder oldest = PercentileRecord.newBuilder().setEntityOid(12)
                        .setCommodityType(32).setCapacity(100f);
        store.addPoints(ImmutableList.of(0.1d, 0.2d, 0.3d, 0.3d), capacity, 100);
        for (int i = 0; i <= 100; ++i) {
            oldest.addUtilization(i == 20 ? 1 : 0);
        }
        PercentileRecord.Builder full = store.checkpoint(oldest.build());
        Assert.assertNotNull(full);
        PercentileRecord record = full.build();

        Assert.assertTrue(record.hasCapacity());
        Assert.assertEquals(capacity, record.getCapacity(), delta);
        Assert.assertEquals(101, record.getUtilizationCount());
        Assert.assertEquals(0, record.getUtilization(0));
        Assert.assertEquals(1, record.getUtilization(10));
        Assert.assertEquals(0, record.getUtilization(20));
        Assert.assertEquals(2, record.getUtilization(30));
    }
}
