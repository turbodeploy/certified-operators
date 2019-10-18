package com.vmturbo.topology.processor.history.percentile;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.UtilizationData;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.history.CommodityField;
import com.vmturbo.topology.processor.history.CommodityFieldAccessor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.percentile.PercentileDto.PercentileCounts.PercentileRecord;

/**
 * Unit tests for PercentileCommodityData.
 */
public class PercentileCommodityDataTest extends BaseGraphRelatedTest {
    private static final double DELTA = 0.001;
    private static final CommodityType commType = CommodityType.newBuilder().setType(1).build();
    private static final EntityCommodityFieldReference field =
                     new EntityCommodityFieldReference(1, commType, CommodityField.USED);
    private static final PercentileHistoricalEditorConfig config =
                     new PercentileHistoricalEditorConfig(1,
                                                          24,
                                                          10,
                                                          100,
                                                          Collections.emptyMap());

    /**
     * Test that init method stores passed db values.
     *
     * @throws HistoryCalculationException when initialization fails
     */
    @Test
    public void testInit() throws HistoryCalculationException {
        float cap = 100f;
        int used = 70;
        TopologyEntity entity = mockEntity(1, 1, commType, cap, used, null, null, null, null);
        ICommodityFieldAccessor accessor = Mockito
                        .spy(new CommodityFieldAccessor(mockGraph(Collections.singleton(entity))));
        Mockito.doReturn((double)cap).when(accessor).getCapacity(field);
        PercentileRecord.Builder dbValue = PercentileRecord.newBuilder().setCapacity(cap)
                        .setEntityOid(entity.getOid()).setCommodityType(commType.getType()).setPeriod(30);
        for (int i = 0; i <= 100; ++i) {
            dbValue.addUtilization(i == used ? 1 : 0);
        }

        PercentileCommodityData pcd = new PercentileCommodityData();
        pcd.init(field, dbValue.build(), config, accessor);

        Assert.assertEquals(0, pcd.getUtilizationCountStore().getPercentile(0));
        Assert.assertEquals(used, pcd.getUtilizationCountStore().getPercentile(100));
        PercentileRecord record = pcd.getUtilizationCountStore().getLatestCountsRecord().build();
        Assert.assertEquals(101, record.getUtilizationCount());
        Assert.assertEquals(1, record.getUtilization(used));
    }

    /**
     * Test that aggregate method accounts for passed running values from DTOs.
     */
    @Test
    public void testAggregate() {
        float cap = 100f;
        double used1 = 76d;
        double used2 = 99d;
        double used3 = 12d;
        double realTime = 80d;
        TopologyEntity entity = mockEntity(1, 1, commType, cap, used1, null, null, null, null);
        ICommodityFieldAccessor accessor = Mockito
                        .spy(new CommodityFieldAccessor(mockGraph(Collections.singleton(entity))));
        Mockito.doReturn((double)cap).when(accessor).getCapacity(field);
        Mockito.doReturn(realTime).when(accessor).getRealTimeValue(field);
        UtilizationData data = UtilizationData.newBuilder().setLastPointTimestampMs(1000)
                        .setIntervalMs(10).addPoint(used2 / cap * 100).addPoint(used3 / cap * 100)
                        .build();
        Mockito.doReturn(data).when(accessor).getUtilizationData(Mockito.any());

        PercentileCommodityData pcd = new PercentileCommodityData();
        pcd.init(field, null, config, accessor);
        pcd.aggregate(field, config, accessor);
        CommoditySoldDTO.Builder commSold = entity.getTopologyEntityDtoBuilder()
                        .getCommoditySoldListBuilderList().get(0);
        Assert.assertTrue(commSold.hasHistoricalUsed());
        Assert.assertEquals(used3 / cap, commSold.getHistoricalUsed().getPercentile(), DELTA);
    }
}
