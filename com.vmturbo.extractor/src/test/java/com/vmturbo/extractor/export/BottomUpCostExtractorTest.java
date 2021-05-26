package com.vmturbo.extractor.export;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;
import com.vmturbo.extractor.schema.json.export.EntityCost;
import com.vmturbo.extractor.schema.json.export.EntityCost.CostByCategory;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData.BottomUpCostDataPoint;

/**
 * Test for {@link BottomUpCostExtractor}.
 */
public class BottomUpCostExtractorTest {

    private static final long vmId = 1111L;
    private BottomUpCostData bottomUpCostData = mock(BottomUpCostData.class);
    private BottomUpCostExtractor bottomUpCostExtractor = new BottomUpCostExtractor(bottomUpCostData);

    /**
     * Mock data for test.
     */
    @Before
    public void setUp() {
        BottomUpCostDataPoint point11 = new BottomUpCostDataPoint(
                CostCategory.ON_DEMAND_COMPUTE, CostSource.ON_DEMAND_RATE, 5);
        BottomUpCostDataPoint point12 = new BottomUpCostDataPoint(
                CostCategory.ON_DEMAND_COMPUTE, CostSource.RI_INVENTORY_DISCOUNT, -4);
        BottomUpCostDataPoint point13 = new BottomUpCostDataPoint(
                CostCategory.ON_DEMAND_COMPUTE, CostSource.TOTAL, 1);
        BottomUpCostDataPoint point21 = new BottomUpCostDataPoint(
                CostCategory.STORAGE, CostSource.ON_DEMAND_RATE, 2);
        BottomUpCostDataPoint point22 = new BottomUpCostDataPoint(
                CostCategory.STORAGE, CostSource.TOTAL, 2);
        BottomUpCostDataPoint point31 = new BottomUpCostDataPoint(
                CostCategory.RI_COMPUTE, CostSource.UNCLASSIFIED, 6);
        BottomUpCostDataPoint point32 = new BottomUpCostDataPoint(
                CostCategory.RI_COMPUTE, CostSource.TOTAL, 6);
        BottomUpCostDataPoint point41 = new BottomUpCostDataPoint(
                CostCategory.IP, CostSource.ON_DEMAND_RATE, 2);
        BottomUpCostDataPoint point42 = new BottomUpCostDataPoint(
                CostCategory.IP, CostSource.TOTAL, 2);
        BottomUpCostDataPoint point51 = new BottomUpCostDataPoint(
                CostCategory.TOTAL, CostSource.TOTAL, 11);
        doReturn(Arrays.asList(point11, point12, point13, point21, point22, point31, point32,
                point41, point42, point51))
                .when(bottomUpCostData).getEntityCostDataPoints(vmId);
    }

    /**
     * Test that entity cost is extracted correctly. It should contain breakdown by category and
     * further breakdown by source for each category.
     */
    @Test
    public void testGetEntityCost() {
        EntityCost cost = bottomUpCostExtractor.getCost(vmId);

        assertThat(cost.getTotal(), is(11f));
        assertThat(cost.getUnit(), is(StringConstants.DOLLARS_PER_HOUR));

        Map<String, CostByCategory> byCategory = cost.getCategory();
        assertThat(byCategory.size(), is(4));

        CostByCategory onDemandCompute = byCategory.get(CostCategory.ON_DEMAND_COMPUTE.getLiteral());
        assertThat(onDemandCompute.getTotal(), is(1f));
        assertThat(onDemandCompute.getSource().size(), is(2));
        assertThat(onDemandCompute.getSource().get(CostSource.ON_DEMAND_RATE.getLiteral()), is(5f));
        assertThat(onDemandCompute.getSource().get(CostSource.RI_INVENTORY_DISCOUNT.getLiteral()), is(-4f));

        CostByCategory storage = byCategory.get(CostCategory.STORAGE.getLiteral());
        assertThat(storage.getTotal(), is(2f));
        assertThat(storage.getSource().get(CostSource.ON_DEMAND_RATE.getLiteral()), is(2f));

        CostByCategory ri = byCategory.get(CostCategory.RI_COMPUTE.getLiteral());
        assertThat(ri.getTotal(), is(6f));
        assertThat(ri.getSource().get(CostSource.UNCLASSIFIED.getLiteral()), is(6f));

        CostByCategory ip = byCategory.get(CostCategory.IP.getLiteral());
        assertThat(ip.getTotal(), is(2f));
        assertThat(ip.getSource().get(CostSource.ON_DEMAND_RATE.getLiteral()), is(2f));
    }
}