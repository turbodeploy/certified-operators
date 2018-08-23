package com.vmturbo.cost.component.pricing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;

public class PriceTableMergeTest {

    private PriceTableMerge merge = PriceTableMerge.newFactory().newMerge();

    @Test
    public void testMergeEmpty() {
        assertThat(merge.merge(Collections.emptyList()), is(PriceTable.getDefaultInstance()));
    }

    @Test
    public void testMergeSinglePriceTable() {
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
                .build();
        assertThat(merge.merge(Collections.singleton(priceTable)), is(priceTable));
    }

    @Test
    public void testMergeOnDemandTables() {
        final long region1Id = 7L;
        final long region2Id = 2L;
        final OnDemandPriceTable region1PriceTable = OnDemandPriceTable.newBuilder()
            .putIpPricesById(10L, IpPriceList.getDefaultInstance())
            .build();
        final OnDemandPriceTable region2PriceTable = OnDemandPriceTable.newBuilder()
            .putComputePricesByTierId(20L, ComputeTierPriceList.getDefaultInstance())
            .build();

        final PriceTable priceTable1 = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(region1Id, region1PriceTable)
                .build();
        final PriceTable priceTable2 = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(region2Id, region2PriceTable)
                .build();
        final PriceTable mergedPriceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(region1Id, region1PriceTable)
                .putOnDemandPriceByRegionId(region2Id, region2PriceTable)
                .build();

        assertThat(merge.merge(Arrays.asList(priceTable1, priceTable2)), is(mergedPriceTable));
    }

    @Test
    public void testMergeOnDemandTablesDropDuplicateRegion() {
        final long region1Id = 7L;
        final OnDemandPriceTable region1PriceTable1 = OnDemandPriceTable.newBuilder()
                .putIpPricesById(10L, IpPriceList.getDefaultInstance())
                .build();
        final OnDemandPriceTable region1PriceTable2 = OnDemandPriceTable.newBuilder()
                .putComputePricesByTierId(20L, ComputeTierPriceList.getDefaultInstance())
                .build();

        final PriceTable priceTable1 = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(region1Id, region1PriceTable1)
                .build();
        final PriceTable priceTable2 = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(region1Id, region1PriceTable2)
                .build();
        final PriceTable mergedPriceTable = PriceTable.newBuilder()
                // Should keep the first encountered table
                .putOnDemandPriceByRegionId(region1Id, region1PriceTable1)
                .build();

        assertThat(merge.merge(Arrays.asList(priceTable1, priceTable2)), is(mergedPriceTable));
    }

    @Test
    public void testMergeSpotTables() {
        final long region1Id = 7L;
        final long region2Id = 2L;
        final SpotInstancePriceTable region1PriceTable = SpotInstancePriceTable.newBuilder()
                .putSpotPriceByInstanceId(10L, Price.getDefaultInstance())
                .build();
        final SpotInstancePriceTable region2PriceTable = SpotInstancePriceTable.newBuilder()
                .putSpotPriceByInstanceId(10L, Price.getDefaultInstance())
                .build();

        final PriceTable priceTable1 = PriceTable.newBuilder()
                .putSpotPriceByRegionId(region1Id, region1PriceTable)
                .build();
        final PriceTable priceTable2 = PriceTable.newBuilder()
                .putSpotPriceByRegionId(region2Id, region2PriceTable)
                .build();
        final PriceTable mergedPriceTable = PriceTable.newBuilder()
                .putSpotPriceByRegionId(region1Id, region1PriceTable)
                .putSpotPriceByRegionId(region2Id, region2PriceTable)
                .build();

        assertThat(merge.merge(Arrays.asList(priceTable1, priceTable2)), is(mergedPriceTable));
    }

    @Test
    public void testMergeSpotTablesDropDuplicateRegion() {
        final long region1Id = 7L;
        final SpotInstancePriceTable region1PriceTable1 = SpotInstancePriceTable.newBuilder()
                .putSpotPriceByInstanceId(10L, Price.getDefaultInstance())
                .build();
        final SpotInstancePriceTable region1PriceTable2 = SpotInstancePriceTable.newBuilder()
                .putSpotPriceByInstanceId(10L, Price.getDefaultInstance())
                .build();

        final PriceTable priceTable1 = PriceTable.newBuilder()
                .putSpotPriceByRegionId(region1Id, region1PriceTable1)
                .build();
        final PriceTable priceTable2 = PriceTable.newBuilder()
                .putSpotPriceByRegionId(region1Id, region1PriceTable2)
                .build();
        final PriceTable mergedPriceTable = PriceTable.newBuilder()
                // Should keep the first one encountered.
                .putSpotPriceByRegionId(region1Id, region1PriceTable1)
                .build();

        assertThat(merge.merge(Arrays.asList(priceTable1, priceTable2)), is(mergedPriceTable));
    }

}
