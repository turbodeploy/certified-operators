package com.vmturbo.common.protobuf.memory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Collections;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;

import org.junit.Test;

import com.vmturbo.common.protobuf.memory.Duplicates.DuplicateSummary;
import com.vmturbo.common.protobuf.memory.Duplicates.MemUsageCount;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test for Duplicates utilities.
 */
public class DuplicatesTest {

    private final CommodityBoughtDTO.Builder commodityBought = CommodityBoughtDTO.newBuilder()
        .setCommodityType(CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.BALLOONING_VALUE)
            .setKey("foo"))
        .setUsed(1.48924)
        .setDisplayName("display_name")
        .setScalingFactor(3.0)
        .setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(2.0))
        .setHistoricalPeak(HistoricalValues.newBuilder().setPercentile(3.0));

    private final CommoditySoldDTO.Builder commoditySold = CommoditySoldDTO.newBuilder()
        .setCommodityType(CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.BALLOONING_VALUE)
            .setKey("foo"))
        .setUsed(1.80238924)
        .setDisplayName("display_name")
        .setScalingFactor(3.33)
        .setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(2.5))
        .setHistoricalPeak(HistoricalValues.newBuilder().setPercentile(3.5))
        .setHotResizeInfo(HotResizeInfo.newBuilder().setHotAddSupported(true).setHotRemoveSupported(false));

    /**
     * Test when no duplicates are present.
     */
    @Test
    public void testNoDuplicates() {
        final DuplicateSummary<AbstractMessage> summary =
            Duplicates.detectDuplicateProtobufs(Collections.singleton(commoditySold.build()));

        final MemUsageCount usageCount = summary.getSummary().values().iterator().next();
        assertEquals(1, usageCount.getInstanceCount());
        assertEquals(0, usageCount.getDuplicateCount());
    }

    /**
     * Test when there are duplicates present.
     */
    @Test
    public void testDuplicate() {
        final DuplicateSummary<AbstractMessage> summary =
            Duplicates.detectDuplicateProtobufs(Arrays.asList(
                commoditySold.build(),
                commoditySold.build()));

        final MemUsageCount usageCount = summary
            .usageBy(m -> m instanceof CommoditySoldDTO)
            .findAny()
            .get();
        assertEquals(2, usageCount.getInstanceCount());
        assertEquals(1, usageCount.getDuplicateCount());
    }

    /**
     * Test duplication when the top-level objects are not duplicated but their
     * inner objects are.
     */
    @Test
    public void testInnerDuplication() {
        final TopologyEntityDTO e1 = entityBuilder(1)
            .addCommoditiesBoughtFromProviders(
                fromProvider(commodityBought.build()))
            .build();
        final TopologyEntityDTO e2 = entityBuilder(2)
            .addCommoditiesBoughtFromProviders(
                fromProvider(commodityBought.build()))
            .build();
        final DuplicateSummary<AbstractMessage> summary =
            Duplicates.detectDuplicateProtobufs(Arrays.asList(e1, e2));

        final MemUsageCount e1Usage = summary
            .usageBy(m -> m == e1)
            .findAny()
            .get();
        final MemUsageCount e2Usage = summary
            .usageBy(m -> m == e2)
            .findAny()
            .get();

        // The entities themselves should not be duplicated
        assertFalse(e1Usage == e2Usage);
        assertEquals(1, e1Usage.getInstanceCount());
        assertEquals(0, e1Usage.getDuplicateCount());
        assertEquals(1, e2Usage.getInstanceCount());
        assertEquals(0, e2Usage.getDuplicateCount());

        // But the inner commodities bought should be duplicated
        final MemUsageCount commUsage = summary
            .usageBy(m -> m instanceof CommodityBoughtDTO)
            .findAny()
            .get();
        assertEquals(2, commUsage.getInstanceCount());
        assertEquals(1, commUsage.getDuplicateCount());
    }

    /**
     * Test reporting on duplication.
     */
    @Test
    public void testReportSummary() {
        final TopologyEntityDTO e1 = entityBuilder(1)
            .addCommoditiesBoughtFromProviders(
                fromProvider(commodityBought.build()))
            .build();
        final TopologyEntityDTO e2 = entityBuilder(2)
            .addCommoditiesBoughtFromProviders(
                fromProvider(commodityBought.build()))
            .build();
        final DuplicateSummary<AbstractMessage> summary =
            Duplicates.detectDuplicateProtobufs(Arrays.asList(e1, e2));

        final String report = summary.toString();
        assertThat(report, containsString("DEEP     SAVING   %DEEP    SHALLOW  %SHALW       COUNT  %COUNT TYPE"));
        assertThat(report, containsString("TopologyEntityDTO"));
        assertThat(report, containsString("CommodityType"));
        assertThat(report, containsString("HistoricalValues"));
    }

    private CommoditiesBoughtFromProvider fromProvider(@Nonnull final CommodityBoughtDTO bought) {
        return CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(2345L)
            .addCommodityBought(bought)
            .build();
    }

    private TopologyEntityDTO.Builder entityBuilder(long oid) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(oid);
    }
}