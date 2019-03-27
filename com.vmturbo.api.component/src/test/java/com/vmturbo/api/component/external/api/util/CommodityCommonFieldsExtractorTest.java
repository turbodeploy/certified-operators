package com.vmturbo.api.component.external.api.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

public class CommodityCommonFieldsExtractorTest {

    private static final double delta = 10e-7;

    private static final CommodityType COMMODITY_TYPE = CommodityType.newBuilder()
        .setKey("key").setType(1).build();
    private static final String DISPLAY_NAME = "commodity display name";
    private static final double USED = 66.6d;
    private static final double PEAK = 233.3d;
    private static final double CAPACITY = 888.8d;
    private static final List<String> AGGREGATES = ImmutableList.of("key1", "key2");

    @Test
    public void testCommoditySold() {
        final CommoditySoldDTO commoditySold = CommoditySoldDTO.newBuilder()
            .setCommodityType(COMMODITY_TYPE)
            .setUsed(USED)
            .setPeak(PEAK)
            .setCapacity(CAPACITY)
            .setDisplayName(DISPLAY_NAME)
            .addAllAggregates(AGGREGATES)
            .build();
        assertTrue(CommodityCommonFieldsExtractor.isCommoditySold(commoditySold));
        assertEquals(COMMODITY_TYPE, CommodityCommonFieldsExtractor.getCommodityType(commoditySold));
        assertEquals(DISPLAY_NAME, CommodityCommonFieldsExtractor.getDisplayName(commoditySold));
        assertEquals(USED, CommodityCommonFieldsExtractor.getUsed(commoditySold), delta);
        assertEquals(PEAK, CommodityCommonFieldsExtractor.getPeak(commoditySold), delta);
        assertEquals(CAPACITY, CommodityCommonFieldsExtractor.getCapacity(commoditySold), delta);
        assertEquals(AGGREGATES, CommodityCommonFieldsExtractor.getAggregates(commoditySold));
    }

    @Test
    public void testCommodityboughts() {
        final CommodityBoughtDTO commodityBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(COMMODITY_TYPE)
            .setUsed(USED)
            .setPeak(PEAK)
            .setDisplayName(DISPLAY_NAME)
            .addAllAggregates(AGGREGATES)
            .build();
        assertTrue(CommodityCommonFieldsExtractor.isCommodityBought(commodityBought));
        assertEquals(COMMODITY_TYPE, CommodityCommonFieldsExtractor.getCommodityType(commodityBought));
        assertEquals(DISPLAY_NAME, CommodityCommonFieldsExtractor.getDisplayName(commodityBought));
        assertEquals(USED, CommodityCommonFieldsExtractor.getUsed(commodityBought), delta);
        assertEquals(PEAK, CommodityCommonFieldsExtractor.getPeak(commodityBought), delta);
        assertEquals(AGGREGATES, CommodityCommonFieldsExtractor.getAggregates(commodityBought));
    }

    @Test
    public void testInvalidCommodity() {
        final TopologyInfo invalid = TopologyInfo.getDefaultInstance();
        assertFalse(CommodityCommonFieldsExtractor.isCommoditySold(invalid));
        assertFalse(CommodityCommonFieldsExtractor.isCommodityBought(invalid));
        assertEquals(CommodityType.getDefaultInstance(),
            CommodityCommonFieldsExtractor.getCommodityType(invalid));
        assertNull(CommodityCommonFieldsExtractor.getDisplayName(invalid));
        assertEquals(0d, CommodityCommonFieldsExtractor.getUsed(invalid), delta);
        assertEquals(0d, CommodityCommonFieldsExtractor.getPeak(invalid), delta);
        assertEquals(0d, CommodityCommonFieldsExtractor.getCapacity(invalid), delta);
        assertEquals(Collections.emptyList(), CommodityCommonFieldsExtractor.getAggregates(invalid));
    }
}
