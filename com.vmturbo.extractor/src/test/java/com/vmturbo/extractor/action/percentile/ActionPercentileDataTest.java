package com.vmturbo.extractor.action.percentile;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.extractor.schema.json.common.CommodityPercentileChange;

/**
 * Unit tests for {@link ActionPercentileData}.
 */
public class ActionPercentileDataTest {

    /**
     * Test that the percentile data structure lookup method works.
     */
    @Test
    public void testGetChange() {
        final long entityId = 1L;
        final CommodityType commType = CommodityType.newBuilder()
                .setType(1)
                .build();
        final CommodityPercentileChange percentileChange = new CommodityPercentileChange();
        percentileChange.setObservationPeriodDays(1);
        final Long2ObjectMap<Map<CommodityType, CommodityPercentileChange>> changesMap = new Long2ObjectOpenHashMap<>();
        changesMap.computeIfAbsent(entityId, k -> new HashMap<>()).put(commType, percentileChange);
        final ActionPercentileData data = new ActionPercentileData(changesMap);

        assertThat(data.getChange(entityId, commType), is(percentileChange));
        assertNull(data.getChange(entityId + 1, commType));
        assertNull(data.getChange(entityId, commType.toBuilder()
            .setKey("foo")
            .build()));
    }

}