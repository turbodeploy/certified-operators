package com.vmturbo.topology.processor.topology.pipeline;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Tests for {@link com.vmturbo.topology.processor.topology.pipeline.Stages.EntityCounter}
 */
public class EntityCounterTest {

    private final Stages.EntityCounter counter = new Stages.EntityCounter();

    @Test
    public void testCumulativeSizeEmpty() {
        assertEquals(0, counter.getEntityCount());
        assertEquals(0, counter.getCumulativeSizeBytes());
    }

    @Test
    public void testCumulativeSizeSingle() {
        counter.count(TopologyEntityDTO.getDefaultInstance());
        assertEquals(1, counter.getEntityCount());
        assertEquals(TopologyEntityDTO.getDefaultInstance().getSerializedSize(),
            counter.getCumulativeSizeBytes());
    }

    @Test
    public void testCumulativeSizeMultiple() {
        final TopologyEntityDTO first = TopologyEntityDTO.getDefaultInstance();
        final TopologyEntityDTO second = TopologyEntityDTO.newBuilder()
            .setEntityType(1)
            .setOid(2L)
            .setDisplayName("foo")
            .build();
        final TopologyEntityDTO third = TopologyEntityDTO.newBuilder()
            .setEntityType(55)
            .setOid(4L)
            .setDisplayName("bar")
            .build();

        counter.count(first);
        counter.count(second);
        counter.count(third);

        assertEquals(3, counter.getEntityCount());
        assertEquals(first.getSerializedSize() + second.getSerializedSize() + third.getSerializedSize(),
            counter.getCumulativeSizeBytes());
    }
}
