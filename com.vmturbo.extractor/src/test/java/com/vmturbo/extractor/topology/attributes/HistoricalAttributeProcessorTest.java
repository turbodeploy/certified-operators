package com.vmturbo.extractor.topology.attributes;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.sql.Timestamp;
import java.util.function.Consumer;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.AttrType;

/**
 * Unit tests for {@link HistoricalAttributeProcessor}.
 */
public class HistoricalAttributeProcessorTest {
    private final TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
            .setOid(7L)
            .setDisplayName("foo")
            .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
            .setEntityState(EntityState.POWERED_ON)
            .build();

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setCreationTime(1_000_000)
            .build();

    /**
     * Test extracting values from a {@link TopologyEntityDTO} into a {@link Record}.
     */
    @Test
    public void testProcessorExtraction() {
        Long2LongMap map = new Long2LongOpenHashMap();
        Consumer<Long2LongMap> onSuccess = map::putAll;
        HistoricalAttributeProcessor<Integer> processor = new HistoricalAttributeProcessor<Integer>(
                HistoricalAttributes.INT_VALUE, AttrType.NUM_VCPU, map, onSuccess) {
            @Override
            Integer extractValue(TopologyEntityDTO entity) {
                return 1;
            }
        };

        assertThat(map.get(entity.getOid()), is(0L));

        Record record = processor.processEntity(entity, topologyInfo);
        assertNotNull(record);
        assertThat(record.get(HistoricalAttributes.INT_VALUE), is(1));
        assertThat(record.get(HistoricalAttributes.ENTITY_OID), is(entity.getOid()));
        assertThat(record.get(HistoricalAttributes.TYPE), is(AttrType.NUM_VCPU));
        assertThat(record.get(HistoricalAttributes.TIME), is(new Timestamp(topologyInfo.getCreationTime())));
        assertThat(record.asMap().size(), is(4));

        // Until the "onFinish" method is called we don't want the updated hashes to be recorded
        // to the checksum map.
        assertThat(map.get(entity.getOid()), is(0L));

        processor.onSuccess();
        assertThat(map.get(entity.getOid()), is(HistoricalAttributes.INT_VALUE.getColType().xxxHash(1)));
    }
}