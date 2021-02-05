package com.vmturbo.extractor.topology.attributes;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.models.Table;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Unit tests for {@link EntityStateAttributeProcessorFactory}.
 */
public class EntityStateAttributeProcessorFactoryTest {

    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private final long forceUpdateIntervalMs = 10;

    private final DbEndpoint endpoint = mock(DbEndpoint.class);

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1)
            .setCreationTime(clock.millis())
            .build();

    /**
     * Test processing an entity and getting an entity state {@link Record}.
     */
    @Test
    public void testEntityState() {
        Map<EntityState, Integer> enumOidMap = ImmutableMap.of(EntityState.POWERED_ON, 1, EntityState.POWERED_OFF, 2);
        EntityStateAttributeProcessorFactory f = new EntityStateAttributeProcessorFactory(endpoint -> enumOidMap, clock, forceUpdateIntervalMs, TimeUnit.MILLISECONDS);
        HistoricalAttributeProcessor<Integer> processor = f.newProcessor(endpoint);
        Table.Record record = processor.processEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setOid(1)
                .setDisplayName("foo")
                .setEntityState(TopologyDTO.EntityState.POWERED_ON)
                .build(), topologyInfo);
        assertNotNull(record);
        assertThat(record.get(HistoricalAttributes.INT_VALUE), is(1));
    }

    /**
     * Test processing an entity without a valid entity state.
     */
    @Test
    public void testEntityStateNoMapping() {
        EntityStateAttributeProcessorFactory f = new EntityStateAttributeProcessorFactory(
                endpoint -> Collections.emptyMap(), clock, forceUpdateIntervalMs, TimeUnit.MILLISECONDS);
        HistoricalAttributeProcessor<Integer> processor = f.newProcessor(endpoint);
        Table.Record record = processor.processEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setOid(1)
                .setDisplayName("foo")
                .setEntityState(TopologyDTO.EntityState.POWERED_ON)
                .build(), topologyInfo);
        assertNull(record);
    }
}