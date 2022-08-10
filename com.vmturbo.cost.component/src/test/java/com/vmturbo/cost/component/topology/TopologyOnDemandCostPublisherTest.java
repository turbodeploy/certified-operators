package com.vmturbo.cost.component.topology;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk.Data;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk.End;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.notification.TopologyCostSender;
import com.vmturbo.cost.component.topology.cloud.listener.TopologyOnDemandCostPublisher;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbException;

/**
 * Unit tests for the TopologyOnDemandCostPublisher.
 */
public class TopologyOnDemandCostPublisherTest {

    private static final long TOPOLOGY_OID = 789L;
    private static final TopologyInfo TOPOLOGY_INFO =
            TopologyInfo.newBuilder().setTopologyId(TOPOLOGY_OID).build();

    private static final TopologyOnDemandCostChunk START_CHUNK = TopologyOnDemandCostChunk.newBuilder()
            .setStart(Start.newBuilder().setTopologyInfo(TOPOLOGY_INFO))
            .setTopologyOid(TOPOLOGY_OID)
            .build();
    private static final TopologyOnDemandCostChunk END_CHUNK = TopologyOnDemandCostChunk.newBuilder()
            .setEnd(End.getDefaultInstance())
            .setTopologyOid(TOPOLOGY_OID)
            .build();

    private static final long ENTITY_ID_1 = 123L;
    private static final TopologyEntityDTO ENTITY_1 = TopologyEntityDTO.newBuilder()
            .setOid(ENTITY_ID_1).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build();
    private static final EntityCost ENTITY_COST_1 = EntityCost.getDefaultInstance();
    private static final TopologyOnDemandCostChunk DATA_CHUNK_1 = TopologyOnDemandCostChunk.newBuilder()
            .setTopologyOid(TOPOLOGY_OID)
            .setData(Data.newBuilder().addEntityCosts(ENTITY_COST_1))
            .build();

    private CloudTopology<TopologyEntityDTO> topology;
    private EntityCostStore store;
    private TopologyCostSender sender;

    /**
     * Set up for tests.
     */
    @Before
    public void setup() {
        topology = mock(CloudTopology.class);
        store = mock(EntityCostStore.class);
        sender = mock(TopologyCostSender.class);
    }

    /**
     * Test that if no costs are available, nothing is sent.
     */
    @Test
    public void testNoMsgSentWhenNoCostsAvailable() {
        when(topology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE))
                .thenReturn(Collections.singletonList(ENTITY_1));
        final TopologyOnDemandCostPublisher publisher =
                new TopologyOnDemandCostPublisher(store, 1L, 2, sender);
        publisher.process(topology, TOPOLOGY_INFO);
        verifyZeroInteractions(sender);
    }

    /**
     * Test that start, data, and end chunks are sent correctly.
     *
     * @throws DbException never
     * @throws CommunicationException never
     * @throws InterruptedException never
     */
    @Test
    public void testStartDataEndMsgsSent() throws DbException, CommunicationException, InterruptedException {
        when(topology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE))
                .thenReturn(Collections.singletonList(ENTITY_1));
        final Map<Long, Map<Long, EntityCost>> costResult =
                ImmutableMap.of(1L, ImmutableMap.of(ENTITY_ID_1, ENTITY_COST_1));
        when(store.getEntityCosts(any())).thenReturn(costResult);
        final TopologyOnDemandCostPublisher publisher =
                new TopologyOnDemandCostPublisher(store, 1L, 2, sender);
        publisher.process(topology, TOPOLOGY_INFO);
        final ArgumentCaptor<TopologyOnDemandCostChunk> chunkArgumentCaptor =
                ArgumentCaptor.forClass(TopologyOnDemandCostChunk.class);
        verify(sender, times(3)).sendTopologyCostChunk(chunkArgumentCaptor.capture());
        final List<TopologyOnDemandCostChunk> sentChunks = chunkArgumentCaptor.getAllValues();
        assertTrue(sentChunks.contains(START_CHUNK));
        assertTrue(sentChunks.contains(DATA_CHUNK_1));
        assertTrue(sentChunks.contains(END_CHUNK));
    }

    /**
     * Test that data is subdivided into separate chunks if number of VMs is greater than maximum chunk size.
     *
     * @throws DbException never
     * @throws CommunicationException never
     * @throws InterruptedException never
     */
    @Test
    public void testDataChunkedCorrectly() throws DbException, CommunicationException, InterruptedException {
        final long entityId2 = 234L;
        final TopologyEntityDTO entity2 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setOid(entityId2).build();
        when(topology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE))
                .thenReturn(Arrays.asList(ENTITY_1, entity2));
        final EntityCost entityCost2 = EntityCost.getDefaultInstance();
        final Map<Long, Map<Long, EntityCost>> costResult = ImmutableMap.of(1L,
                ImmutableMap.of(ENTITY_ID_1, ENTITY_COST_1, entityId2, entityCost2));
        when(store.getEntityCosts(any())).thenReturn(costResult);
        final TopologyOnDemandCostPublisher publisher =
                new TopologyOnDemandCostPublisher(store, 1L, 1, sender);
        publisher.process(topology, TOPOLOGY_INFO);
        final ArgumentCaptor<TopologyOnDemandCostChunk> chunkArgumentCaptor =
                ArgumentCaptor.forClass(TopologyOnDemandCostChunk.class);
        verify(sender, times(4)).sendTopologyCostChunk(chunkArgumentCaptor.capture());
        final List<TopologyOnDemandCostChunk> sentChunks = chunkArgumentCaptor.getAllValues();
        final TopologyOnDemandCostChunk dataChunk2 = TopologyOnDemandCostChunk.newBuilder()
                .setTopologyOid(TOPOLOGY_OID)
                .setData(Data.newBuilder().addEntityCosts(entityCost2))
                .build();
        assertTrue(sentChunks.contains(START_CHUNK));
        assertTrue(sentChunks.contains(DATA_CHUNK_1));
        assertTrue(sentChunks.contains(dataChunk2));
        assertTrue(sentChunks.contains(END_CHUNK));
    }
}
