package com.vmturbo.repository.listener;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommodityBoughtList;
import com.vmturbo.components.test.utilities.utils.TopologyUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.constant.RepoObjectType.RepoEntityType;
import com.vmturbo.repository.topology.IncrementalTopologyRelationshipRecorder;

/**
 * Unit tests for {@link IncrementalTopologyRelationshipRecorder}.
 *
 */
public class IncrementalTopologyRelationshipRecorderTest {

    private final TopologyEntityDTO datacenter = TopologyEntityDTO.newBuilder()
        .setOid(71930913569376L)
        .setEntityType(EntityType.DATACENTER.getNumber())
        .build();

    private final TopologyEntityDTO physicalMachine = TopologyEntityDTO.newBuilder()
        .setOid(1234567890L)
        .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
        .putCommodityBoughtMap(71930913569376L, CommodityBoughtList.getDefaultInstance())
        .build();

    private final List<TopologyEntityDTO> pmChunk = Collections.singletonList(physicalMachine);
    private final List<TopologyEntityDTO> datacenterChunk = Collections.singletonList(datacenter);

    @Test
    public void testDependenciesArriveInSeparateChunksInForwardOrder() {
        final IncrementalTopologyRelationshipRecorder recorder = new IncrementalTopologyRelationshipRecorder();
        recorder.processChunk(datacenterChunk);
        recorder.processChunk(pmChunk);

        final Multimap<String, String> providerRelationships = recorder.supplyChain();
        assertEquals(
            RepoEntityType.DATACENTER.getValue(),
            providerRelationships.get(RepoEntityType.PHYSICAL_MACHINE.getValue()).iterator().next()
        );
    }

    @Test
    public void testDependenciesArriveInSeparateChunksInReverseOrder() {
        final IncrementalTopologyRelationshipRecorder recorder = new IncrementalTopologyRelationshipRecorder();
        recorder.processChunk(pmChunk);
        recorder.processChunk(datacenterChunk);

        final Multimap<String, String> providerRelationships = recorder.supplyChain();
        assertEquals(
            RepoEntityType.DATACENTER.getValue(),
            providerRelationships.get(RepoEntityType.PHYSICAL_MACHINE.getValue()).iterator().next()
        );
    }

    /**
     * Test that the supply chain produced for the whole collection of DTOs in one chunk
     * is the same as the one produced when the collection is chunked.
     * @throws InterruptedException if generating the test topology is interrupted
     */
    @Test
    public void testSupplyChainChunks() throws InterruptedException {
        List<TopologyEntityDTO> topology = TopologyUtils.generateTopology(200);
        IncrementalTopologyRelationshipRecorder recorder0 = new IncrementalTopologyRelationshipRecorder();
        recorder0.processChunk(topology);
        Multimap<String, String> noChunksSupplyChain = recorder0.supplyChain();
        for (int i = 5; i < 50; i *= 1.5) {
            final List<List<TopologyEntityDTO>> chunks =
                            Lists.partition(ImmutableList.copyOf(topology), i);
            IncrementalTopologyRelationshipRecorder recorder =
                            new IncrementalTopologyRelationshipRecorder();
            chunks.stream().forEach(recorder::processChunk);
            assertEquals("Chunk size : " + i, noChunksSupplyChain, recorder.supplyChain());
        }
    }
}
