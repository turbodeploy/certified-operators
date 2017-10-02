package com.vmturbo.history.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.utils.TopologyOrganizer;

/**
 * Test the SnapshotRegistry utility class; manages async receipt of topologies and priceIndex results from M2.
 */
public class TopologySnapshotRegistryTest {

    private long topologyContextId = 1L;
    private long topologyContextId2 = 2L;
    private long topologyId = 3L;
    private long topologyId2 = 4L;

    private List<TopologyEntityDTO> getTopologyDTOs() {
        TopologyEntityDTO dto1 = TopologyEntityDTO.newBuilder()
                .setOid(456)
                .setEntityType(1)
                .build();
        TopologyEntityDTO dto2 = TopologyEntityDTO.newBuilder()
                .setOid(789)
                .setEntityType(2)
                .build();
        return Lists.newArrayList(dto1, dto2);
    }

    @Test
    public void testRegisterTopologyFirst() throws Exception {
        // Arrange
        TopologySnapshotRegistry registry = new TopologySnapshotRegistry();
        List<TopologyEntityDTO> topologyDTOs = getTopologyDTOs();
        TopologyOrganizer topologyOrganizer = new TopologyOrganizer(topologyContextId, topologyId);
        RemoteIterator<TopologyEntityDTO> allDTOs = Mockito.mock(RemoteIterator.class);
        when(allDTOs.hasNext()).thenReturn(true).thenReturn(false);
        when(allDTOs.nextChunk()).thenReturn(topologyDTOs);

        final boolean[] called = {false};
        final long[] time = {-1};

        // Act
        registry.registerTopologySnapshot(topologyContextId, topologyOrganizer);
        registry.registerPriceIndexInfo(topologyContextId, topologyId, info -> {
            called[0] = true;
            time[0] = info.getSnapshotTime();
        });

        // Assert
        assertThat(called[0], is(true));
        assertThat(time[0], is(topologyOrganizer.getSnapshotTime()));
    }

    @Test
    public void testRegisterPriceIndexOnly() throws Exception {
        // Arrange
        TopologySnapshotRegistry registry = new TopologySnapshotRegistry();
        long contextId = 123;
        final boolean[] called = {false};

        // Act
        registry.registerPriceIndexInfo(contextId, topologyId, info -> called[0] = true);

        // Assert
        assertThat(called[0], is(false));
    }


    @Test
    public void testRegisterPriceIndexFirst() throws Exception {
        // Arrange
        TopologySnapshotRegistry registry = new TopologySnapshotRegistry();
        long contextId = 123;
        TopologyOrganizer topologyOrganizer = new TopologyOrganizer(topologyContextId, topologyId);
        final boolean[] called = {false};

        // Act
        registry.registerPriceIndexInfo(contextId, topologyId, info -> called[0] = true);
        registry.registerTopologySnapshot(contextId, topologyOrganizer);

        // Assert
        assertThat(called[0], is(true));
    }

    @Test
    public void testRegisterTwoTopologies() throws Exception {
        // Arrange
        TopologySnapshotRegistry registry = new TopologySnapshotRegistry();
        TopologyOrganizer topologyOrganizer = new TopologyOrganizer(topologyContextId, topologyId);
        TopologyOrganizer topologyOrganizer1 = new TopologyOrganizer(topologyContextId2, topologyId2);
        final boolean[] called = {false, false};

        // Act
        registry.registerTopologySnapshot(topologyContextId, topologyOrganizer);
        registry.registerTopologySnapshot(topologyContextId2, topologyOrganizer1);
        registry.registerPriceIndexInfo(topologyContextId,
                topologyId, info -> called[0] = true);
        registry.registerPriceIndexInfo(topologyContextId2,
                topologyId, info -> called[1] = true);

        // Assert
        assertThat(called[0], is(true));
        assertThat(called[1], is(true));
    }

    @Test
    public void testRegisterTwoTopologiesWithSameContext() throws Exception {
        // Arrange
        TopologySnapshotRegistry registry = new TopologySnapshotRegistry();
        TopologyOrganizer topologyOrganizer1 = new TopologyOrganizer(topologyContextId, topologyId);
        TopologyOrganizer topologyOrganizer2 = new TopologyOrganizer(topologyContextId, topologyId2);
        final boolean[] called = {false, false};

        // Act
        registry.registerTopologySnapshot(topologyContextId, topologyOrganizer1);
        registry.registerTopologySnapshot(topologyContextId, topologyOrganizer2);
        registry.registerPriceIndexInfo(topologyContextId,
                topologyId, info -> called[0] = true);
        registry.registerPriceIndexInfo(topologyContextId,
                topologyId2, info -> called[1] = true);

        // Assert
        assertThat(called[0], is(true));
        assertThat(called[1], is(false));
    }

    @Test
    public void testInterleaveTwoTopologies() throws Exception {
        // Arrange
        TopologySnapshotRegistry registry = new TopologySnapshotRegistry();
        long contextId = 123;
        long contextId2 = 456;
        TopologyOrganizer snapshotInfo = new TopologyOrganizer(topologyContextId, topologyId);
        TopologyOrganizer snapshotInfo2 = new TopologyOrganizer(topologyContextId, topologyId);
        final boolean[] called = {false, false};

        // Act
        registry.registerTopologySnapshot(contextId, snapshotInfo);
        registry.registerPriceIndexInfo(contextId,
                topologyId, info -> called[0] = true);
        registry.registerPriceIndexInfo(contextId2,
                topologyId, info -> called[1] = true);

        // Assert
        assertThat(called[0], is(true));
        assertThat(called[1], is(false));

        // Act
        registry.registerTopologySnapshot(contextId2, snapshotInfo2);
        // Assert
        assertThat(called[0], is(true));
        assertThat(called[1], is(true));
    }

    /**
     * Receive an invalid topology and then priceIndexInfo; should be discarded.
     */
    @Test
    public void testInvalidTopologyFirst() throws Exception {
        // Arrange
        TopologySnapshotRegistry registry = new TopologySnapshotRegistry();
        final boolean[] called = {false};

        // Act
        registry.registerInvalidTopology(topologyContextId, topologyId);
        registry.registerPriceIndexInfo(topologyContextId, topologyId, info -> {
            called[0] = true;
        });

        // Assert
        assertThat(called[0], is(false));
    }

    /**
     * Receive a priceIndex Info and then invalid topology; priceIndex info should be discarded
     */
    @Test
    public void testInvalidTopologySecond() {
        // Arrange
        TopologySnapshotRegistry registry = new TopologySnapshotRegistry();
        TopologyOrganizer topologyOrganizer = new TopologyOrganizer(topologyContextId, topologyId);
        final boolean[] called = {false};

        // Act
        registry.registerPriceIndexInfo(topologyContextId, topologyId, info -> called[0] = true);
        registry.registerInvalidTopology(topologyContextId, topologyId);

        // Assert
        assertThat(called[0], is(false));

    }
}