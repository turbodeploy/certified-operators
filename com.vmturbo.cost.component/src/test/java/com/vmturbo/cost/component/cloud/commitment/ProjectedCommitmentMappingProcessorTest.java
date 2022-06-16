package com.vmturbo.cost.component.cloud.commitment;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.Pair;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.repository.api.RepositoryClient;

public class ProjectedCommitmentMappingProcessorTest {

    private final long topologyId = 111L;
    private final long topologyContextId = 222L;
    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();

    private final HashMap<Long, Long> projectedTopologyIDToContextId = new HashMap<>();
    private HashMap<Long, Pair<TopologyInfo, List<CloudCommitmentMapping>>> projectedCloudCommitmentMapping = new HashMap<>();

    private TopologyCommitmentCoverageWriter.Factory commitmentCoverageWriterFactory = mock(TopologyCommitmentCoverageWriter.Factory.class);
    private final RepositoryClient repositoryClient = mock(RepositoryClient.class);
    private TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
    private ProjectedCommitmentMappingProcessor projectedCommitmentMappingProcessor;
    private final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
    private final TopologyCommitmentCoverageWriter topologyCommitmentCoverageWriter = mock(TopologyCommitmentCoverageWriter.class);

    private final Stream<TopologyEntityDTO> topologyEntities = ImmutableList.of(
            TopologyEntityDTO.newBuilder()
                    .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOid(333L)
                    .build()).stream();

    private CloudCommitmentMapping commitmentMapping1 = CloudCommitmentMapping.newBuilder().build();
    private CloudCommitmentMapping commitmentMapping2 = CloudCommitmentMapping.newBuilder().build();
    private List<CloudCommitmentMapping> cloudCommitmentMappingList = Arrays.asList(commitmentMapping1, commitmentMapping2);

    @Before
    public void setup() {
        projectedTopologyIDToContextId.put(topologyId, topologyContextId);
        projectedCloudCommitmentMapping.put(topologyId, new Pair<>(topologyInfo, cloudCommitmentMappingList));
        projectedCommitmentMappingProcessor = new ProjectedCommitmentMappingProcessor(projectedTopologyIDToContextId,
                projectedCloudCommitmentMapping,
                commitmentCoverageWriterFactory,
                repositoryClient,
                cloudTopologyFactory,
                true);
        when(repositoryClient.retrieveTopologyEntities(anyList(), anyLong())).thenReturn(topologyEntities);
        when(cloudTopologyFactory.newCloudTopology(topologyEntities)).thenReturn(cloudTopology);
        when(commitmentCoverageWriterFactory.newWriter(cloudTopology)).thenReturn(topologyCommitmentCoverageWriter);
    }

    /**
     * Test when mappings received and topology is available, persistCommitmentAllocations should get called.
     */
    @Test
    public void testMappingAvailablePersistCommitmentAllocationsGetsCalled() {
        projectedCommitmentMappingProcessor.mappingsAvailable(topologyId, topologyInfo, cloudCommitmentMappingList);

        verify(topologyCommitmentCoverageWriter).persistCommitmentAllocations(any(), any());
    }

    /**
     * Test when topology received and mappings is available, persistCommitmentAllocations should get called.
     */
    @Test
    public void testProjectedAvailablePersistCommitmentAllocationsGetsCalled() {
        final Stream<TopologyEntityDTO> topologyEntities = ImmutableList.of(
                TopologyEntityDTO.newBuilder()
                        .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOid(333L)
                        .build()).stream();
        projectedCommitmentMappingProcessor.projectedAvailable(topologyId, topologyContextId, cloudTopology);

        verify(topologyCommitmentCoverageWriter).persistCommitmentAllocations(any(), any());
        projectedCommitmentMappingProcessor.projectedTopologyIDToContextId.put(topologyId, topologyContextId);
    }

    /**
     * Test when mappings received but topology is not available, save the mappings.
     */
    @Test
    public void testMappingsSaved() {
        final long newTopologyId = 222L;
        assertFalse(projectedCommitmentMappingProcessor.projectedMapping.containsKey(newTopologyId));
        projectedCommitmentMappingProcessor.mappingsAvailable(newTopologyId, topologyInfo, cloudCommitmentMappingList);
        assertTrue(projectedCommitmentMappingProcessor.projectedMapping.containsKey(newTopologyId));
        projectedCommitmentMappingProcessor.projectedMapping.remove(newTopologyId);
    }

    /**
     * Test when topology received but mappings is not available, save the topology.
     */
    @Test
    public void testTopologySaved() {
        final long newTopologyId = 222L;
        assertFalse(projectedCommitmentMappingProcessor.projectedTopologyIDToContextId.containsKey(newTopologyId));
        projectedCommitmentMappingProcessor.projectedAvailable(newTopologyId, topologyContextId, cloudTopology);
        assertTrue(projectedCommitmentMappingProcessor.projectedTopologyIDToContextId.containsKey(newTopologyId));
        projectedCommitmentMappingProcessor.projectedMapping.remove(newTopologyId);
    }
}