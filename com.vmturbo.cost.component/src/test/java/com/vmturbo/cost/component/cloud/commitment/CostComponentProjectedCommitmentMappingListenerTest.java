package com.vmturbo.cost.component.cloud.commitment;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeoutException;

import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;

public class CostComponentProjectedCommitmentMappingListenerTest {
    private final long topologyId = 111L;
    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();
    private final RemoteIterator<CloudCommitmentMapping> remoteIterator = mock(RemoteIterator.class);

    /**
     * Test when cloud commitment mappings received, mappingsAvailable is being called.
     */
    @Test
    public void testMappingsAvailableBeingCalled()
            throws CommunicationException, InterruptedException, TimeoutException {
        ProjectedCommitmentMappingProcessor projectedCommitmentMappingProcessor = mock(ProjectedCommitmentMappingProcessor.class);
        when(remoteIterator.hasNext()).thenReturn(true, false);
        CloudCommitmentMapping commitmentMapping1 = CloudCommitmentMapping.newBuilder().build();
        CloudCommitmentMapping commitmentMapping2 = CloudCommitmentMapping.newBuilder().build();
        when(remoteIterator.nextChunk())
                .thenReturn(Sets.newHashSet(commitmentMapping1, commitmentMapping2));

        CostComponentProjectedCommitmentMappingListener costComponentProjectedCommitmentMappingListener =
                new CostComponentProjectedCommitmentMappingListener(projectedCommitmentMappingProcessor);
        costComponentProjectedCommitmentMappingListener.onProjectedCommitmentMappingReceived(topologyId, topologyInfo, remoteIterator);

        verify(projectedCommitmentMappingProcessor).mappingsAvailable(eq(topologyId), eq(topologyInfo), any());
    }
}