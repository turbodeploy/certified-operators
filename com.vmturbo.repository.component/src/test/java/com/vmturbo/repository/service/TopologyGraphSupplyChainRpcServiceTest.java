package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.grpc.Status.Code;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainStatsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainStatsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainGroupBy;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.service.SupplyChainMerger.MergedSupplyChainException;
import com.vmturbo.repository.service.SupplyChainStatistician.TopologyEntityLookup;
import com.vmturbo.repository.service.TopologyGraphSupplyChainRpcService.RealtimeSupplyChainResolver;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for {@link TopologyGraphSupplyChainRpcService}.
 */
public class TopologyGraphSupplyChainRpcServiceTest {

    private static final long REALTIME_CONTEXT_ID = 7777;

    private ArangoSupplyChainRpcService arangoSupplyChainService = mock(ArangoSupplyChainRpcService.class);

    private LiveTopologyStore liveTopologyStore = mock(LiveTopologyStore.class);

    private SupplyChainStatistician supplyChainStatistician = mock(SupplyChainStatistician.class);

    private RealtimeSupplyChainResolver mockRealtimeSupplyChainResolver = mock(RealtimeSupplyChainResolver.class);

    /**
     * gRPC test server. We use this instead of a direct {@link TopologyGraphSupplyChainRpcService}
     * instance so that we can make sure gRPC calls to the service will actually work.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(
        new TopologyGraphSupplyChainRpcService(mockRealtimeSupplyChainResolver,
            liveTopologyStore, arangoSupplyChainService, supplyChainStatistician,
            REALTIME_CONTEXT_ID));

    /**
     * Rule to help apply matchers to expected exceptions.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private SupplyChainServiceBlockingStub client;

    /**
     * Common setup before all tests.
     */
    @Before
    public void setup() {
        client = SupplyChainServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
    }

    /**
     * Test getting supply chain stats via the {@link SupplyChainStatistician}.
     *
     * @throws MergedSupplyChainException To satisfy compiler.
     */
    @Test
    public void testSupplyChainStats() throws MergedSupplyChainException {
        // ARRANGE
        final SupplyChain supplyChain = SupplyChain.newBuilder()
            .addSupplyChainNodes(SupplyChainNode.newBuilder()
                .setEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .build();
        final SourceRealtimeTopology topology = mock(SourceRealtimeTopology.class);
        final TopologyGraph<RepoGraphEntity> entityGraph = mock(TopologyGraph.class);
        when(topology.entityGraph()).thenReturn(entityGraph);
        final RepoGraphEntity mockEntity = mock(RepoGraphEntity.class);
        final long mockEntityId = 1;
        when(entityGraph.getEntity(mockEntityId)).thenReturn(Optional.of(mockEntity));

        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.of(topology));
        when(mockRealtimeSupplyChainResolver.getRealtimeSupplyChain(any(), anyBoolean(), any()))
            .thenReturn(supplyChain);
        final List<SupplyChainStat> expectedStats = Collections.singletonList(
            SupplyChainStat.newBuilder()
                .setNumEntities(1)
                .build());

        when(supplyChainStatistician.calculateStats(any(), any(), any()))
            .thenReturn(expectedStats);

        final SupplyChainScope scope = SupplyChainScope.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

        // ACT
        final GetSupplyChainStatsResponse response = client.getSupplyChainStats(
            GetSupplyChainStatsRequest.newBuilder()
                .setScope(scope)
                .addGroupBy(SupplyChainGroupBy.ENTITY_TYPE)
                .build());

        // ASSERT
        verify(liveTopologyStore).getSourceTopology();
        verify(mockRealtimeSupplyChainResolver).getRealtimeSupplyChain(eq(scope),
            eq(true),
            eq(Optional.of(topology)));

        final ArgumentCaptor<TopologyEntityLookup> lookupCaptor = ArgumentCaptor.forClass(TopologyEntityLookup.class);
        verify(supplyChainStatistician).calculateStats(eq(supplyChain),
            eq(Collections.singletonList(SupplyChainGroupBy.ENTITY_TYPE)),
            lookupCaptor.capture());

        // Verify we passed the correct lookup function - the one that will look up entities
        // in the same topology that the supply chain was resolved from.
        final TopologyEntityLookup lookup = lookupCaptor.getValue();
        assertThat(lookup.getEntity(mockEntityId), is(Optional.of(mockEntity)));

        // Verify the final response.
        assertThat(response.getStatsList(), is(expectedStats));
    }

    /**
     * Test that exceptions thrown resolving the supply chain when getting supply chain stats
     * get handled properly.
     *
     * @throws MergedSupplyChainException To satisfy compiler.
     */
    @Test
    public void testSupplyChainStatsException() throws MergedSupplyChainException {
        final SourceRealtimeTopology topology = mock(SourceRealtimeTopology.class);
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.of(topology));
        final String errorMsg = "MY ERROR";
        when(mockRealtimeSupplyChainResolver.getRealtimeSupplyChain(any(), anyBoolean(), any()))
            .thenThrow(new MergedSupplyChainException(Collections.singletonList(errorMsg)));

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL)
                .descriptionContains(errorMsg));
        client.getSupplyChainStats(GetSupplyChainStatsRequest.getDefaultInstance());
    }

    /**
     * Test getting supply chain stats when there is no realtime topology in the repository.
     */
    @Test
    public void testSupplyChainStatsNoTopology() {
        when(liveTopologyStore.getSourceTopology()).thenReturn(Optional.empty());
        final GetSupplyChainStatsResponse resp = client.getSupplyChainStats(
            GetSupplyChainStatsRequest.getDefaultInstance());
        assertThat(resp, is(GetSupplyChainStatsResponse.getDefaultInstance()));
    }

}