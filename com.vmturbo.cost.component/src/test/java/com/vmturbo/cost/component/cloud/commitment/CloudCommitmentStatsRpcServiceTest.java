package com.vmturbo.cost.component.cloud.commitment;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilization;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsResponse.CloudCommitmentUtilizationRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.TopologyType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc.CloudCommitmentStatsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.SourceProjectedFieldsDataStore;

/**
 * Unit test for {@link CloudCommitmentStatsRpcService}.
 */
public class CloudCommitmentStatsRpcServiceTest {

    private final SourceProjectedFieldsDataStore<UtilizationInfo> utilizationStore = Mockito.mock(
            SourceProjectedFieldsDataStore.class);

    private final CloudCommitmentStatsRpcService service = new CloudCommitmentStatsRpcService(
            Mockito.mock(CloudCommitmentCoverageStore.class),
            Mockito.mock(CloudCommitmentUtilizationStore.class),
            Mockito.mock(SourceProjectedFieldsDataStore.class), utilizationStore, 3);

    /**
     * GRPC test server.
     */
    @Rule
    public final GrpcTestServer server = GrpcTestServer.newServer(service);

    private CloudCommitmentStatsServiceBlockingStub cloudCommitmentStatsService;

    /**
     * Setup.
     */
    @Before
    public void setUp() {
        cloudCommitmentStatsService = CloudCommitmentStatsServiceGrpc.newBlockingStub(
                server.getChannel());
    }

    /**
     * Test for {@link CloudCommitmentStatsRpcService#getTopologyCommitmentUtilization}.
     */
    @Test
    public void testGetTopologyCommitmentUtilizationStats() {
        testGetTopologyCommitmentUtilizationInternal(TopologyType.TOPOLOGY_TYPE_SOURCE,
                () -> Mockito.when(utilizationStore.getSourceData()),
                () -> Mockito.verify(utilizationStore, Mockito.times(1)).getSourceData());
        testGetTopologyCommitmentUtilizationInternal(TopologyType.TOPOLOGY_TYPE_PROJECTED,
                () -> Mockito.when(utilizationStore.getProjectedData()),
                () -> Mockito.verify(utilizationStore, Mockito.times(1)).getProjectedData());
    }

    private void testGetTopologyCommitmentUtilizationInternal(
            @Nonnull final TopologyType topologyType,
            @Nonnull final Supplier<OngoingStubbing<Optional<UtilizationInfo>>> storeMethodStubber,
            @Nonnull final Runnable numberOfInvocationsVerifier) {
        // ARRANGE
        final Set<Long> commitments = LongStream.range(1, 10).boxed().collect(Collectors.toSet());
        storeMethodStubber.get().thenReturn(Optional.of(UtilizationInfo.builder()
                .topologyInfo(TopologyInfo.newBuilder().setTopologyId(123).build())
                .putAllCommitmentIdToUtilization(commitments.stream()
                        .collect(Collectors.toMap(Function.identity(),
                                key -> CloudCommitmentUtilization.getDefaultInstance())))
                .build()));

        // ACT
        final List<GetTopologyCommitmentUtilizationStatsResponse> responses = new ArrayList<>();
        cloudCommitmentStatsService.getTopologyCommitmentUtilization(
                GetTopologyCommitmentUtilizationStatsRequest.newBuilder().setTopologyType(
                        topologyType).setChunkSize(5).build()).forEachRemaining(responses::add);

        // ASSERT
        numberOfInvocationsVerifier.run();
        Assert.assertEquals(3, responses.size());

        Assert.assertEquals(commitments.stream()
                .map(oid -> CloudCommitmentUtilizationRecord.newBuilder()
                        .setCommitmentOid(oid)
                        .setUtilization(CloudCommitmentUtilization.getDefaultInstance())
                        .build())
                .collect(Collectors.toSet()), responses.stream()
                .flatMap(r -> r.getCommitmentUtilizationRecordChunkList().stream())
                .collect(Collectors.toSet()));
    }
}