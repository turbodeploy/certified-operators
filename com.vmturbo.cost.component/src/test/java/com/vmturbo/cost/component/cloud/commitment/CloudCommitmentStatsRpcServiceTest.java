package com.vmturbo.cost.component.cloud.commitment;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.cloud.common.commitment.CloudCommitmentUtils;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationGroupBy;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentUtilization;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord.StatValue;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.TopologyType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc.CloudCommitmentStatsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.coverage.CommitmentCoverageAggregator;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.CommitmentUtilizationAggregator;
import com.vmturbo.cost.component.cloud.commitment.utilization.TopologyCommitmentUtilizationFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.SourceProjectedFieldsDataStore;

/**
 * Unit test for {@link CloudCommitmentStatsRpcService}.
 */
public class CloudCommitmentStatsRpcServiceTest {

    private final SourceProjectedFieldsDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> utilizationStore =
            Mockito.mock(SourceProjectedFieldsDataStore.class);

    private final CloudCommitmentStatsConverter statsConverter = new CloudCommitmentStatsConverter(
            new CommitmentCoverageAggregator(),
            new CommitmentUtilizationAggregator());

    private final CloudCommitmentStatsRpcService service = new CloudCommitmentStatsRpcService(
            Mockito.mock(CloudCommitmentCoverageStore.class),
            Mockito.mock(CloudCommitmentUtilizationStore.class),
            Mockito.mock(SourceProjectedFieldsDataStore.class),
            utilizationStore,
            statsConverter,
            3);

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

    @Test
    public void testGetTopologyCommitmentUtilizationStatsSource() {

        // set up the utilization store
        final Set<Long> commitments = LongStream.range(1, 10).boxed().collect(Collectors.toSet());
        Mockito.when(utilizationStore.filterData(eq(TopologyType.TOPOLOGY_TYPE_SOURCE), any()))
                .thenReturn(Optional.of(UtilizationInfo.builder()
                        .topologyInfo(TopologyInfo.newBuilder().setCreationTime(123).build())
                        .putAllCommitmentUtilizationMap(commitments.stream()
                                .collect(Collectors.toMap(Function.identity(),
                                        commitmentOid -> ScopedCommitmentUtilization.newBuilder()
                                                .setCloudCommitmentOid(commitmentOid)
                                                .addUtilizationVector(
                                                        CloudCommitmentUtilizationVector.newBuilder()
                                                                .setVectorType(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                                                                .setUsed(commitmentOid)
                                                                .setCapacity(10.0))
                                                .build())))
                        .build()));


        // ACT
        final List<GetTopologyCommitmentUtilizationStatsResponse> responses = new ArrayList<>();
        cloudCommitmentStatsService.getTopologyCommitmentUtilization(
                GetTopologyCommitmentUtilizationStatsRequest.newBuilder()
                        .setTopologyType(TopologyType.TOPOLOGY_TYPE_SOURCE)
                        .addGroupBy(CloudCommitmentUtilizationGroupBy.COMMITMENT_UTILIZATION_GROUP_BY_COMMITMENT)
                        .setChunkSize(5)
                        .build())
                .forEachRemaining(responses::add);

        // ASSERT
        Mockito.verify(utilizationStore).filterData(eq(TopologyType.TOPOLOGY_TYPE_SOURCE), any());
        Assert.assertEquals(3, responses.size());

        Assert.assertEquals(commitments.stream()
                .map(oid -> CloudCommitmentStatRecord.newBuilder()
                        .setSnapshotDate(123)
                        .setCommitmentId(oid)
                        .setRegionId(0)
                        .setAccountId(0)
                        .setServiceProviderId(0)
                        .setCoverageTypeInfo(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO)
                        .setSampleCount(1)
                        .setValues(StatValue.newBuilder()
                                .setAvg(oid)
                                .setTotal(oid)
                                .setMin(oid)
                                .setMax(oid)
                                .build())
                        .setCapacity(StatValue.newBuilder()
                                .setAvg(10.0)
                                .setTotal(10.0)
                                .setMin(10.0)
                                .setMax(10.0)
                                .build())
                        .build())
                .collect(Collectors.toSet()), responses.stream()
                .flatMap(r -> r.getCommitmentUtilizationRecordChunkList().stream())
                .collect(Collectors.toSet()));
    }
}