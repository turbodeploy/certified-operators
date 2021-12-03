package com.vmturbo.cost.component.cloud.commitment;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilization;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServiceGrpc;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServiceGrpc.CloudCommitmentServiceBlockingStub;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetCloudCommitmentInfoForAnalysisRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetCloudCommitmentInfoForAnalysisResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetCloudCommitmentInfoForAnalysisResponse.CommitmentInfoBucket;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.cloud.commitment.mapping.MappingInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.DiagnosableSingleFieldDataStore;
import com.vmturbo.cost.component.stores.SingleFieldDataStore;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Unit test for {@link CloudCommitmentRpcService}.
 */
public class CloudCommitmentRpcServiceTest {

    private final SingleFieldDataStore<UtilizationInfo> utilizationStore = Mockito.mock(
            DiagnosableSingleFieldDataStore.class);

    private final SingleFieldDataStore<MappingInfo> mappingStore = Mockito.mock(
            DiagnosableSingleFieldDataStore.class);

    private final CloudCommitmentRpcService service = new CloudCommitmentRpcService(mappingStore,
            utilizationStore);

    /**
     * GRPC test server.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(service);

    private CloudCommitmentServiceBlockingStub cloudCommitmentServiceStub;

    /**
     * Setup.
     */
    @Before
    public void setUp() {
        cloudCommitmentServiceStub = CloudCommitmentServiceGrpc.newBlockingStub(
                server.getChannel());
    }

    /**
     * Test for {@link CloudCommitmentRpcService#getCloudCommitmentInfoForAnalysis}.
     */
    @Test
    public void testGetCloudCommitmentInfoForAnalysis() {
        Stream.of(ImmutableTriple.of(UtilizationInfo.builder()
                        .topologyInfo(TopologyInfo.newBuilder().setCreationTime(1234).build())
                        .putCommitmentIdToUtilization(1, CloudCommitmentUtilization.newBuilder()
                                .setOverhead(CloudCommitmentAmount.newBuilder()
                                        .setAmount(CurrencyAmount.newBuilder().setCurrency(111)))
                                .build())
                        .build(), MappingInfo.builder()
                        .topologyInfo(TopologyInfo.newBuilder().setCreationTime(123).build())
                        .addCloudCommitmentMapping(CloudCommitmentMapping.newBuilder()
                                .setCloudCommitmentOid(1)
                                .setCommitmentAmount(CloudCommitmentAmount.newBuilder()
                                        .setAmount(CurrencyAmount.newBuilder().setCurrency(123)))
                                .build())
                        .build(),
                GetCloudCommitmentInfoForAnalysisResponse.newBuilder()
                        .addCommitmentBucket(CommitmentInfoBucket.newBuilder()
                                .setTimestampMillis(1234)
                                .putCloudCommitmentOverhead(1, CloudCommitmentAmount.newBuilder()
                                        .setAmount(CurrencyAmount.newBuilder().setCurrency(111))
                                        .build()))
                        .addCommitmentBucket(CommitmentInfoBucket.newBuilder()
                                .setTimestampMillis(123)
                                .addCloudCommitmentMapping(CloudCommitmentMapping.newBuilder()
                                        .setCloudCommitmentOid(1)
                                        .setCommitmentAmount(CloudCommitmentAmount.newBuilder()
                                                .setAmount(CurrencyAmount.newBuilder()
                                                        .setCurrency(123)))
                                        .build()))
                        .build()), ImmutableTriple.of(UtilizationInfo.builder()
                        .topologyInfo(TopologyInfo.newBuilder().build())
                        .putCommitmentIdToUtilization(1, CloudCommitmentUtilization.newBuilder()
                                .setOverhead(CloudCommitmentAmount.newBuilder()
                                        .setAmount(CurrencyAmount.newBuilder().setCurrency(111)))
                                .build())
                        .build(), MappingInfo.builder()
                        .topologyInfo(TopologyInfo.newBuilder().build())
                        .addCloudCommitmentMapping(CloudCommitmentMapping.newBuilder()
                                .setCloudCommitmentOid(1)
                                .setCommitmentAmount(CloudCommitmentAmount.newBuilder()
                                        .setAmount(CurrencyAmount.newBuilder().setCurrency(123)))
                                .build())
                        .build(), GetCloudCommitmentInfoForAnalysisResponse.newBuilder()
                        .addCommitmentBucket(CommitmentInfoBucket.newBuilder()
                                .putCloudCommitmentOverhead(1, CloudCommitmentAmount.newBuilder()
                                        .setAmount(CurrencyAmount.newBuilder().setCurrency(111))
                                        .build())
                                .addCloudCommitmentMapping(CloudCommitmentMapping.newBuilder()
                                        .setCloudCommitmentOid(1)
                                        .setCommitmentAmount(CloudCommitmentAmount.newBuilder()
                                                .setAmount(CurrencyAmount.newBuilder().setCurrency(123)))
                                        .build()))
                        .build()),
                ImmutableTriple.<UtilizationInfo, MappingInfo, GetCloudCommitmentInfoForAnalysisResponse>of(
                        null, null, GetCloudCommitmentInfoForAnalysisResponse.getDefaultInstance()))
                .forEach(testCase -> {
                    Mockito.when(utilizationStore.getData()).thenReturn(
                            Optional.ofNullable(testCase.getLeft()));
                    Mockito.when(mappingStore.getData()).thenReturn(
                            Optional.ofNullable(testCase.getMiddle()));
                    final GetCloudCommitmentInfoForAnalysisResponse cloudCommitmentInfoForAnalysis =
                            cloudCommitmentServiceStub.getCloudCommitmentInfoForAnalysis(
                                    GetCloudCommitmentInfoForAnalysisRequest.getDefaultInstance());
                    Assert.assertEquals(testCase.getRight(), cloudCommitmentInfoForAnalysis);
                });
    }
}