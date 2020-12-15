package com.vmturbo.cost.component.cca;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.CloudCommitmentAnalysisManager;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.reserved.instance.PlanReservedInstanceStore;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;

/**
 * Testing the cloud commitment analysis runner.
 */
public class CloudCommitmentAnalysisRunnerTest {

    private final RepositoryServiceMole repositoryService = spy(new RepositoryServiceMole());

    /**
     * Create a test grpc server for the reposiotry client.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(repositoryService);

    private RepositoryServiceBlockingStub repositoryClient;

    private SearchServiceBlockingStub searchServiceStub;

    private CloudCommitmentAnalysisRunner cloudCommitmentAnalysisRunner;

    private TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class));

    private CloudCommitmentAnalysisManager cloudCommitmentAnalysisManager = mock(CloudCommitmentAnalysisManager.class);

    private final CloudCommitmentSettingsFetcher cloudCommitmentSettingsFetcher = mock(CloudCommitmentSettingsFetcher.class);

    private final PlanReservedInstanceStore planReservedInstanceStore = mock(PlanReservedInstanceStore.class);

    private static final String PROVIDER_TYPE = "AWS";

    private final long topologyContextId = 12345678L;

    final List<Cost.ReservedInstanceBought> reservedInstancesBoughtList = ImmutableList.of(
            Cost.ReservedInstanceBought.newBuilder()
                    .setId(1)
                    .setReservedInstanceBoughtInfo(
                            ReservedInstanceBoughtInfo.newBuilder()
                                    .setBusinessAccountId(1)
                                    .setReservedInstanceBoughtCoupons(
                                            ReservedInstanceBoughtCoupons.newBuilder()
                                                    .setNumberOfCoupons(10)
                                                    .build())
                                    .build())
                    .build(),
            Cost.ReservedInstanceBought.newBuilder()
                    .setId(2)
                    .setReservedInstanceBoughtInfo(
                            ReservedInstanceBoughtInfo.newBuilder()
                                    .setBusinessAccountId(1).build())
                    .build(),
            ReservedInstanceBought.newBuilder()
                    .setId(3)
                    .setReservedInstanceBoughtInfo(
                            ReservedInstanceBoughtInfo.newBuilder()
                                    .setBusinessAccountId(2)
                                    .setReservedInstanceBoughtCoupons(
                                            ReservedInstanceBoughtCoupons.newBuilder()
                                                    .setNumberOfCoupons(10)
                                                    .setNumberOfCouponsUsed(1)
                                                    .build())
                                    .build())
                    .build());

    private final CloudCommitmentAnalysisInfo cloudCommitmentAnalysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
            .setAnalysisTag("1234").setOid(11L).setCreationTime(9000L).build();

    /**
     * Setup the test.
     *
     * @throws IOException An IO Exception.
     */
    @Before
    public void setup() throws IOException {
        repositoryClient = RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        searchServiceStub = SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        startGrpcTestServer();
        cloudCommitmentAnalysisRunner = new CloudCommitmentAnalysisRunner(cloudCommitmentAnalysisManager,
                cloudCommitmentSettingsFetcher, planReservedInstanceStore,
                repositoryClient, searchServiceStub, cloudTopologyFactory);
        when(cloudCommitmentSettingsFetcher.allocationFlexible()).thenReturn(true);
        when(cloudCommitmentSettingsFetcher.allocationSuspended()).thenReturn(true);
        when(cloudCommitmentSettingsFetcher.includeTerminatedEntities()).thenReturn(true);
        when(cloudCommitmentSettingsFetcher.maxDemandPercentage()).thenReturn(75f);
        when(cloudCommitmentSettingsFetcher.minimumSavingsOverOnDemand()).thenReturn(80f);
        when(planReservedInstanceStore.getReservedInstanceBoughtByPlanId(topologyContextId)).thenReturn(reservedInstancesBoughtList);
        when(cloudCommitmentAnalysisManager.startAnalysis(any())).thenReturn(cloudCommitmentAnalysisInfo);
    }

    /**
     * Test that the cloud commitment analysis was actually invoked.
     */
    @Test
    public void testCloudCommitmentInvocation() {
        StartBuyRIAnalysisRequest request = buildStartBuyRIAnalysisRequest();
        final StreamObserver<StartBuyRIAnalysisResponse> responseStreamObserver = mock(StreamObserver.class);
        cloudCommitmentAnalysisRunner.runCloudCommitmentAnalysis(request, responseStreamObserver);
        verify(cloudCommitmentAnalysisManager, atLeastOnce()).startAnalysis(any());
    }

    private StartBuyRIAnalysisRequest buildStartBuyRIAnalysisRequest() {
        StartBuyRIAnalysisRequest.Builder requestBuilder = StartBuyRIAnalysisRequest.newBuilder();
        List<OSType> platforms = new ArrayList(Arrays.asList(OSType.values()));
        List<Tenancy> tenancies = new ArrayList(Arrays.asList(Tenancy.values()));
        List<Long> regions = new ArrayList(Arrays.asList(1L, 3L, 4L));
        List<Long> accounts = new ArrayList(Arrays.asList(5L, 6L, 7L, 8L));

        requestBuilder.addAllPlatforms(platforms);
        requestBuilder.addAllTenancies(tenancies);
        requestBuilder.addAllAccounts(accounts);
        requestBuilder.addAllRegions(regions);

        int term = 1;
        OfferingClass offeringClass = OfferingClass.STANDARD;
        PaymentOption paymentOption = PaymentOption.ALL_UPFRONT;

        ReservedInstanceType.Builder typeBuilder = ReservedInstanceType.newBuilder();
        typeBuilder.setTermYears(term);
        typeBuilder.setOfferingClass(offeringClass);
        typeBuilder.setPaymentOption(paymentOption);

        RIPurchaseProfile.Builder profileBuilder =
                com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile.newBuilder();
        profileBuilder.setRiType(typeBuilder.build());
        RIPurchaseProfile riPurchaseProfile = profileBuilder.build();

        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyContextId(topologyContextId).build();
        requestBuilder.setTopologyInfo(topologyInfo);
        requestBuilder.putAllPurchaseProfileByCloudtype(
                ImmutableMap.of(PROVIDER_TYPE, riPurchaseProfile));

        return requestBuilder.build();
    }

    private void startGrpcTestServer() throws IOException {
        grpcServer.start();
    }
}
