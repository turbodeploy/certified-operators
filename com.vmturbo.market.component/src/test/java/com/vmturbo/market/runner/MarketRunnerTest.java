package com.vmturbo.market.runner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.runner.Analysis.AnalysisFactory;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;

/**
 * Unit tests for the {@link MarketRunner}.
 */
@Ignore("Some tests fail intermittently on Jenkins. See issue OM-28793")
public class MarketRunnerTest {

    private MarketRunner runner;
    private ExecutorService threadPool;
    private MarketNotificationSender serverApi = mock(MarketNotificationSender.class);

    private long topologyContextId = 1000;
    private long topologyId = 2000;
    private long creationTime = 3000;
    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());
    private final SettingPolicyServiceMole testSettingPolicyService =
            spy(new SettingPolicyServiceMole());
    private final SettingServiceMole testSettingService =
                 spy(new SettingServiceMole());
    private SettingServiceBlockingStub settingServiceClient;
    private Optional<Integer> maxPlacementsOverride = Optional.empty();
    private final static float rightsizeLowerWatermark = 0.1f;
    private final static float rightsizeUpperWatermark = 0.7f;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService,
                     testSettingPolicyService, testSettingService);

    private TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(topologyId)
            .setTopologyContextId(topologyContextId)
            .setCreationTime(creationTime)
            .setTopologyType(TopologyType.PLAN)
            .build();

    @Before
    public void before() {
        IdentityGenerator.initPrefix(0);
        threadPool = Executors.newFixedThreadPool(2);
        AnalysisFactory analysisFactory = new AnalysisFactory();
        settingServiceClient =
            SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());
        runner = new MarketRunner(threadPool, serverApi, analysisFactory, 0.75f);

        topologyContextId += 100;
    }

    @After
    public void after() {
        threadPool.shutdownNow();
    }

    /**
     * Test that the constructor of {@link Analysis} initializes the fields as expected.
     * @throws InterruptedException because I use Thread.sleep(.)
     */
    @Test
    public void testGetRuns() throws Exception {
        Analysis analysis = runner.scheduleAnalysis(topologyInfo, dtos(true), true,
            settingServiceClient, maxPlacementsOverride, rightsizeLowerWatermark, rightsizeUpperWatermark);
        assertTrue(runner.getRuns().contains(analysis));
        while (!analysis.isDone()) {
            Thread.sleep(100);
        }
        assertSame("Plan completed with an error : " + analysis.getErrorMsg(),
            AnalysisState.SUCCEEDED, analysis.getState());
        Mockito.verify(serverApi, Mockito.times(1)).notifyActionsRecommended(analysis.getActionPlan().get());
        // since the IDgenerator gives us a different projectedTopoID every time, we create a
        // MockitoMatcher using anyLong to represent this parameter
        Mockito.verify(serverApi, Mockito.times(1))
                .notifyProjectedTopology(eq(topologyInfo), anyLong(),
                        eq(analysis.getProjectedTopology().get()));
        PriceIndexMessage pim = PriceIndexMessage.newBuilder(analysis.getPriceIndexMessage().get())
                        .setTopologyContextId(analysis.getContextId())
                        .build();
        Mockito.verify(serverApi).sendPriceIndex(eq(topologyInfo), eq(pim));
    }

    /**
     * Test that when trying to run the same topology context id twice, the first one gets queued,
     * and the second one returns the Analysis object of the first one.
     */
    @Test
    public void testContextIDs() {
        Set<TopologyEntityDTO> dtos = dtos(true);
        Analysis analysis1 =
            runner.scheduleAnalysis(topologyInfo, dtos, true, settingServiceClient, maxPlacementsOverride,
                    rightsizeLowerWatermark, rightsizeLowerWatermark);
        Analysis analysis2 =
            runner.scheduleAnalysis(topologyInfo, dtos, true, settingServiceClient, maxPlacementsOverride,
                    rightsizeLowerWatermark, rightsizeUpperWatermark);
        Analysis analysis3 = runner.scheduleAnalysis(topologyInfo.toBuilder()
                        .setTopologyContextId(topologyInfo.getTopologyContextId() + 1)
                        .build(), dtos, true, settingServiceClient, maxPlacementsOverride,
                    rightsizeLowerWatermark, rightsizeUpperWatermark);
        assertSame(analysis1, analysis2);
        assertNotSame(analysis1, analysis3);
    }

    /**
     * Test that a bad plan results in a FAILED state, and that a completed test is removed from the
     * list of runs.
     * @throws InterruptedException because I use Thread.sleep(.)
     */
    @Test
    public void testBadPlan() throws InterruptedException {
        Set<TopologyEntityDTO> badDtos = dtos(false);
        Analysis analysis =
            runner.scheduleAnalysis(topologyInfo, badDtos, true, settingServiceClient,
                    maxPlacementsOverride, rightsizeLowerWatermark, rightsizeUpperWatermark);
        while (!analysis.isDone()) {
            Thread.sleep(100);
        }
        assertSame(AnalysisState.FAILED, analysis.getState());
        assertNotNull(analysis.getErrorMsg());
        assertFalse(runner.getRuns().contains(analysis));
    }

    private long entityCount = 0;
    private CommodityType commType = CommodityType.newBuilder().setType(1).build();

    /**
     * Construct a set of a entity DTOs representing a few buyers and one seller.
     * @param good when false, the buyers buy a negative amount,
     *     and the resulted plan ends with an error.
     * @return the set of entity DTOs
     */
    private Set<TopologyEntityDTO> dtos(boolean good) {
        Set<TopologyEntityDTO> dtos = Sets.newHashSet();
        TopologyEntityDTO seller = seller();
        dtos.add(seller);
        long sellerOid = seller.getOid();
        long used = good ? 1 : -1;
        for (int i = 0; i < 3; i++) {
            dtos.add(buyer(sellerOid, used));
        }
        return dtos;
    }

    /**
     * A TopologyEntityDTO that represents a seller.
     * @return a TopologyEntityDTO that represents a seller
     */
    private TopologyEntityDTO seller() {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(2000) // This must not be a VDC, which is ignored in M2
                .setOid(entityCount++)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(commType)
                        .setCapacity(1)
                        .build())
                .build();
    }

    /**
     * A TopologyEntityDTO that represents a buyer.
     * @param sellerOid the oid of the seller that this buyer is buying from
     * @param used the amount bought
     * @return a TopologyEntityDTO that represents a buyer
     */
    private TopologyEntityDTO buyer(long sellerOid, long used) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(1000) // This must not be a VDC, which is ignored in M2
            .setOid(entityCount++)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(sellerOid)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                    .setCommodityType(commType)
                    .setUsed(used)))
            .build();
    }
}
