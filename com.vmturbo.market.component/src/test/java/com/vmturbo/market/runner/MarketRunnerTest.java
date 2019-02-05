package com.vmturbo.market.runner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
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
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfigCustomizer;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;

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

    private AnalysisFactory analysisFactory = mock(AnalysisFactory.class);

    private TopologyCostCalculator topologyCostCalculator = mock(TopologyCostCalculator.class);

    private TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            mock(TopologyEntityCloudTopologyFactory.class);

    @Before
    public void before() {
        IdentityGenerator.initPrefix(0);
        threadPool = Executors.newFixedThreadPool(2);
        runner = new MarketRunner(threadPool, serverApi, analysisFactory, Optional.empty());

        topologyContextId += 100;

        AnalysisConfig.Builder configBuilder = AnalysisConfig.newBuilder(AnalysisUtil.QUOTE_FACTOR,
            AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT,
            Collections.emptyMap());
        doAnswer(invocation -> {
            TopologyInfo topologyInfo = invocation.getArgumentAt(0, TopologyInfo.class);
            Set<TopologyEntityDTO> entities = invocation.getArgumentAt(1, Set.class);
            AnalysisConfigCustomizer configCustomizer =
                    invocation.getArgumentAt(2, AnalysisConfigCustomizer.class);
            configCustomizer.customize(configBuilder);

            final TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
            final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
            when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
            final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
            when(cloudCostCalculatorFactory.newCalculator()).thenReturn(cloudCostCalculator);
            final MarketPriceTableFactory priceTableFactory = mock(MarketPriceTableFactory.class);
            when(priceTableFactory.newPriceTable(any(), eq(CloudCostData.empty()))).thenReturn(mock(MarketPriceTable.class));
            when(cloudTopologyFactory.newCloudTopology(any())).thenReturn(mock(TopologyEntityCloudTopology.class));

            return new Analysis(topologyInfo, entities,
                    GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                    Clock.systemUTC(), configBuilder.build(),
                    cloudTopologyFactory, cloudCostCalculatorFactory, priceTableFactory);
        }).when(analysisFactory).newAnalysis(any(), any(), any());
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
            maxPlacementsOverride, rightsizeLowerWatermark, rightsizeUpperWatermark);
        assertTrue(runner.getRuns().contains(analysis));
        while (!analysis.isDone()) {
            Thread.sleep(100);
        }
        assertSame("Plan completed with an error : " + analysis.getErrorMsg(),
            AnalysisState.SUCCEEDED, analysis.getState());
        verify(serverApi, Mockito.times(1)).notifyActionsRecommended(analysis.getActionPlan().get());
        // since the IDgenerator gives us a different projectedTopoID every time, we create a
        // MockitoMatcher using anyLong to represent this parameter
        verify(serverApi, Mockito.times(1))
                .notifyProjectedTopology(eq(topologyInfo), anyLong(), anySet(),
                        eq(analysis.getProjectedTopology().get()));
    }

    /**
     * Test that when trying to run the same topology context id twice, the first one gets queued,
     * and the second one returns the Analysis object of the first one.
     */
    @Test
    public void testContextIDs() {
        Set<TopologyEntityDTO> dtos = dtos(true);
        Analysis analysis1 =
            runner.scheduleAnalysis(topologyInfo, dtos, true,  maxPlacementsOverride,
                    rightsizeLowerWatermark, rightsizeLowerWatermark);
        Analysis analysis2 =
            runner.scheduleAnalysis(topologyInfo, dtos, true,  maxPlacementsOverride,
                    rightsizeLowerWatermark, rightsizeUpperWatermark);
        Analysis analysis3 = runner.scheduleAnalysis(topologyInfo.toBuilder()
                        .setTopologyContextId(topologyInfo.getTopologyContextId() + 1)
                        .build(), dtos, true, maxPlacementsOverride,
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
        Analysis badAnalysis = mock(Analysis.class);
        doReturn(badAnalysis).when(analysisFactory).newAnalysis(any(), any(), any());

        when(badAnalysis.getContextId()).thenReturn(topologyInfo.getTopologyContextId());
        when(badAnalysis.isDone()).thenReturn(true);
        when(badAnalysis.getTopologyInfo()).thenReturn(topologyInfo);
        when(badAnalysis.getState()).thenReturn(AnalysisState.FAILED);

        Analysis analysis =
            runner.scheduleAnalysis(topologyInfo, badDtos, true,
                    maxPlacementsOverride, rightsizeLowerWatermark, rightsizeUpperWatermark);

        assertSame(badAnalysis, analysis);

        verify(analysis, timeout(1000)).execute();
        verify(analysis).isDone();

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
