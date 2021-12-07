package com.vmturbo.market.runner;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfigCustomizer;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.runner.cost.MigratedWorkloadCloudCommitmentAnalysisService;
import com.vmturbo.market.runner.postprocessor.NamespaceQuotaAnalysisEngine;
import com.vmturbo.market.runner.reconfigure.ExternalReconfigureActionEngine;
import com.vmturbo.market.runner.wastedfiles.WastedFilesAnalysisEngine;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcherFactory;
import com.vmturbo.market.topology.conversions.TierExcluder;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.cloud.JournalActionSavingsCalculatorFactory;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.processor.api.util.SingleTopologyProcessingGate;

/**
 * Unit tests for market timeout.
 */
public class MarketTimeoutTest {

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(
            FeatureFlags.NAMESPACE_QUOTA_RESIZING, FeatureFlags.ENABLE_ANALYSIS_TIMEOUT);

    private MarketRunner runner;
    private ExecutorService threadPool;
    private MarketNotificationSender serverApi = mock(MarketNotificationSender.class);

    private long topologyId = 2000;
    private long rtContextId = 777777;
    private long creationTime = 3000;
    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());
    private final SettingServiceMole testSettingService =
                 spy(new SettingServiceMole());
    private Optional<Integer> maxPlacementsOverride = Optional.empty();
    private final boolean useQuoteCacheDuringSnm = false;
    private final boolean replayProvisionsForRt = false;
    private final float rightsizeLowerWatermark = 0.1f;
    private final float rightsizeUpperWatermark = 0.7f;
    private final float discountedComputeCostFactor = 4f;

    /**
     * Rule to initialize and use Grpc.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService, testSettingService);

    private TopologyInfo rtTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(topologyId)
            .setTopologyContextId(rtContextId)
            .setCreationTime(creationTime)
            .setTopologyType(TopologyType.REALTIME)
            .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
            .build();

    private AnalysisFactory analysisFactory = mock(AnalysisFactory.class);

    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);

    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);

    private InitialPlacementFinder initialPlacementFinder =
            mock(InitialPlacementFinder.class);

    private ReversibilitySettingFetcherFactory reversibilitySettingFetcherFactory =
            mock(ReversibilitySettingFetcherFactory.class);

    private JournalActionSavingsCalculatorFactory actionSavingsCalculatorFactory =
            mock(JournalActionSavingsCalculatorFactory.class);

    /**
     * Setting up analysis.
     */
    @Before
    public void before() {
        IdentityGenerator.initPrefix(0);
        threadPool = Executors.newFixedThreadPool(2);
        // initialize market runner with a low timeout of 1secs
        runner = new MarketRunner(threadPool, serverApi,
            analysisFactory, Optional.empty(),
                new SingleTopologyProcessingGate(10, TimeUnit.MINUTES),
                initialPlacementFinder, 1);

        AnalysisConfig.Builder configBuilder = AnalysisConfig.newBuilder(MarketAnalysisUtils.QUOTE_FACTOR,
            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT,
            Collections.emptyMap(), false, MarketAnalysisUtils.PRICE_WEIGHT_SCALE,
            false);
        doAnswer(invocation -> {
            AnalysisConfigCustomizer configCustomizer =
                    invocation.getArgumentAt(2, AnalysisConfigCustomizer.class);
            configCustomizer.customize(configBuilder);

            final TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
            final TopologyCostCalculator cloudCostCalculator = mock(TopologyCostCalculator.class);
            when(cloudCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
            final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
            when(cloudCostCalculatorFactory.newCalculator(any(), any())).thenReturn(cloudCostCalculator);
            ConsistentScalingHelper consistentScalingHelper = mock(ConsistentScalingHelper.class);
            when(consistentScalingHelper.getScalingGroupId(any())).thenReturn(Optional.empty());
            when(consistentScalingHelper.getScalingGroupUsage(any())).thenReturn(Optional.empty());
            when(consistentScalingHelperFactory.newConsistentScalingHelper(any(), any())).thenReturn(consistentScalingHelper);
            final MarketPriceTableFactory priceTableFactory = mock(MarketPriceTableFactory.class);
            when(priceTableFactory.newPriceTable(any(), eq(CloudCostData.empty()))).thenReturn(mock(
                    CloudRateExtractor.class));
            when(cloudTopologyFactory.newCloudTopology(any())).thenReturn(mock(TopologyEntityCloudTopology.class));
            final WastedFilesAnalysisEngine wastedFilesAnalysisEngine =
                mock(WastedFilesAnalysisEngine.class);
            final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory =
                    mock(BuyRIImpactAnalysisFactory.class);
            final NamespaceQuotaAnalysisEngine namespaceQuotaAnalysisEngine =
                mock(NamespaceQuotaAnalysisEngine.class);
            when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
            final GroupServiceBlockingStub groupServiceGrpc =
                    GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
            final MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService = mock(MigratedWorkloadCloudCommitmentAnalysisService.class);
            doNothing().when(migratedWorkloadCloudCommitmentAnalysisService).startAnalysis(anyLong(), any(), anyList());

            final ExternalReconfigureActionEngine externalReconfigureActionEngine = mock(
                    ExternalReconfigureActionEngine.class);
            Set<TopologyEntityDTO> entities = invocation.getArgumentAt(1, Set.class);
            return new Analysis(rtTopologyInfo, entities, new GroupMemberRetriever(groupServiceGrpc),
                    Clock.systemUTC(), configBuilder.build(),
                    cloudTopologyFactory, cloudCostCalculatorFactory, priceTableFactory,
                    wastedFilesAnalysisEngine, buyRIImpactAnalysisFactory, namespaceQuotaAnalysisEngine,
                    tierExcluderFactory, mock(AnalysisRICoverageListener.class),
                    consistentScalingHelperFactory, initialPlacementFinder,
                    reversibilitySettingFetcherFactory, migratedWorkloadCloudCommitmentAnalysisService,
                    new CommodityIdUpdater(), actionSavingsCalculatorFactory,
                    externalReconfigureActionEngine);
        }).when(analysisFactory).newAnalysis(any(), any(), any(), any());
    }

    /**
     * Shutdown after analysis.
     */
    @After
    public void after() {
        threadPool.shutdownNow();
    }

    /**
     * Test that the market times out while processing 100000 entities and FAILS gracefully.
     * @throws InterruptedException potential due to sleep.
     */
    @Test
    public void testMarketTimeout() throws InterruptedException {
        featureFlagTestRule.enable(FeatureFlags.ENABLE_ANALYSIS_TIMEOUT);
        Analysis analysis = runner.scheduleAnalysis(rtTopologyInfo, dtos(true),
            Tracing.trace("test").spanContext(), true,
            maxPlacementsOverride, useQuoteCacheDuringSnm, replayProvisionsForRt,
            rightsizeLowerWatermark, rightsizeUpperWatermark,
            discountedComputeCostFactor);
        assertTrue(runner.getRuns().contains(analysis));
        int timeoutcounter = 100;
        while (!analysis.isDone() && timeoutcounter > 0) {
            timeoutcounter--;
            Thread.sleep(100);
        }
        // test market failed due to timeout
        assertTrue("Analysis did not complete.", timeoutcounter > 0);
        assertSame(AnalysisState.FAILED, analysis.getState());
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
        for (int i = 0; i < 100000; i++) {
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
