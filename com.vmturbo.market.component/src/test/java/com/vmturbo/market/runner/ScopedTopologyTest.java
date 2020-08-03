package com.vmturbo.market.runner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
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
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.topology.conversions.CommodityIndex;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.MarketAnalysisUtils;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcher;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcherFactory;
import com.vmturbo.market.topology.conversions.TierExcluder;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.topology.processor.api.util.TopologyProcessingGate;

@Ignore("Some tests fail intermittently on Jenkins. See issue OM-28793")
public class ScopedTopologyTest {

    private static final TopologyInfo PLAN_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanProjectType(PlanProjectType.USER))
            .build();

    private static final boolean INCLUDE_VDC = false;
    private static final long ID_GENERATOR_PREFIX = 1;
    private static final int CLUSTER_1_EXPANDED_SE_COUNT = 20;
    private static final int TOTAL_SE_COUNT = 22;
    @SuppressWarnings("FieldCanBeLocal")
    private static String simpleTopologyjSonFile = "protobuf/messages/simple-topology.json";
    private static final long HOST_11_OID = 72207427031424L;
    private static final long HOST_12_OID = 72207427031409L;
    private static final long HOST_13_OID = 72207427031408L;
    private static final long VM_14_OID = 72207427031425L;
    private static final long BAPP_OID = 72207427031448L;
    private static final List<Long> CLUSTER0_SCOPE = ImmutableList.of(HOST_11_OID, HOST_13_OID);
    private static final List<Long> CLUSTER1_SCOPE = ImmutableList.of(HOST_12_OID);

    private static final Gson GSON = new Gson();
    private Set<TopologyEntityDTO.Builder> topologyDTOBuilderSet;
    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());
    private final SettingServiceMole testSettingService =
                 spy(new SettingServiceMole());
    private final float rightsizeLowerWatermark = 0.1f;
    private final float rightsizeUpperWatermark = 0.7f;
    private final float discountedComputeCostFactor = 4f;

    private GroupServiceBlockingStub groupServiceClient;
    private Analysis testAnalysis;
    private MarketPriceTable marketPriceTable = mock(MarketPriceTable.class);
    private CloudCostData ccd = mock(CloudCostData.class);
    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);

    private InitialPlacementFinder initialPlacementFinder =
            mock(InitialPlacementFinder.class);

    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);

    private ReversibilitySettingFetcherFactory reversibilitySettingFetcherFactory =
            mock(ReversibilitySettingFetcherFactory.class);

    private ReversibilitySettingFetcher reversibilitySettingFetcher =
            mock(ReversibilitySettingFetcher.class);

    private final TopologyProcessingGate passthroughGate = new TopologyProcessingGate() {
        @Nonnull
        @Override
        public Ticket enter(@Nonnull final TopologyInfo topologyInfo, @Nonnull final Collection<TopologyEntityDTO> entities) {
            return () -> { };
        }
    };

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupService, testSettingService);

    /**
     * Read the test topology from a resource file (.json).
     *
     * @throws FileNotFoundException if the topology test file is not found
     * @throws InvalidProtocolBufferException if the topology is in the wrong format
     */
    @Before
    public void setup() throws FileNotFoundException, InvalidProtocolBufferException {
        topologyDTOBuilderSet = Objects.requireNonNull(readTopologyFromJsonFile());
        IdentityGenerator.initPrefix(ID_GENERATOR_PREFIX);
        final AnalysisConfig analysisConfig = AnalysisConfig.newBuilder(MarketAnalysisUtils.QUOTE_FACTOR,
            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT,
            Collections.emptyMap())
            .setIncludeVDC(INCLUDE_VDC)
            .build();
        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());

        final TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
        final TopologyCostCalculatorFactory cloudCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        final TopologyCostCalculator topologyCostCalculator = mock(TopologyCostCalculator.class);
        when(topologyCostCalculator.getCloudCostData()).thenReturn(mock(CloudCostData.class));
        when(cloudTopologyFactory.newCloudTopology(any())).thenReturn(mock(TopologyEntityCloudTopology.class));
        when(cloudCostCalculatorFactory.newCalculator(PLAN_TOPOLOGY_INFO, any())).thenReturn(topologyCostCalculator);
        final MarketPriceTableFactory priceTableFactory = mock(MarketPriceTableFactory.class);
        when(priceTableFactory.newPriceTable(any(), any())).thenReturn(mock(MarketPriceTable.class));
        when(ccd.getExistingRiBought()).thenReturn(new ArrayList<>());
        final WastedFilesAnalysisFactory wastedFilesAnalysisFactory =
            mock(WastedFilesAnalysisFactory.class);
        final BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory =
                mock(BuyRIImpactAnalysisFactory.class);
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
        testAnalysis = new Analysis(PLAN_TOPOLOGY_INFO,
            Collections.emptySet(),
            new GroupMemberRetriever(groupServiceClient),
            Clock.systemUTC(),
            analysisConfig,
            cloudTopologyFactory,
            cloudCostCalculatorFactory,
            priceTableFactory,
            wastedFilesAnalysisFactory,
            buyRIImpactAnalysisFactory,
            tierExcluderFactory,
            mock(AnalysisRICoverageListener.class),
            consistentScalingHelperFactory,
            initialPlacementFinder,
            reversibilitySettingFetcherFactory);
    }

    /**
     * In this test we start with "Host #12", which hosts "VM #14" which hosts "App #15".
     * The VM buys "CLUSTER-1" and "STORAGE-CLUSTER-1". There is only one Host providing
     * "CLUSTER-1", and so the expanded scope includes the original Host, the VM and App,
     * the Datastores that provide "STORAGE-CLUSTER-1" (2,4,6,8,10), and all of the
     * DiskArrays.
     *The VM hosts a BusinessApp #1. This entity gets skipped. Thus not bringing in hosts
     * from CLUSTER-0.
     *
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeTopologyCluster1() throws InvalidTopologyException {

        final TopologyConverter converter =
            new TopologyConverter(PLAN_TOPOLOGY_INFO, marketPriceTable, ccd,
                CommodityIndex.newFactory(), tierExcluderFactory, consistentScalingHelperFactory,
                    reversibilitySettingFetcher);
        final Set<EconomyDTOs.TraderTO> traderTOs = convertToMarket(converter);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
                Sets.newHashSet(HOST_12_OID));

        // "Host #12", "VM #14", "App #15", "Datastore #2,4,6,8,10", "DiskArray #1-10", "Datacenter #0"
        // and Business App 1 (which buys from VM14)
        assertThat(scopedTraderTOs.size(), equalTo(CLUSTER_1_EXPANDED_SE_COUNT));
    }

    /**
     * In this test we start with "BusinessApp #1", This is indirectly linked to every host
     * in the topology. We pull in all hosts into the scope in this case.
     *
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeTopologyBusinessApp() throws InvalidTopologyException {

        final TopologyConverter converter =
                new TopologyConverter(PLAN_TOPOLOGY_INFO, marketPriceTable, ccd,
                    CommodityIndex.newFactory(), tierExcluderFactory,
                    consistentScalingHelperFactory, reversibilitySettingFetcher);
        final Set<EconomyDTOs.TraderTO> traderTOs = convertToMarket(converter);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
                Sets.newHashSet(BAPP_OID));

        // "Host #12", "VM #14", "App #15", "Datastore #2,4,6,8,10", "DiskArray #1-10", "Datacenter #0"
        assertThat(scopedTraderTOs.size(), equalTo(TOTAL_SE_COUNT));
    }

    /**
     * In this test, we start with "Host #11", which has no applications. Therefore there are no
     * "upwards" traders. The only other element in the Scoped Topology is the "downwards" Datacenter.
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeTopologyOneHost() throws InvalidTopologyException {
        final TopologyConverter converter =
            new TopologyConverter(PLAN_TOPOLOGY_INFO, marketPriceTable,
                ccd, CommodityIndex.newFactory(), tierExcluderFactory,
                consistentScalingHelperFactory, reversibilitySettingFetcher);
        final Set<EconomyDTOs.TraderTO> traderTOs = convertToMarket(converter);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
                Sets.newHashSet(HOST_11_OID));

        // "Host #11", "Datacenter #0"
        assertThat(scopedTraderTOs.size(), equalTo(2));
    }

    /**
     * In this test, we start with "Host #11" and "Host #13", neither of which has any applications.
     * Therefore there are no "upwards" traders. The only other element in the Scoped Topology
     * is the "downwards" Datacenter.
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeTopologyTwoHosts() throws InvalidTopologyException {
        final TopologyConverter converter =
            new TopologyConverter(PLAN_TOPOLOGY_INFO, marketPriceTable,
                ccd, CommodityIndex.newFactory(), tierExcluderFactory,
                consistentScalingHelperFactory, reversibilitySettingFetcher);
        final Set<EconomyDTOs.TraderTO> traderTOs = convertToMarket(converter);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
                Sets.newHashSet(HOST_11_OID, HOST_13_OID));

        // "Host #11", "Host #13", "Datacenter #0", VM #13 and BApp #1
        assertThat(scopedTraderTOs.size(), equalTo(5));
    }

    /**
     * In this test, we start with "Host #11", which has no applications. Therefore there are no
     * "upwards" traders. The only other element in the Scoped Topology is the "downwards" Datacenter.
     * We add a new, i.e. unplaced ServiceEntity (a VM), and check that is included in the output.
     *
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeTopologyUplacedEnities() throws InvalidTopologyException {
        final TopologyConverter converter =
            new TopologyConverter(PLAN_TOPOLOGY_INFO, marketPriceTable,
                ccd, CommodityIndex.newFactory(), tierExcluderFactory,
                consistentScalingHelperFactory, reversibilitySettingFetcher);
        // add an additional VM, which should be considered unplaced
        topologyDTOBuilderSet.add(TopologyEntityDTO.newBuilder()
                .setDisplayName("VM-unplaced")
                .setOid(999L)
                .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(-1)
                .build()));
        final Set<EconomyDTOs.TraderTO> traderTOs = convertToMarket(converter);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
                Sets.newHashSet(HOST_11_OID));

        // "Host #11", "Datacenter #0", "VM-unplaced"
        assertThat(scopedTraderTOs.size(), equalTo(3));
    }

    /**
     * Test a scoped analysis run with the simple test topology.
     * Scoped to testScopeTopologyCluster1, which consists of the host HOST_12_OID,
     * there should be 10 SE's in the source and projected topology (see testScopeTopologyCluster1
     * above).
     *
     * @throws InterruptedException since we use sleep(.)
     * @throws CommunicationException should never occur
     */
    @Test
    public void testScopedMarketRunner() throws InterruptedException, CommunicationException {

        // Arrange
        MarketNotificationSender serverApi = mock(MarketNotificationSender.class);
        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        AnalysisFactory analysisFactory = mock(AnalysisFactory.class);
        WastedFilesAnalysisFactory wastedFilesAnalysisFactory = mock(WastedFilesAnalysisFactory.class);
        BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory = mock(BuyRIImpactAnalysisFactory.class);
        TopologyCostCalculatorFactory topologyCostCalculatorFactory = mock(TopologyCostCalculatorFactory.class);
        TopologyCostCalculator topologyCostCalculator = mock(TopologyCostCalculator.class);
        when(topologyCostCalculator.getCloudCostData()).thenReturn(CloudCostData.empty());
        TopologyEntityCloudTopologyFactory cloudTopologyFactory =
                mock(TopologyEntityCloudTopologyFactory.class);
        when(cloudTopologyFactory.newCloudTopology(any())).thenReturn(mock(TopologyEntityCloudTopology.class));
        when(topologyCostCalculatorFactory.newCalculator(PLAN_TOPOLOGY_INFO, any())).thenReturn(topologyCostCalculator);
        MarketRunner runner = new MarketRunner(threadPool, serverApi, analysisFactory, Optional.empty(), passthroughGate, initialPlacementFinder);

        long topologyContextId = 1000;
        long topologyId = 2000;
        long creationTime = 3000;

        TopologyDTO.TopologyInfo topologyInfo = TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContextId)
                .setCreationTime(creationTime)
                .setTopologyType(TopologyDTO.TopologyType.PLAN)
                .addScopeSeedOids(HOST_12_OID)
                .build();

        Set<TopologyEntityDTO> topologyDTOs = topologyDTOBuilderSet.stream()
            .map(dtoBuilder -> dtoBuilder.build()).collect(Collectors.toSet());

        // Act
        AnalysisConfig.Builder configBuilder = AnalysisConfig.newBuilder(MarketAnalysisUtils.QUOTE_FACTOR,
            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR, SuspensionsThrottlingConfig.DEFAULT,
            Collections.emptyMap());
        when(analysisFactory.newAnalysis(eq(topologyInfo), eq(topologyDTOs), any(), any()))
            .thenAnswer(invocation -> {
                AnalysisConfigCustomizer configCustomizer =
                        invocation.getArgumentAt(2, AnalysisConfigCustomizer.class);
                configCustomizer.customize(configBuilder);

                final MarketPriceTableFactory priceTableFactory = mock(MarketPriceTableFactory.class);
                when(priceTableFactory.newPriceTable(any(), any())).thenReturn(mock(MarketPriceTable.class));
                when(topologyCostCalculatorFactory.newCalculator(any(), any())).thenReturn(topologyCostCalculator);
                return new Analysis(topologyInfo, topologyDTOs,
                        new GroupMemberRetriever(groupServiceClient), Clock.systemUTC(),
                        configBuilder.build(), cloudTopologyFactory, topologyCostCalculatorFactory,
                        priceTableFactory, wastedFilesAnalysisFactory, buyRIImpactAnalysisFactory,
                        tierExcluderFactory, mock(AnalysisRICoverageListener.class),
                        consistentScalingHelperFactory, initialPlacementFinder,
                        reversibilitySettingFetcherFactory);
            });

        Analysis analysis = runner.scheduleAnalysis(topologyInfo, topologyDTOs,
            Tracing.trace("test").spanContext(), true,
            Optional.empty(), false, false, rightsizeLowerWatermark, rightsizeUpperWatermark,
            discountedComputeCostFactor);

        assertThat(analysis.getConfig().getRightsizeLowerWatermark(), is(rightsizeLowerWatermark));
        assertThat(analysis.getConfig().getRightsizeUpperWatermark(), is(rightsizeUpperWatermark));
        assertThat(analysis.getConfig().getIncludeVdc(), is(true));

        assertTrue(runner.getRuns().contains(analysis));
        while (!analysis.isDone()) {
            Thread.sleep(1000);
        }
        assertSame("Plan completed with an error : " + analysis.getErrorMsg(),
                AnalysisState.SUCCEEDED, analysis.getState());

        // Assert
        // wait for the action broadcast to complete
        Thread.sleep(1000);
        assertTrue(analysis.getActionPlan().isPresent());
        Mockito.verify(serverApi, Mockito.times(1)).notifyActionsRecommended(analysis.getActionPlan().get());

        // since the IDgenerator gives us a different projectedTopoID every time, we create a
        // MockitoMatcher using anyLong to represent this parameter
        Mockito.verify(serverApi, Mockito.times(1))
                .notifyProjectedTopology(eq(topologyInfo), anyLong(),
                        eq(analysis.getProjectedTopology().get()),
                        eq(analysis.getActionPlan().get().getId()));

        // check the original topology size
        assertThat(analysis.getOriginalInputTopology().size(), equalTo(topologyDTOs.size()));

        // check projected topology -  "Host #12", "VM #14", "App #15", "Datastore #2,4,6,8,10",
        // "DiskArray #1-10", "Datacenter #0"
        assertThat(analysis.getProjectedTopology().isPresent(), equalTo(true));
        assertThat(analysis.getProjectedTopology().get().size(), equalTo(CLUSTER_1_EXPANDED_SE_COUNT));
    }

    /**
     * Test a scoped topology with Cluster1 with a host in Maintenance mode.
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeTopologyCluster1WithHostInMaintenance() throws InvalidTopologyException {

        TopologyEntityDTO.Builder host12Builder = topologyDTOBuilderSet.stream().filter(dto -> dto.getOid() == HOST_12_OID)
            .findFirst().get();
        host12Builder.getAnalysisSettingsBuilder().setIsAvailableAsProvider(false);
        host12Builder.getAnalysisSettingsBuilder().setCloneable(false);
        host12Builder.setEntityState(EntityState.MAINTENANCE);

        final TopologyConverter converter =
            new TopologyConverter(PLAN_TOPOLOGY_INFO, marketPriceTable, ccd,
                CommodityIndex.newFactory(), tierExcluderFactory, consistentScalingHelperFactory,
                reversibilitySettingFetcher);
        final Set<EconomyDTOs.TraderTO> traderTOs = convertToMarket(converter);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
            Sets.newHashSet(HOST_12_OID));

        // Even though host is in "MAINTENANCE" still below entities should be in scope.
        // "Host #12", "VM #14", "App #15", "Datastore #2,4,6,8,10", "DiskArray #1-10", "Datacenter #0"
        assertThat(scopedTraderTOs.size(), equalTo(CLUSTER_1_EXPANDED_SE_COUNT));
    }

    /**
     * Test scope with VM as seed for scope.
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeWithVMSeed() throws InvalidTopologyException {

        final TopologyConverter converter =
            new TopologyConverter(PLAN_TOPOLOGY_INFO, marketPriceTable, ccd,
                CommodityIndex.newFactory(), tierExcluderFactory, consistentScalingHelperFactory,
                reversibilitySettingFetcher);
        final Set<EconomyDTOs.TraderTO> traderTOs = convertToMarket(converter);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
            Sets.newHashSet(VM_14_OID));

        // Scope to VM and below entities should be in scope.
        // "Host #12", "VM #14", "App #15", "Datastore #2,4,6,8,10", "DiskArray #1-10", "Datacenter #0"
        assertThat(scopedTraderTOs.size(), equalTo(CLUSTER_1_EXPANDED_SE_COUNT));
    }

    /**
     * Test scope with VM Seed On Maintenance Host.
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeWithVMSeedOnMaintenanceHost() throws InvalidTopologyException {

        TopologyEntityDTO.Builder host12Builder = topologyDTOBuilderSet.stream().filter(dto -> dto.getOid() == HOST_12_OID)
            .findFirst().get();
        host12Builder.getAnalysisSettingsBuilder().setIsAvailableAsProvider(false);
        host12Builder.getAnalysisSettingsBuilder().setCloneable(false);
        host12Builder.setEntityState(EntityState.MAINTENANCE);

        final TopologyConverter converter =
            new TopologyConverter(PLAN_TOPOLOGY_INFO, marketPriceTable, ccd,
                CommodityIndex.newFactory(), tierExcluderFactory, consistentScalingHelperFactory,
                reversibilitySettingFetcher);
        final Set<EconomyDTOs.TraderTO> traderTOs = convertToMarket(converter);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
            Sets.newHashSet(VM_14_OID));

        // Scope to VM and below entities should be in scope.
        // "Host #12", "VM #14", "App #15", "Datastore #2,4,6,8,10", "DiskArray #1-10", "Datacenter #0"
        assertThat(scopedTraderTOs.size(), equalTo(CLUSTER_1_EXPANDED_SE_COUNT));
    }

    private Set<EconomyDTOs.TraderTO> convertToMarket(TopologyConverter converter) {
        return converter.convertToMarket(topologyDTOBuilderSet.stream()
            .map(TopologyEntityDTO.Builder::build)
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())));
    }

    /**
     * "App #15": 72207427031437 buys from "VM #14": 72207427031425.
     * "VM #14": 72207427031425, buys from:
     * <ul>
     * <li>"Host #12": 72207427031409 - CLUSTER("CLUSTER-1"), CPU, MEM, NET_THROUGHPUT
     * <li>"Datastore #6": 72207427031433 - STORAGE_CLUSTER("STORAGE-CLUSTER-1"), STORAGE_AMOUNT, DRS_SEGMENTATION
     * </ul>
     * from "Host #12": 72207427031409, "Datastore #6": 72207427031433
     * "Host #11": 72207427031424, buys from "Datacenter #0": 72207427031436
     * "Host #12": 72207427031409, buys from "Datacenter #0": 72207427031436
     * "Host #13": 72207427031408, buys from "Datacenter #0": 72207427031436
     * {Host #13 hosts VM #13 which hosts BApp #1}
     * "Datastore #1": 72207427031430,
     * "Datastore #2": 72207427031428
     * "Datastore #3": 72207427031429
     * "Datastore #4": 72207427031426
     * "Datastore #5": 72207427031427
     * "Datastore #6": 72207427031433
     * "Datastore #7": 72207427031434
     * "Datastore #8": 72207427031431
     * "Datastore #9": 72207427031432
     * "Datastore #10": 72207427031435
     * "Datacenter #0": 72207427031436
     *--
     * sellers of CLUSTER("CLUSTER-0") = "Host #11", "Host #13"
     * sellers of CLUSTER("CLUSTER-1") = "Host #12"
     *--
     * sellers of STORAGE_CLUSTER("STORAGE-CLUSTER-0") = Datastore #1,3,5,7,9
     * sellers of STORAGE_CLUSTER("STORAGE-CLUSTER-1") = Datastore #2,4,6,8,10
     *--
     * the disk arrays are not listed here, but "Datastore #n" buys EXTENT("DiskArray #n")
     *
     * @return a Set of TopologyentityDTO Protobufs read from the file "simple-topology.json"
     * @throws FileNotFoundException if the test file is not found
     * @throws InvalidProtocolBufferException if the JSON file has the wrong format
     */
    private Set<TopologyEntityDTO.Builder> readTopologyFromJsonFile()
            throws FileNotFoundException, InvalidProtocolBufferException {

        topologyDTOBuilderSet = new HashSet<>();
        File file = ResourcePath.getTestResource(getClass(), simpleTopologyjSonFile).toFile();
        final InputStream dtoInputStream = new FileInputStream(file);
        InputStreamReader inputStreamReader = new InputStreamReader(dtoInputStream);
        JsonReader topologyReader = new JsonReader(inputStreamReader);
        List<Object> dtos = GSON.fromJson(topologyReader, List.class);

        for (Object dto : dtos) {
            String dtoString = GSON.toJson(dto);
            TopologyEntityDTO.Builder entityDtoBuilder =
                    TopologyEntityDTO.newBuilder();
            JsonFormat.parser().merge(dtoString, entityDtoBuilder);
            topologyDTOBuilderSet.add(entityDtoBuilder);
        }
        return topologyDTOBuilderSet;
    }

}
