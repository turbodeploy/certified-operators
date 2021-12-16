package com.vmturbo.market.diagnostics;

import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ADJUST_OVERHEAD_DIAGS_FILE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ANALYSIS_CONFIG_DIAGS_FILE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.HISTORICAL_CACHED_COMMTYPE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.HISTORICAL_CACHED_ECONOMY_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.NEW_BUYERS_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.REALTIME_CACHED_COMMTYPE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.REALTIME_CACHED_ECONOMY_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_CONFIG_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_CONTEXT_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_RESERVED_INSTANCE_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_TEMPLATE_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_VIRTUAL_MACHINE_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.TOPOLOGY_INFO_DIAGS_FILE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.TRADER_DIAGS_FILE_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.Gson;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.cloudscaling.sma.analysis.StableMarriageAlgorithm;
import com.vmturbo.market.cloudscaling.sma.entities.SMACloudCostCalculator;
import com.vmturbo.market.cloudscaling.sma.entities.SMAConfig;
import com.vmturbo.market.cloudscaling.sma.entities.SMAContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.InitialPlacementCommTypeMap;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.market.reservations.InitialPlacementFinderResult;
import com.vmturbo.market.reservations.InitialPlacementUtils;
import com.vmturbo.market.runner.Analysis;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.topology.TopologyEntitiesHandler;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.SerializationDTOs.TraderDiagsTO;
import com.vmturbo.platform.analysis.topology.Topology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit test which restores analysis diags.
 */
public class AnalysisDiagnosticsCollectorTest {

    private static final Logger logger = LogManager.getLogger();
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private Collection<TraderTO> traderTOs;
    private Optional<AnalysisConfig> analysisConfig = Optional.empty();
    private Optional<TopologyInfo> topologyInfo = Optional.empty();
    private Optional<SMAInput> smaInput = Optional.empty();
    private List<CommoditySpecification> commSpecsToAdjustOverhead = new ArrayList<>();
    private List<Action> replayActions = new ArrayList<>();
    private List<Deactivate> replayDeactivateActions = new ArrayList<>();
    // This is used in testCheckBalanceAfterAnalysisFromDiags. When running the test, please ensure the directory exists.
    // For ex., change it to "/Users/user_name/Downloads/FileName.xlsx"
    final String workbookOutputPath = "target/test-classes/FileName.xlsx";
    // To run your own diagnostics, change this to the location of your unzipped analysis diags.
    // For ex., change it to "/Users/user_name/Downloads/analysisDiags-777777-73588629312080"
    private final String unzippedAnalysisDiagsLocation = "target/test-classes/analysisDiags";
    //Change this to the location of unzipped SMA diags.
    private final String unzippedSMADiagsLocation = "target/test-classes/cloudvmscaling/smaDiags";
    private final String unzippedInitialPlacementDiagsLocation = "";

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(FeatureFlags.NAMESPACE_QUOTA_RESIZING);

    /**
     * Initializes traderTOs to empty list.
     */
    @Before
    public void setUp() {
        traderTOs = new ArrayList<>();
    }

    /**
     * run the InitialPlacement from diags.
     */
    @Test
    @Ignore
    public void testInitialPlacementFromDiags() {
        List<InitialPlacementCommTypeMap> historicalCachedCommType = new ArrayList<>();
        List<InitialPlacementCommTypeMap> realtimeCachedCommType = new ArrayList<>();
        List<InitialPlacementDTO> newBuyers = new ArrayList<>();
        FileInputStream fi;
        ObjectInputStream oi;
        Economy historicalCachedEconomy = null;
        Economy realtimeCachedEconomy = null;
        try {
            Iterator<Path> paths = Files.walk(Paths.get(unzippedInitialPlacementDiagsLocation), 1)
                    .filter(Files::isRegularFile).iterator();
            while (paths.hasNext()) {
                Path path = paths.next();
                String fileName = path.getFileName().toString();
                switch (fileName) {
                    case HISTORICAL_CACHED_ECONOMY_NAME:
                        fi = new FileInputStream(new File(path.toString()));
                        oi = new ObjectInputStream(fi);
                        // Read objects
                        historicalCachedEconomy = (Economy)(oi.readObject());
                        oi.close();
                        fi.close();
                        break;
                    case REALTIME_CACHED_ECONOMY_NAME:
                        fi = new FileInputStream(new File(path.toString()));
                        oi = new ObjectInputStream(fi);
                        // Read objects
                        realtimeCachedEconomy = (Economy)(oi.readObject());
                        oi.close();
                        fi.close();
                        break;
                    case HISTORICAL_CACHED_COMMTYPE_NAME:
                        historicalCachedCommType = extractMultipleInstancesOfType(path, InitialPlacementCommTypeMap.class);
                        break;
                    case REALTIME_CACHED_COMMTYPE_NAME:
                        realtimeCachedCommType = extractMultipleInstancesOfType(path, InitialPlacementCommTypeMap.class);
                        break;
                    case NEW_BUYERS_NAME:
                        newBuyers = extractMultipleInstancesOfType(path, InitialPlacementDTO.class);
                        break;
                    default:
                        logger.error("Unknown file {} in Analysis diags. Skipping this file.", fileName);
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("Could not extract from file {}.", unzippedAnalysisDiagsLocation, e);
        }
        if (realtimeCachedEconomy == null) {
            logger.error("Could not find realtimeCachedEconomy");
            return;
        }
        final ReservationServiceMole testReservationService = spy(new ReservationServiceMole());
        GrpcTestServer grpcServer = GrpcTestServer.newServer(testReservationService);
        try {
            grpcServer.start();
        } catch (IOException e) {
            logger.error("Could not start grpcServer due to exception {}", e);
        }
        ReservationServiceBlockingStub reservationServiceBlockingStub =
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel());
        InitialPlacementFinder pf = new InitialPlacementFinder(Mockito.mock(DSLContext.class),
                reservationServiceBlockingStub,
                true, 1, 5);
        BiMap<CommodityType, Integer> realtimeCachedCommTypeMap = HashBiMap.create();
        BiMap<CommodityType, Integer> historicalCachedCommTypeMap = HashBiMap.create();
        realtimeCachedCommType.stream().forEach(entry -> realtimeCachedCommTypeMap.put(entry.commodityType, entry.type));
        historicalCachedCommType.stream().forEach(entry -> historicalCachedCommTypeMap.put(entry.commodityType, entry.type));
        pf.getEconomyCaches().getState().setReservationReceived(true);
        pf.getEconomyCaches().setEconomiesAndCachedCommType(historicalCachedCommTypeMap,
                realtimeCachedCommTypeMap,
                historicalCachedEconomy == null ? null : InitialPlacementUtils.cloneEconomy(
                        historicalCachedEconomy, true),
                realtimeCachedEconomy == null ? null : InitialPlacementUtils.cloneEconomy(
                        realtimeCachedEconomy, true));
        FindInitialPlacementRequest.Builder findInitialPlacementRequest = FindInitialPlacementRequest.newBuilder();
        findInitialPlacementRequest.addAllInitialPlacement(newBuyers);
        Table<Long, Long, InitialPlacementFinderResult> result
            = pf.findPlacement(findInitialPlacementRequest.build());
        assertFalse(result.isEmpty());
    }


    /**
     * Unit test to run SMA from the unzipped analysis diags.
     * Steps to run this unit test:
     * 1. Unzip your sma diags,
     * 2. Change the variable unzippedSMADiagsLocation.
     * 3. Run the unit test.
     */
    @Test
    public void testRunSMAFromDiags() {
        restoreSMAsMembers(unzippedSMADiagsLocation);
        if (smaInput.isPresent()) {
            SMAOutput smaOutput = StableMarriageAlgorithm.execute(smaInput.get());
            logger.info("SMA generated {} outputContexts", smaOutput.getContexts().size());
            assertTrue(getActionCount(smaOutput) > 0);
            computeSaving(smaOutput, smaInput.get().getCloudCostCalculator());
        } else {
            logger.error("Could not create SMAInput. SMA was not run.");
        }
    }

    /**
     * Compute the savings obtained after SMA.
     *
     * @param smaOutput the topology of interest.
     * @param cloudCostCalculator cloud cost calculator to compute saving
     */
    public void computeSaving(SMAOutput smaOutput, SMACloudCostCalculator cloudCostCalculator) {
        float saving = 0.0f;
        float investment = 0.0f;
        float netSaving = 0.0f;
        for (SMAOutputContext outputContext : smaOutput.getContexts()) {
            for (SMAMatch smaMatch : outputContext.getMatches()) {
                SMAVirtualMachine virtualMachine = smaMatch.getVirtualMachine();
                float currentCost = cloudCostCalculator.getNetCost(
                        virtualMachine.getCostContext(), virtualMachine.getCurrentRICoverage(), virtualMachine.getCurrentTemplate());
                float projectedCost = cloudCostCalculator.getNetCost(
                        virtualMachine.getCostContext(), smaMatch.getDiscountedCoupons(), smaMatch.getTemplate());
                if (currentCost - projectedCost > 0) {
                    saving += currentCost - projectedCost;
                } else {
                    investment += currentCost - projectedCost;
                }
                netSaving += currentCost - projectedCost;
            }
        }
        logger.info("netSaving: {} , investment: {} , saving: {}", netSaving, investment, saving);
    }


    /**
     * find the number of actions.
     *
     * @param smaOutput outputcontext
     * @return number of actions.
     */
    private int getActionCount(SMAOutput smaOutput) {
        int actionCount = 0;
        for (SMAOutputContext outputContext : smaOutput.getContexts()) {
            for (SMAMatch match : outputContext.getMatches()) {
                if ((match.getVirtualMachine().getCurrentTemplate().getOid() != match.getTemplate().getOid())
                        || (Math.abs(match.getVirtualMachine().getCurrentRICoverage()
                        - match.getDiscountedCoupons()) > SMAUtils.EPSILON)) {
                    actionCount++;
                }
            }
        }
        return actionCount;
    }

    /**
     * Unit test to run analysis from the unzipped analysis diags.
     * Steps to run this unit test:
     * 1. Unzip your analysis diags,
     * 2. Change the variable unzippedAnalysisDiagsLocation.
     * 3. Run the unit test.
     */
    @Test
    public void testRunAnalysisFromDiags() {
        IdentityGenerator.initPrefix(9L);
        restoreAnalysisMembers(unzippedAnalysisDiagsLocation);
        assertFalse(traderTOs.isEmpty());
        assertTrue(analysisConfig.isPresent());
        assertTrue(topologyInfo.isPresent());
        assertFalse(commSpecsToAdjustOverhead.isEmpty());

        Topology topology = TopologyEntitiesHandler.createTopology(traderTOs, topologyInfo.get(),
            commSpecsToAdjustOverhead, analysisConfig.get());
        Analysis analysis = createAnalysis();
        Ede ede = new Ede();
        AnalysisResults results = TopologyEntitiesHandler.performAnalysis(
            topologyInfo.get(), analysisConfig.get(), analysis, topology, ede);
        logger.info("Analysis generated {} actions", results.getActionsList().size());

        assertTrue(results.getActionsList().size() > 0);
    }

    /**
     * Test IsAnalysisDiagsSaveEnabled for various combinations of real time / plan / debug level /
     * numRealTimeAnalysisDiagsToRetain.
     */
    @Test
    public void testIsAnalysisDiagsSaveEnabled() {
        final TopologyInfo rtTopoInfo = TopologyInfo.newBuilder().setTopologyContextId(777777L).setTopologyId(1L).build();
        final TopologyInfo planTopoInfo = TopologyInfo.newBuilder().setTopologyContextId(777777L).setTopologyId(1L).setPlanInfo(
                PlanTopologyInfo.getDefaultInstance()).build();

        Configurator.setLevel(LogManager.getLogger(AnalysisDiagnosticsCollector.class).getName(), Level.DEBUG);
        assertTrue(AnalysisDiagnosticsCollector.isAnalysisDiagsSaveEnabled(rtTopoInfo, 5));
        assertTrue(AnalysisDiagnosticsCollector.isAnalysisDiagsSaveEnabled(rtTopoInfo, 0));
        assertTrue(AnalysisDiagnosticsCollector.isAnalysisDiagsSaveEnabled(planTopoInfo, 5));
        assertTrue(AnalysisDiagnosticsCollector.isAnalysisDiagsSaveEnabled(planTopoInfo, 0));

        Configurator.setLevel(LogManager.getLogger(AnalysisDiagnosticsCollector.class).getName(), Level.INFO);
        assertTrue(AnalysisDiagnosticsCollector.isAnalysisDiagsSaveEnabled(rtTopoInfo, 5));
        assertFalse(AnalysisDiagnosticsCollector.isAnalysisDiagsSaveEnabled(rtTopoInfo, 0));
        assertFalse(AnalysisDiagnosticsCollector.isAnalysisDiagsSaveEnabled(planTopoInfo, 5));
        assertFalse(AnalysisDiagnosticsCollector.isAnalysisDiagsSaveEnabled(planTopoInfo, 0));
    }

    /**<p>The test is not needed to be run during the build as it doesn't really test anything.
     * It produces utilization distribution per commodity. It can be run on an as-needed basis.</p>
     * <p>Unit test to check "balance" of the environment after analysis.
     * The test creates a spreadsheet with the utilization distribution of source and
     * projected topologies.</p>
     * <p>Utilization distribution is a mapping between the utilization of commodity sold (of an
     * entity type) rounded to nearest integer, and the number of entities having that utilization.
     * Steps to run this unit test:
     * 1. Unzip your analysis diags.
     * 2. Change the variable unzippedAnalysisDiagsLocation to the location of the unzipped diags.
     * 3. Create directory referred to by workbookOutputPath.
     * 4. Run the unit test.</p>
     */
    @Ignore
    @Test
    public void testCheckBalanceAfterAnalysisFromDiags() {
        IdentityGenerator.initPrefix(9L);
        restoreAnalysisMembers(unzippedAnalysisDiagsLocation);
        assertFalse(traderTOs.isEmpty());
        assertTrue(analysisConfig.isPresent());
        assertTrue(topologyInfo.isPresent());
        assertFalse(commSpecsToAdjustOverhead.isEmpty());

        Map<Integer, Map<String, UtilizationDistribution>> sourceUtilDistribution =
            UtilizationDistribution.createUtilizationDistribution(traderTOs);

        Topology topology = TopologyEntitiesHandler.createTopology(traderTOs, topologyInfo.get(),
            commSpecsToAdjustOverhead, analysisConfig.get());
        Analysis analysis = createAnalysis();
        Ede ede = new Ede();
        AnalysisResults results = TopologyEntitiesHandler.performAnalysis(
            topologyInfo.get(), analysisConfig.get(), analysis, topology, ede);

        Map<Integer, Map<String, UtilizationDistribution>> projectedUtilDistribution =
            UtilizationDistribution.createUtilizationDistribution(results.getProjectedTopoEntityTOList());
        new WorkbookHelper().createWorkbookWithUtilizationDistributions(sourceUtilDistribution,
            projectedUtilDistribution, workbookOutputPath);
    }

    private Analysis createAnalysis() {
        Analysis analysis = mock(Analysis.class);
        when(analysis.isStopAnalysis()).thenReturn(false);
        ReplayActions restoredReplayActions = new ReplayActions(replayActions,
            ImmutableList.copyOf(replayDeactivateActions));
        when(analysis.getReplayActions()).thenReturn(restoredReplayActions);
        when(analysis.getDiagnosticsCleaner()).thenReturn(new AnalysisDiagnosticsCleaner(10, 10, new DiagsFileSystem()));
        return analysis;
    }

    /**
     * Get economy stats from the unzipped analysis diags.
     * Steps to run this unit test:
     * 1. Unzip your analysis diags,
     * 2. Change the variable unzippedAnalysisDiagsLocation.
     * 3. Run the unit test.
     */
    @Test
    public void testGetEconomyStatsFromAnalysiDiags() {
        IdentityGenerator.initPrefix(9L);
        restoreAnalysisMembers(unzippedAnalysisDiagsLocation);
        assertFalse(traderTOs.isEmpty());
        assertTrue(analysisConfig.isPresent());
        assertTrue(topologyInfo.isPresent());
        assertFalse(commSpecsToAdjustOverhead.isEmpty());

        Topology topology = TopologyEntitiesHandler.createTopology(traderTOs, topologyInfo.get(),
            commSpecsToAdjustOverhead, analysisConfig.get());
        Economy economy = topology.getEconomyForTesting();
        economy.composeMarketSubsetForPlacement();

        logger.info("Total number of traders in economy = {}", economy.getTraders().size());
        logger.info("----------------------------------------------------------");
        Map<Integer, Set<Trader>> tradersByType = Maps.newHashMap();
        economy.getTraders().forEach(t ->
            tradersByType.computeIfAbsent(t.getType(), type -> new HashSet<>()).add(t));
        tradersByType.entrySet().stream()
            .sorted((e1, e2) -> Integer.compare(e2.getValue().size(), e1.getValue().size()))
            .forEach(e -> logger.info("Number of {}S = {}", EntityType.forNumber(e.getKey()), e.getValue().size()));
        Map<Integer, Set<Trader>> shopTogetherTradersByType = Maps.newHashMap();
        for (Trader t : economy.getTraders()) {
            if (t.getSettings().isShopTogether()) {
                shopTogetherTradersByType.computeIfAbsent(t.getType(), type -> new HashSet<>()).add(t);
            }
        }
        logger.info("----------------------------------------------------------");
        shopTogetherTradersByType.entrySet().stream()
            .sorted((e1, e2) -> Integer.compare(e2.getValue().size(), e1.getValue().size()))
            .forEach(e -> logger.info("Number of Shop Together {}S = {}", EntityType.forNumber(e.getKey()), e.getValue().size()));
        logger.info("----------------------------------------------------------");
        Set<ShoppingList> sls = economy.getMarkets().stream().map(m -> m.getBuyers())
            .flatMap(List::stream).collect(Collectors.toSet());
        long movableSlsCount = sls.stream().filter(ShoppingList::isMovable).count();
        logger.info("Total number of shopping lists = {}", sls.size());
        logger.info("Total number of movable shopping lists = {}", movableSlsCount);
        logger.info("----------------------------------------------------------");
        Map<Integer, Set<ShoppingList>> shoppingListsByType = Maps.newHashMap();
        sls.forEach(sl -> shoppingListsByType.computeIfAbsent(sl.getBuyer().getType(), type -> new HashSet<>()).add(sl));
        shoppingListsByType.entrySet().stream()
            .sorted((e1, e2) -> Integer.compare(e2.getValue().size(), e1.getValue().size()))
            .forEach(e -> logger.info("Number of shopping lists for {}S = {}", EntityType.forNumber(e.getKey()), e.getValue().size()));
        logger.info("----------------------------------------------------------");
        logger.info("Number of markets = {}", economy.getMarkets().size());
        logger.info("Number of markets for placement = {}", economy.getMarketsForPlacement().size());
        logger.info("----------------------------------------------------------");
        Set<Long> cliquesInMarketsForPlacement = economy.getMarketsForPlacement().stream().map(m -> m.getCliques().keySet())
            .flatMap(Set::stream).collect(Collectors.toSet());
        logger.info("Number of cliques in markets for placement = {}", cliquesInMarketsForPlacement.size());
        Set<Long> allCliques = economy.getMarkets().stream().map(m -> m.getCliques().keySet())
            .flatMap(Set::stream).collect(Collectors.toSet());
        logger.info("Total number of cliques in all markets = {}", allCliques.size());
        logger.info("----------------------------------------------------------");
        List<Integer> marketCliques = economy.getMarketsForPlacement().stream()
            .filter(m -> m.getCliques().size() > 0).map(m -> m.getCliques().size()).collect(Collectors.toList());
        logger.info("Max number of cliques in any market for placement = {}", marketCliques.stream().mapToInt(Integer::intValue).max().orElse(0));
        logger.info("Min number of cliques in any market for placement = {}", marketCliques.stream().mapToInt(Integer::intValue).min().orElse(0));
        logger.info("Avg number of cliques in a market for placement = {}", marketCliques.stream().mapToInt(Integer::intValue).average().orElse(0));
        logger.info("----------------------------------------------------------");
        Set<Trader> hosts = tradersByType.get(EntityType.PHYSICAL_MACHINE_VALUE);
        List<Integer> cliquesOfHosts = hosts.stream().map(h -> h.getCliques().size()).collect(Collectors.toList());
        logger.info("Max number of cliques any host is part of = {}", cliquesOfHosts.stream().mapToInt(Integer::intValue).max().orElse(0));
        logger.info("Min number of cliques any host is part of = {}", cliquesOfHosts.stream().mapToInt(Integer::intValue).min().orElse(0));
        logger.info("Avg number of cliques a host is part of = {}", cliquesOfHosts.stream().mapToInt(Integer::intValue).average().orElse(0));
        logger.info("----------------------------------------------------------");
        Set<Trader> storages = tradersByType.get(EntityType.STORAGE_VALUE);
        List<Integer> cliquesOfStorages = storages.stream().map(s -> s.getCliques().size()).collect(Collectors.toList());
        logger.info("Max number of cliques any storage is part of = {}", cliquesOfStorages.stream().mapToInt(Integer::intValue).max().orElse(0));
        logger.info("Min number of cliques any storage is part of = {}", cliquesOfStorages.stream().mapToInt(Integer::intValue).min().orElse(0));
        logger.info("Avg number of cliques a storage is part of = {}", cliquesOfStorages.stream().mapToInt(Integer::intValue).average().orElse(0));
        logger.info("----------------------------------------------------------");
        logger.info("Max number of markets any host sells in = {}", hosts.stream().mapToInt(h -> economy.getMarketsAsSeller(h).size()).max().orElse(0));
        logger.info("Min number of markets any host sells in = {}", hosts.stream().mapToInt(h -> economy.getMarketsAsSeller(h).size()).min().orElse(0));
        logger.info("Avg number of markets a host sells in = {}", hosts.stream().mapToInt(h -> economy.getMarketsAsSeller(h).size()).average().orElse(0));
        logger.info("----------------------------------------------------------");
        logger.info("Max number of markets any storage sells in = {}", storages.stream().mapToInt(s -> economy.getMarketsAsSeller(s).size()).max().orElse(0));
        logger.info("Min number of markets any storage sells in = {}", storages.stream().mapToInt(s -> economy.getMarketsAsSeller(s).size()).min().orElse(0));
        logger.info("Avg number of markets a storage sells in = {}", storages.stream().mapToInt(s -> economy.getMarketsAsSeller(s).size()).average().orElse(0));
        logger.info("----------------------------------------------------------");
        Map<Long, Set<Trader>> cliqueToHosts = Maps.newHashMap();
        Map<Long, Set<Trader>> cliqueToStorages = Maps.newHashMap();
        for (Market market : economy.getMarketsForPlacement()) {
            market.getCliques().forEach((clique, traders) -> {
                for (Trader t : traders) {
                    if (t.getType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                        cliqueToHosts.computeIfAbsent(clique, k -> new HashSet<>()).add(t);
                    } else if (t.getType() == EntityType.STORAGE_VALUE) {
                        cliqueToStorages.computeIfAbsent(clique, k -> new HashSet<>()).add(t);
                    }
                }
            });
        }
        List<Integer> numHostsInEachClique = cliqueToHosts.entrySet().stream().map(e -> e.getValue().size()).collect(Collectors.toList());
        List<Integer> numStoragesInEachClique = cliqueToStorages.entrySet().stream().map(e -> e.getValue().size()).collect(Collectors.toList());
        logger.info("Max number of Hosts in any clique = {}", numHostsInEachClique.stream().mapToInt(Integer::intValue).max().orElse(0));
        logger.info("Min number of Hosts in any clique = {}", numHostsInEachClique.stream().mapToInt(Integer::intValue).min().orElse(0));
        logger.info("Avg number of Hosts in a clique = {}", numHostsInEachClique.stream().mapToInt(Integer::intValue).average().orElse(0));
        logger.info("----------------------------------------------------------");
        logger.info("Max number of Storages in any clique = {}", numStoragesInEachClique.stream().mapToInt(Integer::intValue).max().orElse(0));
        logger.info("Min number of Storages in any clique = {}", numStoragesInEachClique.stream().mapToInt(Integer::intValue).min().orElse(0));
        logger.info("Avg number of Storages in a clique = {}", numStoragesInEachClique.stream().mapToInt(Integer::intValue).average().orElse(0));
    }

    /**
     * restore SMAInput from diags.
     * @param unzippedSMADiagsLocation diags location.
     */
    private void restoreSMAsMembers(String unzippedSMADiagsLocation) {
        try {
            Iterator<Path> paths = Files.walk(Paths.get(unzippedSMADiagsLocation), 1)
                    .filter(Files::isRegularFile).iterator();
            Map<Integer, List<SMAVirtualMachine>> virtualMachineList = new HashMap<>();
            Map<Integer, List<SMAReservedInstance>> reservedInstanceList = new HashMap<>();
            Map<Integer, List<SMATemplate>> templateList = new HashMap<>();
            Map<Integer, SMAContext> contextList = new HashMap<>();
            Map<Integer, SMAConfig> configList = new HashMap<>();
            List<SMAInputContext> smaInputContexts = new ArrayList<>();
            while (paths.hasNext()) {
                Path path = paths.next();
                String fileName = path.getFileName().toString();
                String[] splitStrings = fileName.split("_");
                if (splitStrings.length != 3) {
                    continue;
                }
                int index = Integer.parseInt(splitStrings[1]);
                switch (splitStrings[0]) {
                    case SMA_RESERVED_INSTANCE_PREFIX:
                        reservedInstanceList.put(index, extractMultipleInstancesOfType(path, SMAReservedInstance.class));
                        break;
                    case SMA_CONTEXT_PREFIX:
                        Optional<SMAContext> context = extractSingleInstanceOfType(path, SMAContext.class);
                        if (!context.isPresent()) {
                            smaInput = Optional.empty();
                            logger.error("Could not create SMAInput. Context is absent.");
                            return;
                        }
                        contextList.put(index, context.get());
                        break;
                    case SMA_CONFIG_PREFIX:
                        Optional<SMAConfig> config = extractSingleInstanceOfType(path, SMAConfig.class);
                        if (!config.isPresent()) {
                            smaInput = Optional.empty();
                            logger.error("Could not create SMAInput. config is absent.");
                            return;
                        }
                        configList.put(index, config.get());
                        break;
                    case SMA_TEMPLATE_PREFIX:
                        templateList.put(index, extractMultipleInstancesOfType(path, SMATemplate.class));
                        break;
                    case SMA_VIRTUAL_MACHINE_PREFIX:
                        virtualMachineList.put(index, extractMultipleInstancesOfType(path, SMAVirtualMachine.class));
                        break;
                    default:
                        logger.error("Unknown file {} in Analysis diags. Skipping this file.", fileName);
                        break;
                }
            }
            SMACloudCostCalculator cloudCostCalculator = new SMACloudCostCalculator();
            for (Integer index : contextList.keySet()) {
                if (contextList.get(index) == null || virtualMachineList.get(index) == null
                        || reservedInstanceList.get(index) == null
                        || templateList.get(index) == null || configList.get(index) == null) {
                    smaInput = Optional.empty();
                    logger.error("Could not create SMAInput.");
                    return;
                }
                SMAInputContext smaInputContext = new SMAInputContext(contextList.get(index),
                            virtualMachineList.get(index),
                            reservedInstanceList.get(index),
                            templateList.get(index), configList.get(index));
                smaInputContext.decompress(cloudCostCalculator);
                // this will initialize fields which are not set in json.
                smaInputContexts.add(new SMAInputContext(smaInputContext, cloudCostCalculator));
            }
            smaInput = Optional.of(new SMAInput(smaInputContexts, cloudCostCalculator));
        } catch (Exception e) {
            logger.error("Could not extract from file {}.", unzippedSMADiagsLocation, e);
        }
    }

    private void restoreAnalysisMembers(String unzippedAnalysisDiagsLocation) {
        try {
            Iterator<Path> paths = Files.walk(Paths.get(unzippedAnalysisDiagsLocation), 1)
                .filter(Files::isRegularFile).sorted().iterator();
            while (paths.hasNext()) {
                Path path = paths.next();
                String fileName = path.getFileName().toString();
                if (fileName.startsWith(TRADER_DIAGS_FILE_NAME)) {
                    try (FileInputStream fi = new FileInputStream(new File(path.toString()))) {
                        traderTOs.addAll(TraderDiagsTO.parseFrom(fi).getTraderTOsList());
                    }
                    logger.info("Successfully extracted {}", fileName);
                } else {
                    switch (fileName) {
                        case ANALYSIS_CONFIG_DIAGS_FILE_NAME:
                            analysisConfig = extractSingleInstanceOfType(path, AnalysisConfig.class);
                            break;
                        case TOPOLOGY_INFO_DIAGS_FILE_NAME:
                            topologyInfo = extractSingleInstanceOfType(path, TopologyInfo.class);
                            break;
                        case ADJUST_OVERHEAD_DIAGS_FILE_NAME:
                            commSpecsToAdjustOverhead = extractMultipleInstancesOfType(path, CommoditySpecification.class);
                            break;
                        default:
                            logger.error("Unknown file {} in Analysis diags. Skipping this file.", fileName);
                            break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Could not extract from file {}.", unzippedAnalysisDiagsLocation, e);
        }
    }

    private <T> List<T> extractMultipleInstancesOfType(Path diagsFile, Class<T> type) {
        List<T> instances = new ArrayList<>();
        try {
            Iterator<String> serializedInstances = Files.lines(diagsFile).iterator();
            int counter = 0;
            if (!serializedInstances.hasNext()) {
                logger.warn("No data present in {}. Could not extract {}s from it.", diagsFile.getFileName().toString(),
                    type.getSimpleName());
                return instances;
            }
            Stopwatch stopwatch = Stopwatch.createStarted();
            while (serializedInstances.hasNext()) {
                instances.add(GSON.fromJson(serializedInstances.next(), type));
                counter++;
                if (counter % 1000 == 0) {
                    logger.info("Extracted {} {}s", counter, type.getSimpleName());
                }
            }
            stopwatch.stop();
            logger.info("Successfully extracted {} {}s in {} seconds", instances.size(), type.getSimpleName(),
                stopwatch.elapsed(TimeUnit.SECONDS));
            return instances;
        } catch (IOException e) {
            logger.error("Could not extract {}s from {} : ", type.getSimpleName(), diagsFile.getFileName().toString(), e);
            return Collections.emptyList();
        }
    }

    private <T> Optional<T> extractSingleInstanceOfType(Path diagsFile, Class<T> type) {
        Optional<T> extractedInstance = Optional.empty();
        try {
            String serializedInformation = new String(
                Files.readAllBytes(diagsFile), CHARSET);
            if (serializedInformation != null && !serializedInformation.isEmpty()) {
                extractedInstance = Optional.of(GSON.fromJson(serializedInformation, type));
                logger.info("Successfully extracted {}", type.getSimpleName());
            } else {
                logger.warn("No data present in {}. Could not extract {}s from it.", diagsFile.getFileName().toString(),
                    type.getSimpleName());
            }
        } catch (IOException e) {
            logger.error("Could not extract data from {} : ", diagsFile.getFileName().toString(), e);
        }
        return extractedInstance;
    }
}