package com.vmturbo.market.diagnostics;

import static org.junit.Assert.assertEquals;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
import com.vmturbo.common.protobuf.plan.ReservationDTOMoles.ReservationServiceMole;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.cloudscaling.sma.analysis.StableMarriageAlgorithm;
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
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.SerializationDTOs.TraderDiagsTO;
import com.vmturbo.platform.analysis.topology.Topology;

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
    // To run your own diagnostics, change this to the location of your unzipped analysis diags.
    // For ex., change it to "/Users/thiru_arun/Downloads/analysisDiags-777777-73588629312080"
    private final String unzippedAnalysisDiagsLocation = "target/test-classes/analysisDiags";

    //Change this to the location of unzipped analysis diags.
    private final String unzippedSMADiagsLocation = "target/test-classes/cloudvmscaling/smaDiags";
    private final String unzippedSMADiagsLocation2 = "target/test-classes/cloudvmscaling/smaDiags2";
    private final String unzippedInitialPlacementDiagsLocation = "target/test-classes/initialPlacementDiags";

    /**
     * run the InitialPlacement from diags.
     */
    @Test
    public void testInitialPlacementFromDiags() {
        List<InitialPlacementCommTypeMap> historicalCachedCommType = new ArrayList<>();
        List<InitialPlacementCommTypeMap> realtimeCachedCommType = new ArrayList<>();
        List<InitialPlacementBuyer> newBuyers = new ArrayList<>();
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
                    case AnalysisDiagnosticsCollector.HISTORICAL_CACHED_ECONOMY_NAME:
                        fi = new FileInputStream(new File(path.toString()));
                        oi = new ObjectInputStream(fi);
                        // Read objects
                        historicalCachedEconomy = (Economy)(oi.readObject());
                        oi.close();
                        fi.close();
                        break;
                    case AnalysisDiagnosticsCollector.REALTIME_CACHED_ECONOMY_NAME:
                        fi = new FileInputStream(new File(path.toString()));
                        oi = new ObjectInputStream(fi);
                        // Read objects
                        realtimeCachedEconomy = (Economy)(oi.readObject());
                        oi.close();
                        fi.close();
                        break;
                    case AnalysisDiagnosticsCollector.HISTORICAL_CACHED_COMMTYPE_NAME:
                        historicalCachedCommType = extractMultipleInstancesOfType(path, InitialPlacementCommTypeMap.class);
                        break;
                    case AnalysisDiagnosticsCollector.REALTIME_CACHED_COMMTYPE_NAME:
                        realtimeCachedCommType = extractMultipleInstancesOfType(path, InitialPlacementCommTypeMap.class);
                        break;
                    case AnalysisDiagnosticsCollector.NEW_BUYERS_NAME:
                        newBuyers = extractMultipleInstancesOfType(path, InitialPlacementBuyer.class);
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
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final ReservationServiceMole testReservationService = spy(new ReservationServiceMole());
        GrpcTestServer grpcServer = GrpcTestServer.newServer(testReservationService);
        try {
            grpcServer.start();
        } catch (IOException e) {
            logger.error("Could not start grpcServer due to exception {}", e);
        }
        ReservationServiceBlockingStub reservationServiceBlockingStub =
                ReservationServiceGrpc.newBlockingStub(grpcServer.getChannel());
        InitialPlacementFinder pf = new InitialPlacementFinder(executorService,
                reservationServiceBlockingStub,
                true, 1);
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
        Table<Long, Long, InitialPlacementFinderResult> result = pf.findPlacement(newBuyers);
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
            smaInput.get().getContexts().stream().forEach(a -> a.decompress());
            SMAOutput smaOutput = StableMarriageAlgorithm.execute(smaInput.get());
            logger.info("SMA generated {} outputContexts", smaOutput.getContexts().size());
            assertTrue(getActionCount(smaOutput) > 0);
            computeSaving(smaOutput);
        } else {
            logger.error("Could not create SMAInput. SMA was not run.");
        }
    }

    /**
     * Compute the savings obtained after SMA.
     *
     * @param smaOutput the topology of interest.
     */
    public void computeSaving(SMAOutput smaOutput) {
        float saving = 0.0f;
        for (SMAOutputContext outputContext : smaOutput.getContexts()) {
            for (SMAMatch smaMatch : outputContext.getMatches()) {
                SMAVirtualMachine virtualMachine = smaMatch.getVirtualMachine();
                float currentCost = virtualMachine.getCurrentTemplate().getNetCost(
                        virtualMachine.getCostContext(), virtualMachine.getCurrentRICoverage());
                float projectedCost = smaMatch.getTemplate().getNetCost(
                        virtualMachine.getCostContext(), smaMatch.getDiscountedCoupons());
                saving += currentCost - projectedCost;
            }
        }
        logger.info("Savings: {}", saving);
    }

    /**
     * I run the SMA with the SMA diags. I get the output contexts…
     * I update the current template, and current coverage of input vms with the info from outputcontext..
     * (this step is like simulating the customer actually executing the move).
     * We then run SMA again..This time ideally there should be 0 actions
     */
    @Test
    public void testStabilityWithDiags() {
        restoreSMAsMembers(unzippedSMADiagsLocation2);
        if (smaInput.isPresent()) {
            smaInput.get().getContexts().stream().forEach(a -> a.decompress());
            SMAOutput smaOutput = StableMarriageAlgorithm.execute(smaInput.get());
            List<SMAInputContext> newInputContexts = new ArrayList<>();
            for (SMAOutputContext outputContext : smaOutput.getContexts()) {
                for (SMAInputContext inputContext : smaInput.get().getContexts()) {
                    if (outputContext.getContext().equals(inputContext.getContext())) {
                        List<SMAVirtualMachine> smaVirtualMachines = outputContext.getMatches()
                                .stream().map(a -> a.getVirtualMachine()).collect(Collectors.toList());
                        List<SMAVirtualMachine> newVirtualMachines = new ArrayList<>();
                        for (int i = 0; i < smaVirtualMachines.size(); i++) {
                            SMAVirtualMachine oldVM = smaVirtualMachines.get(i);
                            SMAVirtualMachine smaVirtualMachine = new SMAVirtualMachine(oldVM.getOid(),
                                    oldVM.getName(),
                                    oldVM.getGroupName(),
                                    oldVM.getBusinessAccountId(),
                                    outputContext.getMatches().get(i).getTemplate(),
                                    oldVM.getProviders(),
                                    outputContext.getMatches().get(i).getDiscountedCoupons(),
                                    oldVM.getZoneId(),
                                    outputContext.getMatches().get(i).getReservedInstance(),
                                    oldVM.getOsType(),
                                    oldVM.getOsLicenseModel());
                            newVirtualMachines.add(smaVirtualMachine);
                        }
                        SMAContext context = inputContext.getContext();
                        List<SMAReservedInstance> newReservedInstances = new ArrayList<>();
                        List<SMAReservedInstance> oldReservedInstances = inputContext.getReservedInstances();
                        for (int i = 0; i < oldReservedInstances.size(); i++) {
                            SMAReservedInstance oldRI = oldReservedInstances.get(i);
                            SMAReservedInstance newRI = SMAReservedInstance.copyFrom(oldRI);
                            newReservedInstances.add(newRI);
                        }
                        newInputContexts.add(new SMAInputContext(context, newVirtualMachines,
                                newReservedInstances, inputContext.getTemplates(), inputContext.getSmaConfig()));
                    }
                }
            }
            SMAOutput newOutput = StableMarriageAlgorithm
                    .execute(new SMAInput(newInputContexts));
            assertEquals(0, getActionCount(newOutput));
        }
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
            commSpecsToAdjustOverhead);
        Analysis analysis = createAnalysis();
        Ede ede = new Ede();
        AnalysisResults results = TopologyEntitiesHandler.performAnalysis(
            topologyInfo.get(), analysisConfig.get(), analysis, topology, ede);
        logger.info("Analysis generated {} actions", results.getActionsList().size());

        assertTrue(results.getActionsList().size() > 0);
    }

    private Analysis createAnalysis() {
        Analysis analysis = mock(Analysis.class);
        when(analysis.isStopAnalysis()).thenReturn(false);
        ReplayActions restoredReplayActions = new ReplayActions(replayActions,
            ImmutableList.copyOf(replayDeactivateActions));
        when(analysis.getReplayActions()).thenReturn(restoredReplayActions);
        return analysis;
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
                    case AnalysisDiagnosticsCollector.SMA_RESERVED_INSTANCE_PREFIX:
                        reservedInstanceList.put(index, extractMultipleInstancesOfType(path, SMAReservedInstance.class));
                        break;
                    case AnalysisDiagnosticsCollector.SMA_CONTEXT_PREFIX:
                        Optional<SMAContext> context = extractSingleInstanceOfType(path, SMAContext.class);
                        if (!context.isPresent()) {
                            smaInput = Optional.empty();
                            logger.error("Could not create SMAInput. Context is absent.");
                            return;
                        }
                        contextList.put(index, context.get());
                        break;
                    case AnalysisDiagnosticsCollector.SMA_CONFIG_PREFIX:
                        Optional<SMAConfig> config = extractSingleInstanceOfType(path, SMAConfig.class);
                        if (!config.isPresent()) {
                            smaInput = Optional.empty();
                            logger.error("Could not create SMAInput. config is absent.");
                            return;
                        }
                        configList.put(index, config.get());
                        break;
                    case AnalysisDiagnosticsCollector.SMA_TEMPLATE_PREFIX:
                        templateList.put(index, extractMultipleInstancesOfType(path, SMATemplate.class));
                        break;
                    case AnalysisDiagnosticsCollector.SMA_VIRTUAL_MACHINE_PREFIX:
                        virtualMachineList.put(index, extractMultipleInstancesOfType(path, SMAVirtualMachine.class));
                        break;
                    default:
                        logger.error("Unknown file {} in Analysis diags. Skipping this file.", fileName);
                        break;
                }
            }
            for (Integer index : contextList.keySet()) {
                if (contextList.get(index) == null || virtualMachineList.get(index) == null
                        || reservedInstanceList.get(index) == null
                        || templateList.get(index) == null) {
                    smaInput = Optional.empty();
                    logger.error("Could not create SMAInput.");
                    return;
                }
                if (configList.get(index) == null) {
                    smaInputContexts.add(new SMAInputContext(contextList.get(index),
                            virtualMachineList.get(index),
                            reservedInstanceList.get(index), templateList.get(index)));
                } else {
                    smaInputContexts.add(new SMAInputContext(contextList.get(index),
                            virtualMachineList.get(index),
                            reservedInstanceList.get(index),
                            templateList.get(index), configList.get(index)));
                }
            }

            smaInput = Optional.of(new SMAInput(smaInputContexts));

        } catch (Exception e) {
            logger.error("Could not extract from file {}.", unzippedSMADiagsLocation, e);
        }
    }

    private void restoreAnalysisMembers(String unzippedAnalysisDiagsLocation) {
        try {
            Iterator<Path> paths = Files.walk(Paths.get(unzippedAnalysisDiagsLocation), 1)
                .filter(Files::isRegularFile).iterator();
            while (paths.hasNext()) {
                Path path = paths.next();
                String fileName = path.getFileName().toString();
                switch (fileName) {
                    case AnalysisDiagnosticsCollector.TRADER_DIAGS_FILE_NAME:
                        try (FileInputStream fi = new FileInputStream(new File(path.toString()))) {
                            traderTOs = TraderDiagsTO.parseFrom(fi).getTraderTOsList();
                        }
                        break;
                    case AnalysisDiagnosticsCollector.ANALYSIS_CONFIG_DIAGS_FILE_NAME:
                        analysisConfig = extractSingleInstanceOfType(path, AnalysisConfig.class);
                        break;
                    case AnalysisDiagnosticsCollector.TOPOLOGY_INFO_DIAGS_FILE_NAME:
                        topologyInfo = extractSingleInstanceOfType(path, TopologyInfo.class);
                        break;
                    case AnalysisDiagnosticsCollector.ADJUST_OVERHEAD_DIAGS_FILE_NAME:
                        commSpecsToAdjustOverhead = extractMultipleInstancesOfType(path, CommoditySpecification.class);
                        break;
                    default:
                        logger.error("Unknown file {} in Analysis diags. Skipping this file.", fileName);
                        break;
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