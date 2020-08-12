package com.vmturbo.market.diagnostics;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.market.runner.Analysis;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.topology.TopologyEntitiesHandler;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * Unit test which restores analysis diags.
 */
public class AnalysisDiagnosticsCollectorTest {

    private static final Logger logger = LogManager.getLogger();
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private List<TraderTO> traderTOs = new ArrayList<>();
    private Optional<AnalysisConfig> analysisConfig = Optional.empty();
    private Optional<TopologyInfo> topologyInfo = Optional.empty();
    private List<CommoditySpecification> commSpecsToAdjustOverhead = new ArrayList<>();
    private List<Action> replayActions = new ArrayList<>();
    private List<Deactivate> replayDeactivateActions = new ArrayList<>();
    //Change this to the location of unzipped analysis diags
    private final String unzippedAnalysisDiagsLocation = "/Users/thiru_arun/Downloads/analysisDiags-777777-73588625748280";

    /**
     * Unit test to run analysis from the unzipped analysis diags.
     * Steps to run this unit test:
     * 1. Unzip your analysis diags,
     * 2. Change the variable unzippedAnalysisDiagsLocation.
     * 3. Run the unit test.
     */
    @Ignore
    @Test
    public void testRunAnalysisFromDiags() {
        IdentityGenerator.initPrefix(9L);
        restoreAnalysisMembers(unzippedAnalysisDiagsLocation);
        if (!traderTOs.isEmpty() && analysisConfig.isPresent() && topologyInfo.isPresent()
            && !commSpecsToAdjustOverhead.isEmpty()) {
            Topology topology = TopologyEntitiesHandler.createTopology(traderTOs, topologyInfo.get(),
                commSpecsToAdjustOverhead);
            Analysis analysis = createAnalysis(topology);
            AnalysisResults results = TopologyEntitiesHandler.performAnalysis(
                traderTOs, topologyInfo.get(), analysisConfig.get(), analysis, topology);
            logger.info("Analysis generated {} actions", results.getActionsList().size());
        } else {
            logger.error("Could not create topology. Analysis was not run.");
        }
    }

    private Analysis createAnalysis(Topology topology) {
        Analysis analysis = mock(Analysis.class);
        when(analysis.isStopAnalysis()).thenReturn(false);
        ReplayActions restoredReplayActions = new ReplayActions(replayActions, replayDeactivateActions, topology);
        when(analysis.getReplayActions()).thenReturn(restoredReplayActions);
        return analysis;
    }

    private void restoreAnalysisMembers(String unzippedAnalysisDiagsLocation) {
        try {
            Iterator<Path> paths = Files.walk(Paths.get(unzippedAnalysisDiagsLocation), 1).iterator();
            while (paths.hasNext()) {
                Path path = paths.next();
                String fileName = path.getFileName().toString();
                switch (fileName) {
                    case AnalysisDiagnosticsCollector.TRADER_DIAGS_FILE_NAME:
                        traderTOs = extractMultipleInstancesOfType(path, TraderTO.class);
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
        } catch (IOException e) {
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