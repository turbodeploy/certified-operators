package com.vmturbo.market.diagnostics;

import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ACTIONS_FILE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ADJUST_OVERHEAD_DIAGS_FILE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ANALYSIS_CONFIG_DIAGS_FILE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.HISTORICAL_CACHED_COMMTYPE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.HISTORICAL_CACHED_ECONOMY_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.MAX_NUM_TRADERS_PER_PARTITION;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.NEW_BUYERS_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.REALTIME_CACHED_COMMTYPE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.REALTIME_CACHED_ECONOMY_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMAINPUT_FILE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_CONFIG_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_CONTEXT_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_RESERVED_INSTANCE_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_TEMPLATE_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_VIRTUAL_MACHINE_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.TOPOLOGY_INFO_DIAGS_FILE_NAME;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.TRADER_DIAGS_FILE_NAME;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.BiMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandler;
import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.SerializationDTOs.TraderDiagsTO;

/**
 * Saves diags for analysis.
 */
public class AnalysisDiagnosticsCollector {

    private static final Logger logger = LogManager.getLogger();
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final String zipFilenameSuffix;
    private final AnalysisMode analysisMode;

    /**
     * To identify which analysis diags we are saving.
     */
    public enum AnalysisMode {
        /**
         * mode for saving m2 diags.
         */
        M2,

        /**
         * mode for saving SMA diags.
         */
        SMA,

        /**
         * mode for saving actions.
         */
        ACTIONS,

        /**
         * mode for saving economy of initialPlacement.
         */
        INITIAL_PLACEMENT;
    }

    /**
     * Wrapper for storing the InitialPlacement CommType map.
     */
    public class InitialPlacementCommTypeMap {
        /**
         * the type assigned in the traderTO.
         */
        public final Integer type;
        /**
         * the commodityType in the TopologyDTO.
         */
        public final CommodityType commodityType;

        InitialPlacementCommTypeMap(CommodityType commodityType,
                                    Integer type) {
            this.type = type;
            this.commodityType = commodityType;
        }
    }

    /**
     * AnalysisDiagnosticsCollector constructor.
     *
     * @param zipFilenameSuffix   the diagnostics writer.
     * @param analysisMode the zip output stream.
     */
    private AnalysisDiagnosticsCollector(String zipFilenameSuffix, AnalysisMode analysisMode) {
        this.zipFilenameSuffix = zipFilenameSuffix;
        this.analysisMode = analysisMode;
    }

    /**
     * Save SMA and M2 cloud VM compute actions.
     *
     * @param actionLogs   list of actions.
     * @param topologyInfo topology info
     */
    public void saveActionsIfEnabled(List<String> actionLogs,
                                     final TopologyInfo topologyInfo) {
        if (!isEnabled()) {
            return;
        }
        ZipOutputStream diagnosticZip = null;
        try {
            diagnosticZip = createZipOutputStream(zipFilenameSuffix, analysisMode);
            DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);
            logger.info("Starting dump of Actions for topology context id {}",
                    topologyInfo.getTopologyContextId());
            final Stopwatch stopwatch = Stopwatch.createStarted();
            diagsWriter.writeZipEntry(ACTIONS_FILE_NAME, actionLogs.iterator());
            stopwatch.stop();

            logger.info("Completed dump of Actions for topology context id {} in {} seconds",
                    topologyInfo.getTopologyContextId(), stopwatch.elapsed(TimeUnit.SECONDS));
        } catch (Exception e) {
            // Analysis should not stop because there was an error in saving diags.
            logger.error("Error when attempting to save Actions. But analysis will continue.", e);
        } finally {
            closeZipOutputStream(diagnosticZip);
        }
    }

    /**
     * Save Initial Placement Diags.
     *
     * @param timeStamp unix time.
     * @param newInitialPlacements the initialPlacements to save.
     * @param historicalCachedCommTypeMap A map that stores the TopologyDTO.CommodityType
     *                                    to traderTO's CommoditySpecification in historical economy.
     * @param realtimeCachedCommTypeMap A map that stores the TopologyDTO.CommodityType
     *                                  to traderTO's CommoditySpecification in realtime economy.
     * @param historicalCachedEconomy historical cached economy.
     * @param realtimeCachedEconomy realtime cached economy.
     */
    public void saveInitialPlacementDiagsIfEnabled(@Nullable String timeStamp,
                                                   @Nullable BiMap<CommodityType, Integer> historicalCachedCommTypeMap,
                                                   @Nullable BiMap<CommodityType, Integer> realtimeCachedCommTypeMap,
                                                   @Nonnull List<InitialPlacementDTO> newInitialPlacements,
                                                   @Nullable Economy historicalCachedEconomy,
                                                   @Nullable Economy realtimeCachedEconomy) {
        if (!isEnabled()) {
            return;
        }
        final Stopwatch stopwatch = Stopwatch.createStarted();
        ZipOutputStream diagnosticZip = null;
        try {
            logger.info("FindInitialPlacement: Starting dump of InitialPlacement diagnostics with timeStamp {}",
                    timeStamp);
            diagnosticZip = createZipOutputStream(zipFilenameSuffix, analysisMode);
            DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);
            if (historicalCachedEconomy != null) {
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                     ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                    oos.writeObject(historicalCachedEconomy);
                    oos.flush();
                    diagsWriter.writeZipEntry(HISTORICAL_CACHED_ECONOMY_NAME, bos.toByteArray());
                }
            }

            if (realtimeCachedEconomy != null) {
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                     ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                    oos.writeObject(realtimeCachedEconomy);
                    oos.flush();
                    diagsWriter.writeZipEntry(REALTIME_CACHED_ECONOMY_NAME, bos.toByteArray());
                }
            }

            writeAnalysisDiagsEntry(diagsWriter, newInitialPlacements.stream(),
                    NEW_BUYERS_NAME,
                    newInitialPlacements.size() + " " +  NEW_BUYERS_NAME);

            if (historicalCachedCommTypeMap != null) {
                List<InitialPlacementCommTypeMap> historicalCachedCommType = new ArrayList<>();
                historicalCachedCommTypeMap.entrySet().stream().forEach(entry ->
                        historicalCachedCommType.add(new InitialPlacementCommTypeMap(entry.getKey(), entry.getValue())));
                writeAnalysisDiagsEntry(diagsWriter, historicalCachedCommType.stream(),
                        HISTORICAL_CACHED_COMMTYPE_NAME,
                        historicalCachedCommType.size() + " " + HISTORICAL_CACHED_COMMTYPE_NAME);
            }

            if (realtimeCachedCommTypeMap != null) {
                List<InitialPlacementCommTypeMap> realtimeCachedCommType = new ArrayList<>();
                realtimeCachedCommTypeMap.entrySet().stream().forEach(entry ->
                        realtimeCachedCommType.add(new InitialPlacementCommTypeMap(entry.getKey(), entry.getValue())));
                writeAnalysisDiagsEntry(diagsWriter, realtimeCachedCommType.stream(),
                        REALTIME_CACHED_COMMTYPE_NAME,
                        realtimeCachedCommType.size() + " " + REALTIME_CACHED_COMMTYPE_NAME);
            }

        } catch (StackOverflowError e) {
            // If any of the objects being converted to JSON have a circular reference, then it
            // can lead to a StackOverflowError. We only print the message of the exception because
            // we don't want to spam the logs with the stack trace for a stack overflow exception
            logger.error("FindInitialPlacement: StackOverflowError when attempting to save InitialPlacement diags.", e.toString());
        } catch (Exception e) {
            logger.error("FindInitialPlacement: Error when attempting to save InitialPlacement diags", e);
        } finally {
            closeZipOutputStream(diagnosticZip);
            stopwatch.stop();
            logger.info("FindInitialPlacement: Completed dump of InitialPlacement diagnostics with timeStamp  {} in {} seconds",
                    timeStamp, stopwatch.elapsed(TimeUnit.SECONDS));
        }
    }


    /**
     * Save SMA Input data.
     *
     * @param smaInput     smaInput to save
     * @param topologyInfo topology info
     */
    public void saveSMAInputIfEnabled(final SMAInput smaInput,
                                      final TopologyInfo topologyInfo) {
        if (!isEnabled()) {
            return;
        }
        final Stopwatch stopwatch = Stopwatch.createStarted();
        smaInput.getContexts().stream().forEach(a -> a.compress());
        ZipOutputStream diagnosticZip = null;
        try {
            diagnosticZip = createZipOutputStream(zipFilenameSuffix, analysisMode);
            DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);
            logger.info("Starting dump of SMA diagnostics for topology context id {}",
                    topologyInfo.getTopologyContextId());
            for (int contextIndex = 0; contextIndex < smaInput.getContexts().size(); contextIndex++) {
                SMAInputContext inputContext = smaInput.getContexts().get(contextIndex);
                writeAnalysisDiagsEntry(diagsWriter, inputContext.getReservedInstances().stream(),
                        SMA_RESERVED_INSTANCE_PREFIX + "_" + contextIndex + "_"
                                + SMAINPUT_FILE_NAME,
                        inputContext.getReservedInstances().size()
                                + " Reserved Instances" + " contextID: " + contextIndex);
                writeAnalysisDiagsEntry(diagsWriter, inputContext.getVirtualMachines().stream(),
                        SMA_VIRTUAL_MACHINE_PREFIX + "_" + contextIndex + "_"
                                + SMAINPUT_FILE_NAME,
                        inputContext.getVirtualMachines().size()
                                + " Virtual Machines" + " contextID: " + contextIndex);
                writeAnalysisDiagsEntry(diagsWriter, inputContext.getTemplates().stream(),
                        SMA_TEMPLATE_PREFIX + "_" + contextIndex + "_"
                                + SMAINPUT_FILE_NAME,
                        inputContext.getTemplates().size()
                                + " Templates" + " contextID: " + contextIndex);
                writeAnalysisDiagsEntry(diagsWriter, Stream.of(inputContext.getContext()),
                        SMA_CONTEXT_PREFIX + "_" + contextIndex + "_"
                                + SMAINPUT_FILE_NAME,
                        "SMA Context" + " contextID: " + contextIndex);
                writeAnalysisDiagsEntry(diagsWriter, Stream.of(inputContext.getSmaConfig()),
                        SMA_CONFIG_PREFIX + "_" + contextIndex + "_"
                                + SMAINPUT_FILE_NAME,
                        "SMA Config" + " contextID: " + contextIndex);
            }
        } catch (StackOverflowError e) {
            // If any of the objects being converted to JSON have a circular reference, then it
            // can lead to a StackOverflowError. We only print the message of the exception because
            // we don't want to spam the logs with the stack trace for a stack overflow exception
            logger.error("Error when attempting to save SMA diags. But analysis will continue.", e.toString());
        } catch (Exception e) {
            // Analysis should not stop because there was an error in saving diags.
            logger.error("Error when attempting to save SMA diags. But analysis will continue.", e);
        } finally {
            closeZipOutputStream(diagnosticZip);
            smaInput.getContexts().stream().forEach(a -> a.decompress(smaInput.getCloudCostCalculator()));

            stopwatch.stop();
            logger.info("Completed dump of SMA diagnostics for topology context id {} in {} seconds",
                    topologyInfo.getTopologyContextId(), stopwatch.elapsed(TimeUnit.SECONDS));
        }
    }

    /**
     * create a zip output stream.
     * @param zipFilenameSuffix suffix for the zip file created
     * @param analysisMode determines what data is stored
     * @return the zipOutputStream created.
     * @throws FileNotFoundException if ANALYSIS_DIAGS_DIRECTORY is not found.
     */
    private ZipOutputStream createZipOutputStream(String zipFilenameSuffix, AnalysisMode analysisMode)
            throws FileNotFoundException {
        final String zipLocation = AnalysisDiagnosticsUtils.getZipFileFullPath(
                AnalysisDiagnosticsUtils.getFilePrefix(analysisMode), zipFilenameSuffix);
        FileOutputStream fos = new FileOutputStream(zipLocation);
        return new ZipOutputStream(fos);
    }

    /**
     * Save analysis data.
     *
     * @param traderTOs                 traders
     * @param topologyInfo              topology info
     * @param analysisConfig            analysis config
     * @param commSpecsToAdjustOverhead commSpecsToAdjustOverhead
     * @param numRealTimeAnalysisDiagsToRetain number of real time analysis diagnostics to retain
     */
    public void saveAnalysisIfEnabled(final List<TraderTO> traderTOs,
                                      final TopologyInfo topologyInfo,
                                      final AnalysisConfig analysisConfig,
                                      final List<CommoditySpecification> commSpecsToAdjustOverhead,
                                      final int numRealTimeAnalysisDiagsToRetain) {
        if (!isAnalysisDiagsSaveEnabled(topologyInfo, numRealTimeAnalysisDiagsToRetain)) {
            return;
        }
        ZipOutputStream diagnosticZip = null;
        try {
            logger.info("Starting dump of Analysis diagnostics for topology context id {}",
                    topologyInfo.getTopologyContextId());
            final Stopwatch stopwatch = Stopwatch.createStarted();

            diagnosticZip = createZipOutputStream(zipFilenameSuffix, analysisMode);
            DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);

            List<List<TraderTO>> partitonedTraderTOs = Lists.partition(traderTOs, MAX_NUM_TRADERS_PER_PARTITION);
            logger.info("Starting dump of TraderTOs");
            for (int i = 0; i < partitonedTraderTOs.size(); i++) {
                diagsWriter.writeZipEntry(TRADER_DIAGS_FILE_NAME + String.format( "%03d", i),
                        TraderDiagsTO.newBuilder().addAllTraderTOs(partitonedTraderTOs.get(i)).build());
            }
            logger.info("Completed dump of TraderTOs");

            writeAnalysisDiagsEntry(diagsWriter, Stream.of(topologyInfo),
                    TOPOLOGY_INFO_DIAGS_FILE_NAME, "TopologyInfo");

            writeAnalysisDiagsEntry(diagsWriter, Stream.of(analysisConfig),
                    ANALYSIS_CONFIG_DIAGS_FILE_NAME, "AnalysisConfig");

            writeAnalysisDiagsEntry(diagsWriter, commSpecsToAdjustOverhead.stream(),
                    ADJUST_OVERHEAD_DIAGS_FILE_NAME, commSpecsToAdjustOverhead.size()
                            + " CommSpecsToAdjustOverhead");

            if (!diagsWriter.getErrors().isEmpty()) {
                logger.error("Encountered {} errors. Check {} for details",
                        diagsWriter.getErrors().size(), DiagnosticsHandler.ERRORS_FILE);
                diagsWriter.writeZipEntry(DiagnosticsHandler.ERRORS_FILE,
                        diagsWriter.getErrors().iterator());
            }
            stopwatch.stop();
            logger.info("Completed dump of Analysis diagnostics for topology context id {} in {} seconds",
                    topologyInfo.getTopologyContextId(), stopwatch.elapsed(TimeUnit.SECONDS));
        } catch (StackOverflowError e) {
            // If any of the objects being converted to JSON have a circular reference, then it
            // can lead to a StackOverflowError. We only print the message of the exception because
            // we don't want to spam the logs with the stack trace for a stack overflow exception
            logger.error("Error when attempting to save Analysis diags. But analysis will continue.", e.toString());
        } catch (Exception e) {
            // Analysis should not stop because there was an error in saving diags.
            logger.error("Error when attempting to save Analysis diags. But analysis will continue.", e);
        } finally {
            closeZipOutputStream(diagnosticZip);
        }
    }

    /**
     * Analysis diags should be saved when either:
     * 1. debug is enabled
     * OR
     * 2. topology is for real time AND the number of diags to retain is greater than zero.
     * @param topologyInfo topology info
     * @param numRealTimeAnalysisDiagsToRetain number of real time analysis diags to retain
     * @return true if analysis diags is to be saved, false otherwise
     */
    @VisibleForTesting
    static boolean isAnalysisDiagsSaveEnabled(final TopologyInfo topologyInfo, final int numRealTimeAnalysisDiagsToRetain) {
        return isEnabled() || !topologyInfo.hasPlanInfo() && numRealTimeAnalysisDiagsToRetain > 0;
    }

    private <T> void writeAnalysisDiagsEntry(final DiagnosticsWriter diagsWriter,
                                             final Stream<T> diagsEntries,
                                             final String fileName,
                                             final String logSuffix) {
        logger.info("Starting dump of {}", logSuffix);
        diagsWriter.writeZipEntry(fileName, diagsEntries.map(d -> GSON.toJson(d)).iterator());
        logger.info("Completed dump of {}", logSuffix);
    }

    private void closeZipOutputStream(@Nullable ZipOutputStream diagnosticZip) {
        try {
            if (diagnosticZip != null) {
                diagnosticZip.close();
            }
        } catch (IOException e) {
            logger.error("Error occurred while closing action diags zip file", e);
        }
    }

    /**
     * Is saving of diags enabled? Currently, we need to make analysis.dto.logger to Debug or
     * higher to get the diags dumped. And currently, we do not export this when diags are exported.
     *
     * @return true if analysis diags need to be saved, false otherwise
     */
    public static boolean isEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * Factory for instances of {@link AnalysisDiagnosticsCollector}.
     */
    public interface AnalysisDiagnosticsCollectorFactory {
        /**
         * Create a new {@link AnalysisDiagnosticsCollector}.
         *
         * @param zipFilenameSuffix the suffix for the zip filename
         * @param analysisMode collecting sma diags if mode is SMA
         * @return new instance of{@link AnalysisDiagnosticsCollector}
         */
        Optional<AnalysisDiagnosticsCollector> newDiagsCollector(String zipFilenameSuffix, AnalysisMode analysisMode);

        /**
         * The default implementation of {@link AnalysisDiagnosticsCollectorFactory}, for use in "real" code.
         */
        class DefaultAnalysisDiagnosticsCollectorFactory implements AnalysisDiagnosticsCollectorFactory {
            /**
             * Returns a new {@link AnalysisDiagnosticsCollector}.
             *
             * @param zipFilenameSuffix the suffix for the zip filename
             * @param analysisMode collecting sma diags if mode is SMA
             * @return a new {@link AnalysisDiagnosticsCollector}
             */
            @Override
            public Optional<AnalysisDiagnosticsCollector> newDiagsCollector(String zipFilenameSuffix, AnalysisMode analysisMode) {
                AnalysisDiagnosticsCollector diagsCollector = null;
                try {
                    // ZipOutputStream diagnosticZip = createZipOutputStream(zipFilenameSuffix, analysisMode);
                    // DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);
                    diagsCollector = new AnalysisDiagnosticsCollector(zipFilenameSuffix, analysisMode);
                } catch (Exception e) {
                    logger.error("Error when attempting to write DTOs. But analysis will continue.", e);
                }
                return Optional.ofNullable(diagsCollector);
            }
        }
    }
}