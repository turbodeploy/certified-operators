package com.vmturbo.market.diagnostics;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.BiMap;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyer;
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

    @VisibleForTesting
    static final String TRADER_DIAGS_FILE_NAME = "TraderTOs.diags";
    @VisibleForTesting
    static final String TOPOLOGY_INFO_DIAGS_FILE_NAME = "TopologyInfo.diags";
    @VisibleForTesting
    static final String ANALYSIS_CONFIG_DIAGS_FILE_NAME = "AnalysisConfig.diags";
    @VisibleForTesting
    static final String ADJUST_OVERHEAD_DIAGS_FILE_NAME = "CommSpecsToAdjustOverhead.diags";
    @VisibleForTesting
    static final String SMAINPUT_FILE_NAME = "SMAInput.diags";
    @VisibleForTesting
    static final String ACTIONS_FILE_NAME = "Actions.csv";
    @VisibleForTesting
    static final String HISTORICAL_CACHED_COMMTYPE_NAME = "HistoricalCachedCommTypeMap.diags";
    @VisibleForTesting
    static final String REALTIME_CACHED_COMMTYPE_NAME = "RealtimeCachedCommTypeMap.diags";
    @VisibleForTesting
    static final String HISTORICAL_CACHED_ECONOMY_NAME = "HistoricalCachedEconomy.diags";
    @VisibleForTesting
    static final String REALTIME_CACHED_ECONOMY_NAME = "RealtimeCachedEconomy.diags";
    @VisibleForTesting
    static final String NEW_BUYERS_NAME = "NewBuyers.diags";
    @VisibleForTesting
    static final String SMA_RESERVED_INSTANCE_PREFIX = "RI";
    @VisibleForTesting
    static final String SMA_VIRTUAL_MACHINE_PREFIX = "VM";
    @VisibleForTesting
    static final String SMA_CONTEXT_PREFIX = "Context";
    @VisibleForTesting
    static final String SMA_CONFIG_PREFIX = "Config";
    @VisibleForTesting
    static final String SMA_TEMPLATE_PREFIX = "Template";


    static final String ANALYSIS_DIAGS_DIRECTORY = "tmp/";
    static final String SMA_ZIP_LOCATION_PREFIX = "sma";
    static final String M2_ZIP_LOCATION_PREFIX = "analysis";
    static final String ACTION_ZIP_LOCATION_PREFIX = "action";
    static final String INITIAL_PLACEMENT_ZIP_LOCATION_PREFIX = "initialPlacement";
    static final String ANALYSIS_DIAGS_SUFFIX = "Diags-";

    private static final Logger logger = LogManager.getLogger();
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    private final DiagnosticsWriter diagsWriter;
    private final ZipOutputStream diagnosticZip;

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
     * @param diagsWriter   the diagnostics writer.
     * @param diagnosticZip the zip output stream.
     */
    private AnalysisDiagnosticsCollector(DiagnosticsWriter diagsWriter,
                                         ZipOutputStream diagnosticZip) {
        this.diagsWriter = diagsWriter;
        this.diagnosticZip = diagnosticZip;
    }

    /**
     * Save SMA and M2 cloud VM compute actions.
     *
     * @param actionLogs   list of actions.
     * @param topologyInfo topology info
     */
    public void saveActions(List<String> actionLogs,
                            final TopologyInfo topologyInfo) {
        try {
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
            try {
                diagnosticZip.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Save Initial Placement Diags.
     *
     * @param timeStamp unix time.
     * @param newBuyers the current set of InitialPlacementBuyers.
     * @param historicalCachedCommTypeMap A map that stores the TopologyDTO.CommodityType
     *                                    to traderTO's CommoditySpecification in historical economy.
     * @param realtimeCachedCommTypeMap A map that stores the TopologyDTO.CommodityType
     *                                  to traderTO's CommoditySpecification in realtime economy.
     * @param historicalCachedEconomy historical cached economy.
     * @param realtimeCachedEconomy realtime cached economy.
     */
    public void saveInitialPlacementDiags(String timeStamp,
                                          BiMap<CommodityType, Integer> historicalCachedCommTypeMap,
                                          BiMap<CommodityType, Integer> realtimeCachedCommTypeMap,
                                          List<InitialPlacementBuyer> newBuyers,
                                          Economy historicalCachedEconomy,
                                          Economy realtimeCachedEconomy) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            logger.info("FindInitialPlacement: Starting dump of InitialPlacement diagnostics with timeStamp {}",
                    timeStamp);

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

            writeAnalysisDiagsEntry(diagsWriter, newBuyers.stream(),
                    NEW_BUYERS_NAME,
                    newBuyers.size() + " " +  NEW_BUYERS_NAME);

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
            try {
                diagnosticZip.close();
            } catch (IOException e) {
                logger.error("FindInitialPlacement: Error when attempting to close diagnostics zip file.", e);
            }
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
    public void saveSMAInput(final SMAInput smaInput,
                             final TopologyInfo topologyInfo) {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        smaInput.getContexts().stream().forEach(a -> a.compress());
        try {
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
            try {
                diagnosticZip.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            smaInput.getContexts().stream().forEach(a -> a.decompress());
            stopwatch.stop();
            logger.info("Completed dump of SMA diagnostics for topology context id {} in {} seconds",
                    topologyInfo.getTopologyContextId(), stopwatch.elapsed(TimeUnit.SECONDS));
        }
    }

    /**
     * Save analysis data.
     *
     * @param traderTOs                 traders
     * @param topologyInfo              topology info
     * @param analysisConfig            analysis config
     * @param commSpecsToAdjustOverhead commSpecsToAdjustOverhead
     */
    public void saveAnalysis(final Collection<TraderTO> traderTOs,
                             final TopologyInfo topologyInfo,
                             final AnalysisConfig analysisConfig,
                             final List<CommoditySpecification> commSpecsToAdjustOverhead) {
        try {
            logger.info("Starting dump of Analysis diagnostics for topology context id {}",
                    topologyInfo.getTopologyContextId());
            final Stopwatch stopwatch = Stopwatch.createStarted();

            logger.info("Starting dump of TraderTOs");
            diagsWriter.writeZipEntry(TRADER_DIAGS_FILE_NAME,
                TraderDiagsTO.newBuilder().addAllTraderTOs(traderTOs).build());
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
            try {
                diagnosticZip.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private <T> void writeAnalysisDiagsEntry(final DiagnosticsWriter diagsWriter,
                                             final Stream<T> diagsEntries,
                                             final String fileName,
                                             final String logSuffix) {
        logger.info("Starting dump of {}", logSuffix);
        diagsWriter.writeZipEntry(fileName, diagsEntries.map(d -> GSON.toJson(d)).iterator());
        logger.info("Completed dump of {}", logSuffix);
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
                if (AnalysisDiagnosticsCollector.isEnabled()) {
                    try {
                        ZipOutputStream diagnosticZip = createZipOutputStream(zipFilenameSuffix, analysisMode);
                        DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);
                        diagsCollector = new AnalysisDiagnosticsCollector(diagsWriter, diagnosticZip);
                    } catch (Exception e) {
                        logger.error("Error when attempting to write DTOs. But analysis will continue.", e);
                    }
                }
                return Optional.ofNullable(diagsCollector);
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

                String zipPrefix = "";
                if (analysisMode == AnalysisMode.SMA) {
                    zipPrefix = SMA_ZIP_LOCATION_PREFIX;
                } else if (analysisMode == AnalysisMode.M2) {
                    zipPrefix = M2_ZIP_LOCATION_PREFIX;
                } else if (analysisMode == AnalysisMode.ACTIONS) {
                    zipPrefix = ACTION_ZIP_LOCATION_PREFIX;
                } else if (analysisMode == AnalysisMode.INITIAL_PLACEMENT) {
                    zipPrefix = INITIAL_PLACEMENT_ZIP_LOCATION_PREFIX;
                }

                final String zipLocation = ANALYSIS_DIAGS_DIRECTORY + zipPrefix
                        + ANALYSIS_DIAGS_SUFFIX
                        + zipFilenameSuffix
                        + ".zip";

                FileOutputStream fos = new FileOutputStream(zipLocation);
                ZipOutputStream diagnosticZip = new ZipOutputStream(fos);
                return diagnosticZip;
            }
        }
    }
}