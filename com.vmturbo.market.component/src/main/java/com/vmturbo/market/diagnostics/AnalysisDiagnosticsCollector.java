package com.vmturbo.market.diagnostics;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandler;
import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;

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
    private static final Logger logger = LogManager.getLogger();
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    private final DiagnosticsWriter diagsWriter;

    private AnalysisDiagnosticsCollector(DiagnosticsWriter diagsWriter) {
        this.diagsWriter = diagsWriter;
    }

    /**
     * Save analysis data.
     *
     * @param traderTOs traders
     * @param topologyInfo topology info
     * @param analysisConfig analysis config
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

            writeAnalysisDiagsEntry(diagsWriter, traderTOs.stream(), TRADER_DIAGS_FILE_NAME,
                traderTOs.size() + " TraderTOs");

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
        } catch (Throwable e) {
            // Analysis should not stop because there was an error in saving diags.
            logger.error("Error when attempting to save Analysis diags. But analysis will continue.", e);
        }
    }

    private <T> void writeAnalysisDiagsEntry(final DiagnosticsWriter diagsWriter,
                                         final Stream<T> diagsEntries,
                                         final String fileName,
                                         final String logSuffix) {
        logger.info("Starting dump of {}", logSuffix);
        diagsWriter.writeZipEntry(fileName, diagsEntries.map(d ->  GSON.toJson(d)).iterator());
        logger.info("Completed dump of {}", logSuffix);
    }

    /**
     * Is saving of diags enabled? Currently, we need to make analysis.dto.logger to Debug or
     * higher to get the diags dumped. And currently, we do not export this when diags are exported.
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
         * @param topologyInfo the topologyInfo
         * @return new instance of{@link AnalysisDiagnosticsCollector}
         */
        Optional<AnalysisDiagnosticsCollector> newDiagsCollector(TopologyInfo topologyInfo);

        /**
         * The default implementation of {@link AnalysisDiagnosticsCollectorFactory}, for use in "real" code.
         */
        class DefaultAnalysisDiagnosticsCollectorFactory implements AnalysisDiagnosticsCollectorFactory {
            /**
             * Returns a new {@link AnalysisDiagnosticsCollector}.
             * @param topologyInfo the topologyInfo
             * @return a new {@link AnalysisDiagnosticsCollector}
             */
            @Override
            public Optional<AnalysisDiagnosticsCollector> newDiagsCollector(TopologyInfo topologyInfo) {
                AnalysisDiagnosticsCollector diagsCollector = null;
                if (AnalysisDiagnosticsCollector.isEnabled()) {
                    try {
                        DiagnosticsWriter diagsWriter = createDiagnosticsWriter(topologyInfo);
                        diagsCollector = new AnalysisDiagnosticsCollector(diagsWriter);
                    } catch (Exception e) {
                        logger.error("Error when attempting to write DTOs. But analysis will continue.", e);
                    }
                }
                return Optional.ofNullable(diagsCollector);
            }

            private DiagnosticsWriter createDiagnosticsWriter(TopologyInfo topologyInfo)
                throws FileNotFoundException {
                final String zipLocation = "tmp/analysisDiags-" + topologyInfo.getTopologyContextId()
                    + "-" + topologyInfo.getTopologyId() + ".zip";
                FileOutputStream fos = new FileOutputStream(zipLocation);
                ZipOutputStream diagnosticZip = new ZipOutputStream(fos);
                final DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);
                return diagsWriter;
            }
        }
    }
}