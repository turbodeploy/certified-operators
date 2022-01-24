package com.vmturbo.market.diagnostics;

import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ANALYSIS_DIAGS_DIRECTORY;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ANALYSIS_DIAGS_SUFFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.M2_ZIP_LOCATION_PREFIX;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.analysis.economy.Economy;

/**
 * This class has logic that cleans up any analysis diagnostics determined not needed.
 */
public class AnalysisDiagnosticsCleaner implements IDiagnosticsCleaner {

    private static final Logger logger = LogManager.getLogger();
    private static final String REALTIME_TOPOLOGY_CONTEXT = "777777";

    private final long saveAnalysisDiagsTimeoutSecs;
    private final int numRealTimeAnalysisDiagsToRetain;
    private final IDiagsFileSystem fileSystem;

    private Future<?> saveAnalysisDiagsFuture;

    /**
     * The cleanup method of this class cleans up any analysis diagnostics not needed.
     * @param saveAnalysisDiagsTimeoutSecs number of seconds to wait for save of analysis diags before timing out
     * @param numRealTimeAnalysisDiagsToRetain number of real time analysisDiags to retain
     * @param diagsFileSystem abstraction for the file system
     */
    public AnalysisDiagnosticsCleaner(int saveAnalysisDiagsTimeoutSecs,
                                      int numRealTimeAnalysisDiagsToRetain,
                                      IDiagsFileSystem diagsFileSystem) {
        this.saveAnalysisDiagsTimeoutSecs = saveAnalysisDiagsTimeoutSecs;
        this.numRealTimeAnalysisDiagsToRetain = numRealTimeAnalysisDiagsToRetain;
        this.fileSystem = diagsFileSystem;
    }

    @Override
    public void cleanup(Economy economy, TopologyInfo topologyInfo) {
        if (saveAnalysisDiagsFuture == null) {
            logger.error("No saveAnalysisDiagsFuture for {}. This should not happen.",
                    AnalysisDiagnosticsUtils.getTopologyUniqueIdentifier(topologyInfo));
            return;
        }
        // Before deleting, we should wait for the save to finish
        waitForSaveToFinish();

        // If this is zero, we assume the feature is turned off. So no need to delete anything.
        if (numRealTimeAnalysisDiagsToRetain == 0) {
            return;
        }

        // If diags are not needed, delete whatever was written in this market cycle
        if (!areDiagsNeeded(economy, topologyInfo)) {
            String fullPath = AnalysisDiagnosticsUtils.getZipFileFullPath(
                    AnalysisDiagnosticsUtils.getFilePrefix(AnalysisDiagnosticsCollector.AnalysisMode.M2),
                    AnalysisDiagnosticsUtils.getTopologyUniqueIdentifier(topologyInfo));
            Path pathToDelete = Paths.get(fullPath);
            fileSystem.deleteIfExists(pathToDelete);
        }

        // Keep only numRealTimeAnalysisDiagsToRetain real time diags around
        List<Path> realtimeAnalysisDiags;
        try (Stream<Path> stream = fileSystem.listFiles(Paths.get(ANALYSIS_DIAGS_DIRECTORY))) {
            realtimeAnalysisDiags = stream.filter(file -> !fileSystem.isDirectory(file))
                    .filter(f -> f.getFileName().toString().startsWith(getRealTimeAnalysisDiagsStart()))
                    .collect(Collectors.toList());
            if (realtimeAnalysisDiags.size() > numRealTimeAnalysisDiagsToRetain) {
                int numFilesToDelete = realtimeAnalysisDiags.size() - numRealTimeAnalysisDiagsToRetain;
                List<Path> filesToDelete = realtimeAnalysisDiags.stream()
                        .sorted(Comparator.comparingLong(fileSystem::getLastModified))
                        .limit(numFilesToDelete)
                        .collect(Collectors.toList());
                filesToDelete.forEach(fileSystem::deleteIfExists);
            }
        }
    }

    private String getRealTimeAnalysisDiagsStart() {
        return M2_ZIP_LOCATION_PREFIX
                + ANALYSIS_DIAGS_SUFFIX
                + REALTIME_TOPOLOGY_CONTEXT;
    }

    private boolean areDiagsNeeded(Economy economy, TopologyInfo topologyInfo) {
        // Diags are needed if debug is enabled OR its real time with exceptions
        return AnalysisDiagnosticsCollector.isEnabled()
                || !TopologyDTOUtil.isPlan(topologyInfo) && !economy.getExceptionTraders().isEmpty();
    }

    private void waitForSaveToFinish() {
        try {
            saveAnalysisDiagsFuture.get(saveAnalysisDiagsTimeoutSecs, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Interrupted while saving analysis diags: ", e);
            return;
        } catch (ExecutionException e) {
            logger.error("Exception occurred while saving analysis diags: ", e.getCause());
            return;
        } catch (TimeoutException e) {
            logger.error("Timed out waiting for save of anlaysis to complete: ", e);
            return;
        }
    }

    @Override
    public void setSaveAnalysisDiagsFuture(Future<?> saveAnalysisDiagsFuture) {
        this.saveAnalysisDiagsFuture = saveAnalysisDiagsFuture;
    }

    public int getNumRealTimeAnalysisDiagsToRetain() {
        return numRealTimeAnalysisDiagsToRetain;
    }
}