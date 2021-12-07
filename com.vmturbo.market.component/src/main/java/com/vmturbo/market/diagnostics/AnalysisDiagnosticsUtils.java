package com.vmturbo.market.diagnostics;

import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ACTION_ZIP_LOCATION_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ANALYSIS_DIAGS_DIRECTORY;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.ANALYSIS_DIAGS_SUFFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.INITIAL_PLACEMENT_ZIP_LOCATION_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.M2_ZIP_LOCATION_PREFIX;
import static com.vmturbo.market.diagnostics.AnalysisDiagnosticsConstants.SMA_ZIP_LOCATION_PREFIX;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.market.diagnostics.AnalysisDiagnosticsCollector.AnalysisMode;

/**
 * Utility functions for saving/loading/deleting analysis diags.
 */
public class AnalysisDiagnosticsUtils {

    private AnalysisDiagnosticsUtils() {}

    /**
     * Get a string that identifies a topologyInfo uniquely.
     * @param topologyInfo topology info to identify
     * @return string that identifies a topologyInfo uniquely
     */
    public static String getTopologyUniqueIdentifier(TopologyInfo topologyInfo) {
        return topologyInfo.getTopologyContextId() + "-" + topologyInfo.getTopologyId();
    }

    /**
     * Gets the file prefix for saving/deleting diags files based on analysis mode.
     * @param analysisMode analysis mode.
     * @return gets the file prefix based on the analysis mode
     */
    public static String getFilePrefix(AnalysisMode analysisMode) {
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
        return zipPrefix;
    }

    /**
     * Gets the full path of the zip file to save or delete.
     * @param zipPrefix the prefix to be used in file name
     * @param zipSuffix the suffix to be used in file name
     * @return the full path of the zip file
     */
    public static String getZipFileFullPath(String zipPrefix, String zipSuffix) {
        return ANALYSIS_DIAGS_DIRECTORY + zipPrefix
                + ANALYSIS_DIAGS_SUFFIX
                + zipSuffix
                + ".zip";
    }
}