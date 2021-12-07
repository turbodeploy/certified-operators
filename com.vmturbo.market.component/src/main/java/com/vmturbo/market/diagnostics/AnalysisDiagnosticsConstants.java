package com.vmturbo.market.diagnostics;

import com.google.common.annotations.VisibleForTesting;

/**
 * All the constants related to Analysis diagnostics saving/deleting/loading.
 */
public class AnalysisDiagnosticsConstants {

    private AnalysisDiagnosticsConstants() {}

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

    static final int MAX_NUM_TRADERS_PER_PARTITION = 250000;
}
