package com.vmturbo.topology.processor.history.histutil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.AbstractCachingHistoricalEditor;
import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;

/**
 * Calculate the hist utilization for topology commodities.
 * TODO dmitry provide configuration - weights
 */
public class HistUtilizationEditor extends
                AbstractCachingHistoricalEditor<HistUtilizationCommodityData,
                    HistUtilizationLoadingTask,
                    CachingHistoricalEditorConfig,
                    Float,
                    StatsHistoryServiceBlockingStub,
                    Void> {

    public HistUtilizationEditor(CachingHistoricalEditorConfig config,
                    StatsHistoryServiceBlockingStub statsHistoryClient) {
        super(config, statsHistoryClient, HistUtilizationLoadingTask::new,
              HistUtilizationCommodityData::new);
    }

    @Override
    public boolean isApplicable(List<ScenarioChange> changes, TopologyInfo topologyInfo,
                                PlanScope scope) {
        return true;
    }

    @Override
    public boolean isEntityApplicable(TopologyEntity entity) {
        return true;
    }

    @Override
    public boolean isCommodityApplicable(TopologyEntity entity,
                                         TopologyDTO.CommoditySoldDTO.Builder commSold) {
        // TODO dmitry filter access
        return true;
    }

    @Override
    public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
            @Nonnull TopologyDTO.CommodityBoughtDTO.Builder commBought, int providerType) {
        // TODO dmitry filter access
        return true;
    }

    @Override
    public boolean isMandatory() {
        return true;
    }

    @Override
    protected void exportState(@Nonnull OutputStream appender)
                    throws DiagnosticsException, IOException {
        // TODO Alexander Vasin
    }

    @Override
    protected void restoreState(@Nonnull byte[] bytes) throws DiagnosticsException {
        // TODO Alexander Vasin
    }
}
