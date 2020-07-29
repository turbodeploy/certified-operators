package com.vmturbo.topology.processor.history.maxvalue;

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
 * Provide the max quantity for topology commodities.
 * TODO dmitry provide config similar to CommodityPostStitchingOperationConfig
 */
public class MaxValueEditor extends
                AbstractCachingHistoricalEditor<MaxValueCommodityData,
                    MaxValueLoadingTask,
                    CachingHistoricalEditorConfig,
                    Float,
                    StatsHistoryServiceBlockingStub,
                    Void> {
    // TODO dmitry handle background reloading task

    public MaxValueEditor(@Nonnull CachingHistoricalEditorConfig config,
                          @Nonnull StatsHistoryServiceBlockingStub statsHistoryClient) {
        super(config, statsHistoryClient, MaxValueLoadingTask::new, MaxValueCommodityData::new);
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
