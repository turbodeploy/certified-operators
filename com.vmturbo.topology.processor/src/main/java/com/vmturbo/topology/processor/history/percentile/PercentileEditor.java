package com.vmturbo.topology.processor.history.percentile;

import java.util.List;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.AbstractCachingHistoricalEditor;
import com.vmturbo.topology.processor.history.CachingHistoricalEditorConfig;

/**
 * Calculate and provide percentile historical value for topology commodities.
 * TODO dmitry provide config and db value class
 */
public class PercentileEditor extends
                AbstractCachingHistoricalEditor<PercentileCommodityData, PercentileLoadingTask, CachingHistoricalEditorConfig, Void> {

    public PercentileEditor(CachingHistoricalEditorConfig config,
                            StatsHistoryServiceBlockingStub statsHistoryClient) {
        super(config, statsHistoryClient, PercentileLoadingTask::new, PercentileCommodityData::new);
    }

    @Override
    public boolean isApplicable(List<ScenarioChange> changes, TopologyInfo topologyInfo,
                                PlanScope scope) {
        // TODO dmitry implement
        return false;
    }

    @Override
    public boolean isEntityApplicable(TopologyEntity entity) {
        // TODO dmitry implement
        return false;
    }

    @Override
    public boolean isCommodityApplicable(TopologyEntity entity,
                                         TopologyDTO.CommoditySoldDTO.Builder commSold) {
        // TODO dmitry implement
        return false;
    }

    @Override
    public boolean
           isCommodityApplicable(TopologyEntity entity,
                                 TopologyDTO.CommodityBoughtDTO.Builder commSold) {
        // TODO dmitry implement
        return false;
    }

    @Override
    public boolean isMandatory() {
        return false;
    }

}
