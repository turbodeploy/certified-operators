package com.vmturbo.topology.processor.history.timeslot;

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
 * Calculate and provide time slot historical values for topology commodities.
 * TODO dmitry provide config and db value class
 */
public class TimeSlotEditor extends
                AbstractCachingHistoricalEditor<TimeSlotCommodityData,
                TimeSlotLoadingTask,
                CachingHistoricalEditorConfig,
                Void,
                StatsHistoryServiceBlockingStub> {

    public TimeSlotEditor(CachingHistoricalEditorConfig config,
                          StatsHistoryServiceBlockingStub statsHistoryClient) {
        super(config, statsHistoryClient, TimeSlotLoadingTask::new, TimeSlotCommodityData::new);
    }

    @Override
    public boolean isApplicable(List<ScenarioChange> changes, TopologyInfo topologyInfo,
                                PlanScope scope) {
        // TODO dmitry implement
        return true;
    }

    @Override
    public boolean isEntityApplicable(TopologyEntity entity) {
        // TODO dmitry implement
        return true;
    }

    @Override
    public boolean isCommodityApplicable(TopologyEntity entity,
                                         TopologyDTO.CommoditySoldDTO.Builder commSold) {
        // TODO dmitry implement
        return true;
    }

    @Override
    public boolean
           isCommodityApplicable(TopologyEntity entity,
                                 TopologyDTO.CommodityBoughtDTO.Builder commSold) {
        // TODO dmitry implement
        return true;
    }

    @Override
    public boolean isMandatory() {
        return false;
    }

}
