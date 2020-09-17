package com.vmturbo.topology.processor.history.systemload;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.history.AbstractHistoricalEditor;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoricalEditorConfig;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;

/**
 * Calculate the system load for topology commodities.
 * TODO dmitry provide config
 */
public class SystemLoadEditor extends
                AbstractHistoricalEditor<HistoricalEditorConfig, StatsHistoryServiceBlockingStub> {

    /**
     * Construct an instance of editor.
     *
     * @param config configuration
     * @param statsHistoryClient persistence client
     */
    public SystemLoadEditor(HistoricalEditorConfig config, StatsHistoryServiceBlockingStub statsHistoryClient) {
        super(config, statsHistoryClient);
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
    public boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
            @Nonnull TopologyDTO.CommodityBoughtDTO.Builder commBought, int providerType) {
        // TODO dmitry implement
        return false;
    }

    @Override
    public List<? extends Callable<List<EntityCommodityFieldReference>>>
           createPreparationTasks(@Nonnull HistoryAggregationContext context,
                                  @Nonnull List<EntityCommodityReference> commodityRefs) {
        // no caching, no chunking
        // TODO dmitry create single task
        return Collections.emptyList();
    }

    @Override
    public List<? extends Callable<List<Void>>>
           createCalculationTasks(@Nonnull HistoryAggregationContext context,
                                  @Nonnull List<EntityCommodityReference> commodityFieldRefs) {
        // TODO dmitry create single task, move/call functionality from CommoditiesEditor
        return Collections.emptyList();
    }

    @Override
    public boolean isMandatory() {
        return false;
    }

}
