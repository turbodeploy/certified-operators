package com.vmturbo.topology.processor.history;

import java.util.List;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Perform a certain kind of aggregation of per-commodity history points and update the
 * topology commodities.
 *
 * @param <Config> per-editor type configuration values holder
 */
public interface IHistoricalEditor<Config> {
    /**
     * Quick check without walking the topology graph -
     * is aggregation of this kind required in the current context?
     *
     * @param changes scenarios
     * @param topologyInfo topology information
     * @param scope plan scope
     * @return true if an aggregation may be needed
     */
    boolean isApplicable(@Nonnull List<ScenarioChange> changes,
                         @Nonnull TopologyDTO.TopologyInfo topologyInfo,
                         @Nonnull PlanScope scope);

    /**
     * Whether the entity applies to history calculation.
     *
     * @param entity topology entity
     * @return true when some commodity of that entity may have to be updated
     */
    boolean isEntityApplicable(@Nonnull TopologyEntity entity);

    /**
     * Whether the sold commodity applies to history calculation.
     *
     * @param entity topology entity
     * @param commSold sold commodity
     * @return true when commodity has to be updated
     */
    boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
                                  @Nonnull CommoditySoldDTO.Builder commSold);

    /**
     * Whether the bought commodity applies to history calculation.
     *
     * @param entity topology entity
     * @param commSold bought commodity
     * @return true when commodity has to be updated
     */
    boolean isCommodityApplicable(@Nonnull TopologyEntity entity,
                                  @Nonnull CommodityBoughtDTO.Builder commBought);


    /**
     * Whether fatal failure of this editor should stop topology broadcast.
     *
     * @return true when the failure and consequently lack of data is not critical for analysis
     */
    boolean isMandatory();

    /**
     * Create (optionally chunk) the tasks that initialize the pre-calculated history.
     *
     * @param commodityRefs commodities that have to be processed
     * @return tasks for execution, each task should return the commodity fields that it loads
     */
    @Nonnull
    List<? extends Callable<List<EntityCommodityFieldReference>>>
                createPreparationTasks(@Nonnull List<EntityCommodityReferenceWithBuilder> commodityRefs);

    /**
     * Create (optionally chunk) the tasks that aggregate and set the commodity historical data.
     *
     * @param commodityRefs commodities that have to be processed
     * @return tasks for execution
     */
    @Nonnull
    List<? extends Callable<List<Void>>>
                createCalculationTasks(@Nonnull List<EntityCommodityReferenceWithBuilder> commodityRefs);
}
