package com.vmturbo.topology.processor.history;

import java.util.List;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;

/**
 * Perform a certain kind of aggregation of per-commodity history points and update the
 * topology commodities.
 *
 * @param <Config> per-editor type configuration values holder
 */
public interface IHistoricalEditor<Config extends HistoricalEditorConfig> {
    /**
     * Quick check without walking the topology graph -
     * is aggregation of this kind required in the current context?
     *
     * @param changes scenarios
     * @param topologyInfo topology information
     * @param scope plan scope
     * @return true if an aggregation may be needed
     */
    boolean isApplicable(@Nullable List<ScenarioChange> changes,
                         @Nonnull TopologyDTO.TopologyInfo topologyInfo,
                         @Nullable PlanScope scope);

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
                createPreparationTasks(@Nonnull List<EntityCommodityReference> commodityRefs);

    /**
     * Create (optionally chunk) the tasks that aggregate and set the commodity historical data.
     *
     * @param commodityRefs commodities that have to be processed
     * @return tasks for execution
     */
    @Nonnull
    List<? extends Callable<List<Void>>>
                createCalculationTasks(@Nonnull List<EntityCommodityReference> commodityRefs);

    /**
     * Synchronously initialize the data, if necessary, upon broadcast.
     * In particular entity settings only get available way after construction at certain pipeline stage.
     * Which is supposed to run before history calculations stage.
     * This should be initialized when the stage begins.
     * Also editor may have to access persistent store in a single-threaded way.
     *
     * @param graph topology graph with settings
     * @param accessor commodity fields' accessor for the current graph - performance optimization
     * @param eligibleComms relevant commodities in the current topology
     * @param isPlan whether in plan context
     * @throws HistoryCalculationException when initialization fails
     * @throws InterruptedException when interrupted
     */
    void initContext(@Nonnull GraphWithSettings graph, @Nonnull ICommodityFieldAccessor accessor,
                     @Nonnull List<EntityCommodityReference> eligibleComms, boolean isPlan)
                    throws HistoryCalculationException, InterruptedException;

    /**
     * Synchronously finish the handling of the broadcast, after all the calculation tasks
     * have been invoked.
     *
     * @throws HistoryCalculationException when completion fails critically
     * @throws InterruptedException when interrupted
     */
    void completeBroadcast() throws HistoryCalculationException, InterruptedException;
}
