package com.vmturbo.action.orchestrator.store.atomic;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Builder class to build Atomic Resize actions for Plan actions.
 */
public class PlanAtomicResizeBuilder extends AtomicResizeBuilder {
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor for a new builder.
     *
     * @param aggregatedAction  AggregatedAction containing the relevant information
     *                              for creating the atomic action
     */
    PlanAtomicResizeBuilder(@Nonnull AggregatedAction aggregatedAction) {
        super(aggregatedAction);
    }

    /**
     *  Select the de-duplicated resize infos and explanations for aggregation.
     *  Only the selected de-duplicated resizes will be aggregated to the top level aggregated atomic action.
     *
     *  <p>For Plans, all the de-duplicated resize infos are aggregated to the top level aggregate atomic actions.
     *
     * @param deDupedActions                {@link AggregatedAction.DeDupedActions} which comprise of all resize actions
     *                                      that will be de-duplicated to a single de-duplication target entity
     * @param resizeInfoAndExplanations     List of {@link ResizeInfoAndExplanation} for the single de-duplicate target entity
     *
     * @return  List of ResizeInfoAndExplanation that will be part of the aggregation level atomic action
     */
    @Override
    List<ResizeInfoAndExplanation> selectResizesForAggregation(@Nonnull AggregatedAction.DeDupedActions deDupedActions,
                                                               @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        logger.trace("{}::{}: merges {} of {} resizes after de-dup to {}::{}",
                            getAggregateActionTargetEntityType(), aggregatedAction.targetName(),
                            resizeInfoAndExplanations.size(), resizeInfoAndExplanations.size(),
                            getDeDupedActionTargetEntityType(deDupedActions), deDupedActions.targetName());

        return resizeInfoAndExplanations;
    }

    /**
     * Select the resize infos and explanations for de-duplicated atomic action.
     * Only the selected resizes will be used to create the de-duplication level atomic action.
     *
     * <p>For Plans, de-duplication level atomic action will not be created, only the aggregate atomic actions.
     *
     * @param deDupedActions                {@link AggregatedAction.DeDupedActions} which comprise of all resize actions
     * @param resizeInfoAndExplanations     List of {@link ResizeInfoAndExplanation} for the single de-duplicate target entity
     *
     * @return  List of ResizeInfoAndExplanation that will be part of the de-duplication level atomic action
     */
    @Override
    List<ResizeInfoAndExplanation> selectResizesForDeDuplication(@Nonnull AggregatedAction.DeDupedActions deDupedActions,
                                                                 @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        logger.trace("{}: de-duplication level atomic action will not be created", deDupedActions.targetName());
        return Collections.emptyList();
    }
}
