package com.vmturbo.action.orchestrator.store.atomic;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;

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
     * @param deDupedActions            {@link AggregatedAction.DeDupedActions} which comprise of all resize actions
     *                                  that will be de-duplicated to a single de-duplication target entity
     * @param resizeInfoAndExplanations List of {@link ResizeInfoAndExplanation} for the single de-duplicate target entity
     * @param allResizeInfos            List of resize infos that will be aggregated to the top level atomic action
     * @param allExplanations           List of resize explanations that will be aggregated to the top level atomic action
     * @param allTargets                Set of all target entities in the top level aggregated atomic action
     */
    @Override
    void selectDeduplicatedResizesForAggregation(@Nonnull AggregatedAction.DeDupedActions deDupedActions,
                                                 @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations,
                                                 @Nonnull List<ActionDTO.ResizeInfo> allResizeInfos,
                                                 @Nonnull List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity>
                                                         allExplanations,
                                                 @Nonnull Set<Long> allTargets) {
        List<ActionDTO.ResizeInfo> resizeInfos = resizeInfos(resizeInfoAndExplanations);

        List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity>
                explanations = explanations(resizeInfoAndExplanations);

        allResizeInfos.addAll(resizeInfos);
        allExplanations.addAll(explanations);
        allTargets.add(deDupedActions.targetEntity().getId());

        logger.debug("{} {}: merges {} resizes after de-dup to {} {}", getAggregateActionTargetEntityType(),
                aggregatedAction.targetName(), resizeInfos.size(), getDeDupedActionTargetEntityType(deDupedActions),
                deDupedActions.targetName());
    }
}
