package com.vmturbo.action.orchestrator.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.store.AggregatedAction.DeDupedActions;
import com.vmturbo.action.orchestrator.store.AtomicActionFactory.AtomicActionResult;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * Builder class to build Atomic Resize actions.
 */
class AtomicResizeBuilder implements AtomicActionBuilder {

    private final Logger logger = LogManager.getLogger();

    // Class representing the aggregated/de-duplicated actions and the assoicated execution target
    private AggregatedAction aggregatedAction;

    /**
     * Constructor for a new builder.
     *
     * @param aggregatedAction  AggregatedAction containing the relevant information
     *                              for creating the atomic action
     */
    AtomicResizeBuilder(@Nonnull AggregatedAction aggregatedAction) {
        this.aggregatedAction = aggregatedAction;
    }

    /**
     * Create the atomic action DTOs for the aggregated and the de-duplicated actions.
     *
     * @return {@link AtomicActionResult} containing the {@link ActionDTO.Action} for the
     *          aggregated action and the {@link ActionDTO.Action} for the de-duplicated actions.
     */
    @Override
    public AtomicActionResult build() {
        Map<Action, List<Action>> deDupedAtomicActionsMap = new HashMap<>();

        List<ActionDTO.ResizeInfo> allResizeInfos = new ArrayList<>();
        List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity>
                allExplanations = new ArrayList<>();
        // target entity for the individual resize infos
        Set<Long> allTargets = new HashSet<>();

        // Actions that are not duplicated, create a resizeInfo corresponding to each.
        // Do not create the atomic action for this resizeInfo
        for (Action resizeAction : aggregatedAction.aggregateOnlyActions()) {
            Resize resizeInfo = resizeAction.getInfo().getResize();
            ActionEntity entity = resizeInfo.getTarget();

            ResizeInfoAndExplanation resizeInfoAndExplanation
                        = resizeInfoAndExplanation(entity.toString(), entity,
                                        resizeInfo.getCommodityType(),
                                        Collections.singletonList(resizeAction));

            allResizeInfos.add(resizeInfoAndExplanation.resizeInfo());
            allExplanations.add(resizeInfoAndExplanation.explanation());
            allTargets.add(entity.getId());
        }

        // For actions that are to be de-duplicated, create a resize info per de-dup target
        // and commodity type. Also create a corresponding non-executable atomic action.
        aggregatedAction.deDupedActionsMap().forEach((deDupTargetOid, deDupedActions) -> {
            // First create the resize info objects per de-dup target and commodity type
            //DeDupedActions deDupedActions = aggregatedAction.deDupedActionsMap().get(deDupTargetOid);
            List<ResizeInfoAndExplanation> resizeInfoAndExplanations
                    = deDuplicatedResizeInfoAndExplanation(deDupedActions);

            List<ActionDTO.ResizeInfo> resizeInfos = resizeInfoAndExplanations.stream()
                    .map(r -> r.resizeInfo())
                    .collect(Collectors.toList());
            List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity>
                    explanations = resizeInfoAndExplanations.stream()
                    .map(r -> r.explanation())
                    .collect(Collectors.toList());

            // Collect all the resize and explanation info for the atomic action
            allResizeInfos.addAll(resizeInfos);
            allExplanations.addAll(explanations);
            allTargets.add(deDupedActions.targetEntity().getId());

            logger.debug("{}: merges {} resizes after de-dup to {}",
                            aggregatedAction.targetName(),
                            resizeInfos.size(), deDupedActions.targetName());

            // ---- atomic action for this de-duplication target (for displaying in the UI only)
            // comprises of the resizes for all the commodity type
            Action.Builder deDupedAction = createAtomicResizeAction(deDupedActions.targetName(),
                                                    deDupedActions.targetEntity(),
                                                    resizeInfos,
                                                    Collections.singletonList(deDupTargetOid),
                                                    explanations);

            List<Action> mergedActions = deDupedActions.actions();
            logger.trace("{}: de-duplicated {} actions to {} resizes",
                    deDupedActions.targetName(), mergedActions.size(),
                    deDupedAction.getInfo().getAtomicResize().getResizesCount());

            deDupedAtomicActionsMap.put(deDupedAction.build(), mergedActions);
        });

        // ---- Primary atomic action
        // One atomic resize per aggregate target
        Action.Builder mergedAction = createAtomicResizeAction(aggregatedAction.targetName(),
                                                                aggregatedAction.targetEntity(),
                                                                allResizeInfos,
                                                                allTargets, allExplanations);

        // This is required for executing the action in the automatic mode
        mergedAction.setExecutable(true);

        logger.trace("Created Atomic ActionDTO {} with target {} containing total resizes {}",
                mergedAction.getId(), aggregatedAction.targetName(),
                mergedAction.getInfo().getAtomicResize().getResizesCount());

        // -- The complete aggregation result
        AtomicActionResult atomicActionResult =
                ImmutableAtomicActionResult.builder()
                        .atomicAction(mergedAction.build())
                        .deDuplicatedActions(deDupedAtomicActionsMap)
                        .mergedActions(aggregatedAction.aggregateOnlyActions())
                        .build();

        logger.trace("{}: number of de-duplication targets {}", atomicActionResult.atomicAction().getId(),
                atomicActionResult.mergedActions().size());

        List<Action> mergedActions =  aggregatedAction.getAllActions();

        logger.trace("{}: merged {} actions to {} resize items, total {} targets, {} de-duplication targets",
                atomicActionResult.atomicAction().getId(),
                mergedActions.size(),   //total number of all the actions merged
                atomicActionResult.atomicAction().getInfo().getAtomicResize().getResizesCount(),    //total of internal resize infos
                allTargets.size(),  //total number of de-duplication and non-de-duplication targets
                atomicActionResult.deDuplicatedActions().size()); // total number of the de-duplication targets

        return atomicActionResult;
    }

    /**
     * Convenience method to construct the action DTO for an atomic resize.
     * @param target        display name for the execution target entity
     * @param targetEntity  the execution target entity
     * @param mergedResizeInfos the list of {@link ResizeInfo} that will be merged and executed
     *                              atomically by this action
     * @param mergedEntities    the list of the target entities that will handle each of the resizes
     * @param explanations      the list of explanation for each of the resizes
     * @return  the {@link ActionDTO.Action} for the atomic resize
     */
    private Action.Builder createAtomicResizeAction(String target, ActionEntity targetEntity,
                                                    List<ResizeInfo> mergedResizeInfos,
                                                    Collection<Long> mergedEntities,
                                                    List<AtomicResizeExplanation.ResizeExplanationPerEntity>
                                                            explanations) {

        AtomicResize.Builder atomicResizeInfoBuilder = ActionDTO.AtomicResize.newBuilder()
                                                            .setExecutionTarget(targetEntity)
                                                            .addAllResizes(mergedResizeInfos);

        Explanation.Builder explanation = Explanation.newBuilder()
                                             .setAtomicResize(AtomicResizeExplanation.newBuilder()
                                                        .setMergeGroupId(target)
                                                        .addAllPerEntityExplanation(explanations)
                                                        .addAllEntityIds(mergedEntities));


        Action.Builder atomicResize = Action.newBuilder()
                                            .setId(IdentityGenerator.next())
                                            .setExplanation(explanation)
                                            .setDeprecatedImportance(1.0)
                                            .setExecutable(true)
                                            .setInfo(ActionInfo.newBuilder()
                                            .setAtomicResize(atomicResizeInfoBuilder)
                                            .build());

        return atomicResize;
    }

    /**
     * Single Resize info result for the actions that were de-duplicated
     * and will be merged inside a atomic action.
     */
    @Value.Immutable
    interface ResizeInfoAndExplanation {
        // Resize Info
        ActionDTO.ResizeInfo resizeInfo();

        // Explanation for the resize
        ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity explanation();
    }

    /**
     * Create one merged resize info and explanation for the given set of actions.
     *
     * @param targetName        display name of the target which will execute the resize
     * @param targetEntity      {@link ActionEntity} that will execute the resize
      * @param resizeCommType   resize {@link CommodityType}
     * @param actionList        list of market actions belonging to different entities
     *                          and containing identical resize info
     * @return  a single {@link ResizeInfo} and explanation for this set of actions.
      */
    private ResizeInfoAndExplanation resizeInfoAndExplanation(String targetName,
                                                                    ActionEntity targetEntity,
                                                                    CommodityType resizeCommType,
                                                                    List<Action> actionList) {
        logger.trace("{}::{} : creating one resizeInfo for {} actions",
                                        targetName, resizeCommType, actionList.size());

        // source entities
        List<ActionEntity> originalEntities = actionList.stream()
                .map(action -> action.getInfo().getResize().getTarget())
                .collect(Collectors.toList());
        Set<Long> entityIds = originalEntities.stream().map(a -> a.getId())
                .collect(Collectors.toSet());

        Action origAction = actionList.iterator().next();
        ActionDTO.Resize origResize = origAction.getInfo().getResize();

        // ResizeInfo object
        ActionDTO.ResizeInfo.Builder resizeInfoBuilder =
                ActionDTO.ResizeInfo.newBuilder()
                        .setTarget(targetEntity)
                        .addAllSourceEntities(originalEntities)
                        .setCommodityType(resizeCommType)
                        .setCommodityAttribute(origResize.getCommodityAttribute())
                        .setOldCapacity(origResize.getOldCapacity())
                        .setNewCapacity(origResize.getNewCapacity());

        ActionDTO.ResizeInfo resizeInfo = resizeInfoBuilder.build();

        // Explanation for this resize
        AtomicResizeExplanation.ResizeExplanationPerEntity explanationPerEntity
                = AtomicResizeExplanation.ResizeExplanationPerEntity.newBuilder()
                .setEntityId(targetName)
                .addAllResizeEntityIds(entityIds)
                .setPerCommodityExplanation(
                        ResizeExplanationPerEntity.ResizeExplanationPerCommodity.newBuilder()
                                .setCommodityType(resizeCommType))
                .build();

        return ImmutableResizeInfoAndExplanation.builder()
                .resizeInfo(resizeInfo)
                .explanation(explanationPerEntity).build();
    }

    // Create one merged resize info and explanation for the given set of actions
    private List<ResizeInfoAndExplanation> deDuplicatedResizeInfoAndExplanation(DeDupedActions deDupedActions) {
        List<ResizeInfoAndExplanation>  result = new ArrayList<>();
        Map<CommodityType, List<Action>> actionsByCommMap = deDupedActions.actions().stream()
                .collect(Collectors.groupingBy(action -> action.getInfo().getResize().getCommodityType()));

        // Create one resize info all the de-duplicated actions per commodity
        for (CommodityType resizeCommType : actionsByCommMap.keySet()) {
            ResizeInfoAndExplanation resizeInfoAndExplanation
                    = resizeInfoAndExplanation(deDupedActions.targetName(),
                                                deDupedActions.targetEntity(), resizeCommType,
                                                actionsByCommMap.get(resizeCommType));

            result.add(resizeInfoAndExplanation);
        }

        return result;
    }
}

