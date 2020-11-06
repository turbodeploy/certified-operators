package com.vmturbo.action.orchestrator.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.AggregatedAction.DeDupedActions;
import com.vmturbo.action.orchestrator.store.AtomicActionFactory.AtomicActionResult;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
    public Optional<AtomicActionResult> build() {
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
            ActionView actionView = aggregatedAction.actionViews.get(resizeAction.getId());
            if (actionView == null) {
                continue;
            }

            ResizeInfoAndExplanation resizeInfoAndExplanation
                        = resizeInfoAndExplanation(entity.toString(), entity,
                                        resizeInfo.getCommodityType(),
                                        Collections.singletonList(resizeAction));

            allResizeInfos.add(resizeInfoAndExplanation.resizeInfo());
            allExplanations.add(resizeInfoAndExplanation.explanation());
            allTargets.add(entity.getId());
        }

        // Set of action ID of individual container resizes not in recommend mode.
        Set<Long> nonRecommendedActionIds = aggregatedAction.actionViews.entrySet().stream()
            .filter(entry -> entry.getValue().getMode() != ActionMode.RECOMMEND)
            .map(Entry::getKey)
            .collect(Collectors.toSet());

        // For actions that are to be de-duplicated, create a resize info per de-dup target
        // and commodity type. Also create a corresponding non-executable atomic action.
        aggregatedAction.deDupedActionsMap().forEach((deDupTargetOid, deDupedActions) -> {
            // First create the resize info objects per de-dup target and commodity type
            List<ResizeInfoAndExplanation> resizeInfoAndExplanations
                    = deDuplicatedResizeInfoAndExplanation(deDupedActions);

            if (resizeInfoAndExplanations.isEmpty()) {
                logger.debug("cannot create atomic action for de-duplication target {}", deDupedActions.targetName());
                return;
            }

            List<ActionDTO.ResizeInfo> resizeInfos = resizeInfoAndExplanations.stream()
                    .map(ResizeInfoAndExplanation::resizeInfo)
                    .collect(Collectors.toList());
            List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity>
                    explanations = resizeInfoAndExplanations.stream()
                    .map(ResizeInfoAndExplanation::explanation)
                    .collect(Collectors.toList());

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
            // Add non recommended resize infos and explanations to allResizeInfos and allExplanations
            // to be merged to atomic actions.
            addNonRecommendedResizeInfoAndExplanation(nonRecommendedActionIds, deDupedActions,
                resizeInfoAndExplanations, allResizeInfos, allExplanations, allTargets);
        });

        if (allResizeInfos.isEmpty()) {
            logger.debug("Cannot create atomic action for {} {} because all resize actions are in recommend mode",
                EntityType.forNumber(aggregatedAction.targetEntity().getType()), aggregatedAction.targetName());
            // -- The complete aggregation result
            AtomicActionResult atomicActionResult =
                    ImmutableAtomicActionResult.builder()
                            .atomicAction(Optional.empty())
                            .deDuplicatedActions(deDupedAtomicActionsMap)
                            .mergedActions(aggregatedAction.aggregateOnlyActions())
                            .build();
            return Optional.of(atomicActionResult);
        }

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

        logger.trace("{}: number of de-duplication targets {}", atomicActionResult.atomicAction().get().getId(),
                atomicActionResult.mergedActions().size());

        List<Action> mergedActions =  aggregatedAction.getAllActions();

        logger.trace("{}: merged {} actions to {} resize items, total {} targets, {} de-duplication targets",
                atomicActionResult.atomicAction().get().getId(),
                mergedActions.size(),   //total number of all the actions merged
                atomicActionResult.atomicAction().get().getInfo().getAtomicResize().getResizesCount(),    //total of internal resize infos
                allTargets.size(),  //total number of de-duplication and non-de-duplication targets
                atomicActionResult.deDuplicatedActions().size()); // total number of the de-duplication targets

        return Optional.of(atomicActionResult);
    }

    /**
     * Convenience method to construct the action DTO for an atomic resize.
     *
     * @param target            display name for the execution target entity
     * @param targetEntity      the execution target entity
     * @param mergedResizeInfos the list of {@link ResizeInfo} that will be executed atomically by this action
     * @param mergedEntities    the list of the oid's of the target entities that will handle each of the resizes
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
     * Add list of de-duplicated resize infos and explanations to allResizeInfos and allExplanations
     * to be aggregated to atomic actions, where corresponding resize actions are not in recommend mode.
     * <p/>
     * For example, WorkloadController has 2 ContainerSpecs "foo" and "bar", all resize actions in
     * ContainerSpec "foo" are in manual mode, while ContainerSpec "bar" has VCPU resize in "recommend"
     * mode and VMem resize in "manual" mode. In this case, all resize infos of ContainerSpec "foo"
     * and only VMem resize info of ContainerSpec "bar" will be aggregated to an atomic action.
     * Meanwhile, all resize infos will still be added to the merged action on ContainerSpec level.
     *
     * @param nonRecommendedActionIds   Set of action IDs of resize actions in non recommend mode in
     *                                  aggregatedActions of this controller.
     * @param deDupedActions            De-duplicated actions which comprise of the resizes for all
     *                                  the commodity type.
     * @param resizeInfoAndExplanations Resize infos and explanations from de-duplicate actions.
     * @param allResizeInfos            List of resize infos to be aggregated to atomic actions. This
     *                                  includes only resize infos of non recommended resize actions.
     * @param allExplanations           List of resize explanations to be aggregated to atomic actions.
     *                                  This includes only explanations of non recommended resize actions.
     * @param allTargets                Set of all target entities of non recommended resize actions.
     */
    private void addNonRecommendedResizeInfoAndExplanation(@Nonnull Set<Long> nonRecommendedActionIds,
                                                           @Nonnull DeDupedActions deDupedActions,
                                                           @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations,
                                                           @Nonnull List<ActionDTO.ResizeInfo> allResizeInfos,
                                                           @Nonnull List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity>
                                                               allExplanations,
                                                           @Nonnull Set<Long> allTargets) {
        // Get list of ResizeInfoAndExplanation where corresponding resize action are not in
        // recommend mode.
        // De-duplicated resize infos and explanations will be aggregated to atomic action
        // only if the original resize actions are not in RECOMMEND mode.
        List<ResizeInfoAndExplanation> nonRecommendedResizeInfoAndExplanation =
            getNonRecommendedResizeInfoAndExplanation(nonRecommendedActionIds, deDupedActions, resizeInfoAndExplanations);
        if (nonRecommendedResizeInfoAndExplanation.size() > 0) {
            List<ActionDTO.ResizeInfo> nonRecommendedResizeInfos = nonRecommendedResizeInfoAndExplanation.stream()
                .map(ResizeInfoAndExplanation::resizeInfo)
                .collect(Collectors.toList());
            List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity>
                nonRecommendedExplanations = nonRecommendedResizeInfoAndExplanation.stream()
                .map(ResizeInfoAndExplanation::explanation)
                .collect(Collectors.toList());
            allResizeInfos.addAll(nonRecommendedResizeInfos);
            allExplanations.addAll(nonRecommendedExplanations);
            allTargets.add(deDupedActions.targetEntity().getId());

            final EntityType deDeupedActionsTargetEntityType =
                EntityType.forNumber(deDupedActions.targetEntity().getType());
            final EntityType aggregatedActionTargetEntityType =
                EntityType.forNumber(aggregatedAction.targetEntity().getType());
            logger.debug("{} {}: merges {} resizes after de-dup to {} {}", aggregatedActionTargetEntityType,
                aggregatedAction.targetName(), nonRecommendedResizeInfos.size(), deDeupedActionsTargetEntityType,
                deDupedActions.targetName());

            int recommendedResizeInfosCount = resizeInfoAndExplanations.size() - nonRecommendedResizeInfoAndExplanation.size();
            if (recommendedResizeInfosCount > 0) {
                logger.debug("{} {} has recommended actions", deDeupedActionsTargetEntityType,
                    deDupedActions.targetName());
                logger.debug("{} : {} recommended resize infos will not be merged on {} {}",
                    deDupedActions.targetName(), recommendedResizeInfosCount, aggregatedActionTargetEntityType,
                    aggregatedAction.targetName());
            }
        }
    }

    private List<ResizeInfoAndExplanation>
    getNonRecommendedResizeInfoAndExplanation(@Nonnull Set<Long> nonRecommendedActionIds,
                                              @Nonnull DeDupedActions deDupedActions,
                                              @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        // Set of commodity types where corresponding resize actions are not in recommend mode in
        // deDupedActions.
        Set<Integer> nonRecommendedCommTypes = deDupedActions.actions().stream()
            .filter(a -> nonRecommendedActionIds.contains(a.getId()))
            .map(a -> a.getInfo().getResize().getCommodityType().getType())
            .collect(Collectors.toSet());
        // Return list of resizeInfoAndExplanations where actions are not in recommend mode.
        return resizeInfoAndExplanations.stream()
            .filter(resizeInfoAndExplanation ->
                nonRecommendedCommTypes.contains(resizeInfoAndExplanation.resizeInfo().getCommodityType().getType()))
            .collect(Collectors.toList());
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

        List<Action> actionsToDeDuplicate = deDupedActions.actions();

        Map<CommodityType, List<Action>> actionsByCommMap = actionsToDeDuplicate.stream()
                .collect(Collectors.groupingBy(action -> action.getInfo().getResize().getCommodityType()));

        List<ResizeInfoAndExplanation>  result = new ArrayList<>();
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

