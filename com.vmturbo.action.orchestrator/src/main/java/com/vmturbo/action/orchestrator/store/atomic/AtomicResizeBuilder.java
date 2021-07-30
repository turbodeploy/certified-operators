package com.vmturbo.action.orchestrator.store.atomic;

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
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.atomic.AggregatedAction.DeDupedActions;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory.AtomicActionResult;
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
    protected AggregatedAction aggregatedAction;

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
        // Map of de-duplicated atomic action and the corresponding list of original resize actions
        // for which the atomic action was created
        Map<Action, List<Action>> deDupedAtomicActionsMap = new HashMap<>();

        // Components of the Aggregated atomic action
        // resize info and explanation for each de-duplicated action
        List<ResizeInfoAndExplanation> allResizeInfoAndExplanations = new ArrayList<>();
        // target entity for the individual resize infos
        Set<Long> allTargets = new HashSet<>();

        final EntityType aggregatedActionTargetEntityType = getAggregateActionTargetEntityType();

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
                        = resizeInfoAndExplanation(entity.toString(), entity, resizeInfo.getCommodityType(),
                                        Collections.singletonList(resizeAction));
            if (resizeInfoAndExplanation != null) {
                allResizeInfoAndExplanations.add(resizeInfoAndExplanation);
                allTargets.add(entity.getId());
            }
        }

        // ---- First step of de-duplication
        // For actions that are to be de-duplicated, create a resize info per de-dup target
        // and commodity type. Also create a corresponding non-executable atomic action.
        aggregatedAction.deDupedActionsMap().forEach((deDupTargetOid, deDupedActions) -> {
            // First create the resize info objects per de-dup target and commodity type
            List<ResizeInfoAndExplanation> resizeInfoAndExplanations
                    = deDuplicatedResizeInfoAndExplanation(deDupedActions);

            if (resizeInfoAndExplanations.isEmpty()) {
                logger.trace("cannot create atomic action for de-duplication target {}",
                                                deDupedActions.targetName());
                return;
            }
            final EntityType deDupedActionsTargetEntityType
                                        = getDeDupedActionTargetEntityType(deDupedActions);

            // select resize info for creating the atomic action for the de-duplication level
            List<ResizeInfoAndExplanation> resizeInfoAndExplanationForDeDuplication
                    = selectResizesForDeDuplication(deDupedActions, resizeInfoAndExplanations);

            int numOfResizeInfosNotInDeduplicateAtomicAction
                    = resizeInfoAndExplanations.size() - resizeInfoAndExplanationForDeDuplication.size();
            // ---- atomic action for this de-duplication target (for displaying in the UI only)
            // comprises of the resizes for commodity types for actions based on execution mode
            if (resizeInfoAndExplanationForDeDuplication.size() > 0) {
                Action.Builder deDupedAction = createAtomicResizeAction(deDupedActions.targetName(),
                                deDupedActions.targetEntity(),
                                resizeInfoAndExplanationForDeDuplication,
                                Collections.singletonList(deDupTargetOid));
                List<Action> mergedActions = deDupedActions.actions();
                deDupedAtomicActionsMap.put(deDupedAction.build(), mergedActions);

                logger.trace("{} of {} de-duplicated resizes on {}:{} will not be merged on {} {}",
                        numOfResizeInfosNotInDeduplicateAtomicAction, resizeInfoAndExplanations.size(),
                            deDupedActionsTargetEntityType, deDupedActions.targetName(),
                            aggregatedActionTargetEntityType, aggregatedAction.targetName());
            }

            // Select resize infos and explanations that will be allowed to be aggregated to the top level atomic action
            List<ResizeInfoAndExplanation> resizeInfoAndExplanationForAggregation
                    = selectResizesForAggregation(deDupedActions, resizeInfoAndExplanations);
            if (resizeInfoAndExplanationForAggregation.size() > 0) {
                allResizeInfoAndExplanations.addAll(resizeInfoAndExplanationForAggregation);
                allTargets.add(deDupTargetOid);
            }
        });

        if (allResizeInfoAndExplanations.isEmpty()) {
            logger.trace("Cannot create atomic action for {} {} because all resize actions are in recommend mode",
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

        // ---- Second step of aggregation
        // ---- Primary atomic action
        // One atomic resize per aggregate target
        Action.Builder mergedAction = createAtomicResizeAction(aggregatedAction.targetName(),
                                                                aggregatedAction.targetEntity(),
                                                                allResizeInfoAndExplanations,
                                                                allTargets);

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

        List<Action> mergedActions = aggregatedAction.getAllActions();

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
     * @param target                    display name for the execution target entity
     * @param targetEntity              the execution target entity
     * @param resizeInfoAndExplanations list of {@link ResizeInfoAndExplanation} comprising of the
     *                                  {@link ResizeInfo} that will be executed atomically by this action
     *                                  and its corresponding explanation
     * @param mergedEntities            the list of the oid's of the target entities
     *                                  that will handle each of the resizes
     *
     * @return  the {@link ActionDTO.Action} for the atomic resize
     */
    private Action.Builder createAtomicResizeAction(String target, ActionEntity targetEntity,
                                                    List<ResizeInfoAndExplanation> resizeInfoAndExplanations,
                                                    Collection<Long> mergedEntities) {

        // Extract the list of ResizeInfos
        List<ActionDTO.ResizeInfo> mergedResizeInfos = resizeInfos(resizeInfoAndExplanations);

        // Extract the list of explanation for each of the ResizeInfos
        List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity>
                explanations = explanations(resizeInfoAndExplanations);

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
     * Select the de-duplicated resize infos and explanations for aggregation.
     * Only the selected de-duplicated resizes will be aggregated to the top level aggregated atomic action.
     *
     * <p>For real time topologies, only the deduplicated resizes for resize actions that are
     * <em>not</em> in recommend mode are aggregated to the top level atomic action.
     *
     * <p/>
     * For example, WorkloadController has 2 ContainerSpecs "foo" and "bar", all resize actions in
     * ContainerSpec "foo" are in manual mode, while ContainerSpec "bar" has VCPU resize in "recommend"
     * mode and VMem resize in "manual" mode.
     * <ul> In this case -
     * <li>all resize infos of ContainerSpec "foo" and only VMem resize info of ContainerSpec "bar"
     * will be used to create the the aggregation atomic action on the WorkloadController.
     * <li>The VCPU resize info of ContainerSpec "bar" will be used to create the atomic action on ContainerSpec "bar".
     * </ul>
     *
     * @param deDupedActions            {@link AggregatedAction.DeDupedActions} which comprise of all resize actions
     *                                  that will be de-duplicated to a single de-duplication target entity
     * @param resizeInfoAndExplanations     List of {@link ResizeInfoAndExplanation} for the single de-duplicate target entity
     *
     * @return  List of ResizeInfoAndExplanation that will be part of the aggregation level atomic action
     */
    List<ResizeInfoAndExplanation> selectResizesForAggregation(@Nonnull DeDupedActions deDupedActions,
                                                               @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        Set<Long> nonRecommendedActionIds = getNonRecommendModeActionIds();
        List<ResizeInfoAndExplanation> nonRecommendedResizeInfoAndExplanation =
                selectResizeInfoAndExplanation(nonRecommendedActionIds,
                                                deDupedActions.actions(), resizeInfoAndExplanations);
        logger.trace("{}::{}: merges {} of {} resizes after de-dup to {}::{}",
                getAggregateActionTargetEntityType(), aggregatedAction.targetName(),
                nonRecommendedResizeInfoAndExplanation.size(), resizeInfoAndExplanations.size(),
                getDeDupedActionTargetEntityType(deDupedActions), deDupedActions.targetName());
        return nonRecommendedResizeInfoAndExplanation;
    }

    /**
     * Select the resize infos and explanations for de-duplicated atomic action.
     * Only the selected resizes will be used to create the de-duplication level atomic action.
     *
     * <p>For real time topologies, only the deduplicated resizes for resize actions that are
     * in <em>recommend</em> mode are used to create the de-duplication level atomic action.
     *
     * <p/>
     * For example, WorkloadController has 2 ContainerSpecs "foo" and "bar", all resize actions in
     * ContainerSpec "foo" are in manual mode, while ContainerSpec "bar" has VCPU resize in "recommend"
     * mode and VMem resize in "manual" mode.
     * <ul> In this case -
     * <li>Only the VCPU resize info of ContainerSpec "bar" will be used to create
     * the atomic action on ContainerSpec "bar".
     * <li>All resize infos ContainerSpec "foo" and VMem resize of ContainerSpec "bar" will be used to
     * create the the aggregation atomic action on the WorkloadController.
     * </ul>
     *
     * @param deDupedActions            {@link AggregatedAction.DeDupedActions} which comprise of all resize actions
     *                                  that will be de-duplicated to a single de-duplication target entity
     * @param resizeInfoAndExplanations     List of {@link ResizeInfoAndExplanation} for the single de-duplicate target e
     *
     * @return  List of ResizeInfoAndExplanation that will be part of the de-duplication level atomic action
     */
    List<ResizeInfoAndExplanation> selectResizesForDeDuplication(@Nonnull DeDupedActions deDupedActions,
                                                                 @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        Set<Long> recommendedActionIds = getRecommendModeActionIds();
        List<ResizeInfoAndExplanation> recommendedResizeInfoAndExplanation =
                selectResizeInfoAndExplanation(recommendedActionIds,
                                                deDupedActions.actions(), resizeInfoAndExplanations);
        int totalActions = deDupedActions.actions().size();
        logger.trace("{}: de-duplicated {} of {} actions to {} resizes",
                deDupedActions.targetName(), recommendedActionIds.size(), totalActions,
                recommendedResizeInfoAndExplanation.size());
        return recommendedResizeInfoAndExplanation;
    }

    /**
     * Select {@link ResizeInfoAndExplanation} that are created for the actions with the given action ids.
     *
     * @param actionIds List of action OIDs used to select the actions from the action list
     * @param actions   The list of actions used to create the resizeInfoAndExplanations
     * @param resizeInfoAndExplanations list of ResizeInfoAndExplanation
     *
     * @return list of ResizeInfoAndExplanation belonging to the ResizeInfo for the given action ids
     */
    private List<ResizeInfoAndExplanation>
    selectResizeInfoAndExplanation(@Nonnull Set<Long> actionIds,
                                   @Nonnull List<Action> actions,
                                   @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        // Set of commodity types where corresponding resize actions belong to the given action Ids
        Set<Integer> selectedCommTypes = actions.stream()
                .filter(a -> actionIds.contains(a.getId()))
                .map(a -> a.getInfo().getResize().getCommodityType().getType())
                .collect(Collectors.toSet());
        // Return list of resizeInfoAndExplanations for the above commodity types
        return resizeInfoAndExplanations.stream()
                .filter(resizeInfoAndExplanation ->
                        selectedCommTypes.contains(resizeInfoAndExplanation.resizeInfo().getCommodityType().getType()))
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
     * Return the set of action IDs of the original resizes that are not in recommend mode.
     * @return the set of action IDs of non-recommend mode resize actions
     */
    Set<Long> getNonRecommendModeActionIds() {
        // Set of action ID of individual container resizes not in recommend mode.
        return aggregatedAction.actionViews.entrySet().stream()
                .filter(entry -> entry.getValue().getMode() != ActionMode.RECOMMEND)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Return the set of action IDs of the original resizes that are in recommend mode.
     * @return the set of action IDs of Recommend mode resize actions
     */
    Set<Long> getRecommendModeActionIds() {
        // Set of action ID of individual container resizes in recommend mode.
        return aggregatedAction.actionViews.entrySet().stream()
                .filter(entry -> entry.getValue().getMode() == ActionMode.RECOMMEND)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
    }

     List<ActionDTO.ResizeInfo> resizeInfos(List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        return  resizeInfoAndExplanations.stream()
                .map(ResizeInfoAndExplanation::resizeInfo)
                .collect(Collectors.toList());
    }

    List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity> explanations(
            List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        return resizeInfoAndExplanations.stream()
                .map(ResizeInfoAndExplanation::explanation)
                .collect(Collectors.toList());
    }

    EntityType getAggregateActionTargetEntityType() {
        return EntityType.forNumber(aggregatedAction.targetEntity().getType());
    }

    EntityType getDeDupedActionTargetEntityType(DeDupedActions deDupedActions) {
        return EntityType.forNumber(deDupedActions.targetEntity().getType());
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
    @Nullable
    private ResizeInfoAndExplanation resizeInfoAndExplanation(@Nonnull final String targetName,
                                                              @Nonnull final ActionEntity targetEntity,
                                                              @Nonnull final CommodityType resizeCommType,
                                                              @Nonnull final List<Action> actionList) {
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
        Explanation.ResizeExplanation origResizeExplanation = origAction.getExplanation().getResize();

        // ResizeInfo object for atomic resize by de-duplicating the list of given actions
        ActionDTO.ResizeInfo.Builder resizeInfoBuilder =
                ActionDTO.ResizeInfo.newBuilder()
                        .setTarget(targetEntity)
                        .addAllSourceEntities(originalEntities)
                        .setCommodityType(resizeCommType)
                        .setCommodityAttribute(origResize.getCommodityAttribute())
                        .setOldCapacity(origResize.getOldCapacity())
                        .setNewCapacity(origResize.getNewCapacity());

        ActionDTO.ResizeInfo resizeInfo = resizeInfoBuilder.build();

        // Explanation for this atomic resize
        AtomicResizeExplanation.ResizeExplanationPerEntity.Builder explanationPerEntity
                = AtomicResizeExplanation.ResizeExplanationPerEntity.newBuilder()
                .setTargetId(targetEntity.getId())
                .addAllResizeEntityIds(entityIds);

        ResizeExplanationPerEntity.ResizeExplanationPerCommodity.Builder expPerComm =
                ResizeExplanationPerEntity.ResizeExplanationPerCommodity.newBuilder()
                        .setCommodityType(resizeCommType);
        if (origResizeExplanation.hasReason()) {
            expPerComm.setReason(origResizeExplanation.getReason());
        } else {
            expPerComm.setReason(resizeCommType);
        }

        explanationPerEntity.setPerCommodityExplanation(expPerComm);

        return ImmutableResizeInfoAndExplanation.builder()
                .resizeInfo(resizeInfo)
                .explanation(explanationPerEntity.build()).build();
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
            if (resizeInfoAndExplanation != null) {
                result.add(resizeInfoAndExplanation);
            }
        }

        return result;
    }
}

