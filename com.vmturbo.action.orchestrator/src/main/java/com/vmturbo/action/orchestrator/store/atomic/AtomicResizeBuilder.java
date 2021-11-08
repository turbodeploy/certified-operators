package com.vmturbo.action.orchestrator.store.atomic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

/**
 * Builder class to build Atomic Resize actions.
 */
class AtomicResizeBuilder implements AtomicActionBuilder {

    private final Logger logger = LogManager.getLogger();

    // Class representing the aggregated/de-duplicated actions and the associated execution target
    protected AggregatedAction aggregatedAction;
    protected Set<Long> executableActions;
    protected Set<Long> nonExecutableActions;

    /**
     * Constructor for a new builder.
     *
     * @param aggregatedAction  AggregatedAction containing the relevant information
     *                              for creating the atomic action
     */
    AtomicResizeBuilder(@Nonnull AggregatedAction aggregatedAction) {
        this.aggregatedAction = aggregatedAction;
        executableActions = getExecutableActionIds();
        nonExecutableActions = getNonExecutableActionIds();
    }

    protected AtomicResizeBuilder() {}

    /**
     * Create the atomic action DTOs for the executable and non-executable aggregated actions.
     *
     * <p>For real time topologies, only the deduplicated resizes for resize actions that are
     * <em>not</em> in recommend mode are aggregated to the top level atomic action.
     *
     * <p/>
     * For example, WorkloadController has 2 ContainerSpecs "foo" and "bar", all resize actions in
     * ContainerSpec "foo" are executable, while ContainerSpec "bar" has a VMem resize in "manual" mode
     * which is also executable but the VCPU resize is non-executable due to being in "recommend" mode.
     * <ul> In this case -
     * <li>All resize infos of ContainerSpec "foo" and only VMem resize info of ContainerSpec "bar"
     * will be aggregated and will result in an  <b>executable</b> atomic action on the WorkloadController.
     * <li>The VCPU resize info of ContainerSpec "bar" will be aggregated and
     * will result in a <b>non-executable</b> atomic action on the WorkloadController.
     * </ul>
     *
     * <p/>
     * Another example, WorkloadController has 2 ContainerSpecs "foo" and "bar", all resize actions in
     * ContainerSpec "foo" are executable, while ContainerSpec "bar" resizes are blocked due to
     * Namespace resize action and hence these resizes are non-executable.
     * <ul> In this case -
     * <li>All resize infos of ContainerSpec "foo" will be aggregated
     * and will result in an  <b>executable</b> atomic action on the WorkloadController.
     * <li>The resizes infos of ContainerSpec "bar" will be aggregated and
     * will result in a <b>non-executable</b> atomic action on the WorkloadController.
     * </ul>
     *
     * <p/>
     * Another example, WorkloadController has 2 ContainerSpecs "foo" and "bar", all resize actions in
     * ContainerSpec "foo" are executable, while ContainerSpec "bar" has VCPU resize which is also executable
     * <ul> In this case -
     * <li>All resize infos of ContainerSpec "foo" and "bar" will be aggregated
     * and will result in an <b>executable</b> atomic action on the WorkloadController.
     * </ul>
     *
     * <p/>
     * Another example, WorkloadController has 2 ContainerSpecs "foo" and "bar".
     * The execution policy is set to "Recommend" for both the specs
     * <ul> In this case -
     * <li>All resize infos of ContainerSpec "foo" and "bar" will be aggregated
     * and will result in an <b>non-executable</b> atomic action on the WorkloadController.
     * </ul>
     *
     * <p/>
     * Another example, in a Plan WorkloadController has 2 ContainerSpecs "foo" and "bar", all resize actions in
     * ContainerSpec "foo" are non-executable, while ContainerSpec "bar" has VCPU resize which is also non-executable
     * <ul> In this case -
     * <li>All resize infos of ContainerSpec "foo" and "bar" will be aggregated
     * and will result in an <b>non-executable</b> atomic action on the WorkloadController.
     * </ul>
     *
     * @return {@link AtomicActionResult} containing the {@link ActionDTO.Action} for the
     *          executable aggregated action and the {@link ActionDTO.Action} for the non-executable
     *          aggregated actions.
     */
    @Override
    public Optional<AtomicActionResult> build() {
        // Components of the Aggregated atomic action
        // resize info and explanation for each de-duplicated action
        List<ResizeInfoAndExplanation> executableResizeInfoAndExplanations = new ArrayList<>();
        List<ResizeInfoAndExplanation> nonExecutableResizeInfoAndExplanations = new ArrayList<>();

        // target entity for the individual resize infos
        Set<Long> executableTargets = new HashSet<>();
        Set<Long> nonExecutableTargets = new HashSet<>();

        // Actions that are not duplicated, create a resizeInfo corresponding to each.
        // Use the action entity as the target entity for the de-duplication step
        // which will create the ResizeInfo corresponding to the action
        // All resize infos will be aggregated to either an executable or non-executable atomic action
        for (Action resizeAction : aggregatedAction.actionsWithoutDeDuplicationTarget()) {
            Resize resizeInfo = resizeAction.getInfo().getResize();
            ActionEntity entity = resizeInfo.getTarget();
            ActionView actionView = aggregatedAction.actionViews.get(resizeAction.getId());
            if (actionView == null) {
                continue;
            }

            ResizeInfoAndExplanation resizeInfoAndExplanation
                        = resizeInfoAndExplanation(entity.toString(), entity, resizeInfo.getCommodityType(),
                                        Collections.singletonList(resizeAction));
            if (executableActions.contains(actionView.getRecommendation().getId())) {
                executableResizeInfoAndExplanations.add(resizeInfoAndExplanation);
                executableTargets.add(entity.getId());
            } else {
                nonExecutableResizeInfoAndExplanations.add(resizeInfoAndExplanation);
                nonExecutableTargets.add(entity.getId());
            }
        }

        // ---- First step of de-duplication
        // For actions that are to be de-duplicated, create a resize info per de-dup target
        // and commodity type.
        aggregatedAction.deDupedActionsMap().forEach((deDupTargetOid, deDupedActions) -> {
            // First create the resize info objects for each commodity type for this de-duplication target
            List<ResizeInfoAndExplanation> resizeInfoAndExplanations
                    = deDuplicatedResizeInfoAndExplanation(deDupedActions);

            if (resizeInfoAndExplanations.isEmpty()) {
                logger.trace("cannot create atomic action for de-duplication target {}",
                                                deDupedActions.targetName());
                return;
            }

            List<ResizeInfoAndExplanation> executableResizes = selectExecutableResizes(deDupedActions, resizeInfoAndExplanations);
            List<ResizeInfoAndExplanation> nonExecutableResizes = selectNonExecutableResizes(deDupedActions, resizeInfoAndExplanations);
            if (executableResizes.size() > 0) {
                executableResizeInfoAndExplanations.addAll(executableResizes);
                executableTargets.add(deDupTargetOid);
            }
            if (nonExecutableResizes.size() > 0) {
                nonExecutableResizeInfoAndExplanations.addAll(nonExecutableResizes);
                nonExecutableTargets.add(deDupTargetOid);
            }
        });

        // ---- Second step of aggregation
        // ---- Executable atomic action that aggregates the commodity resizes that can executed
        ImmutableAtomicActionResult.Builder atomicActionResultBuilder = ImmutableAtomicActionResult.builder()
                                                                            .atomicAction(Optional.empty())
                                                                            .nonExecutableAtomicAction(Optional.empty());

        if (!executableResizeInfoAndExplanations.isEmpty()) {
            Action.Builder mergedActionBuilder = createAtomicResizeAction(aggregatedAction.targetName(),
                                                                            aggregatedAction.targetEntity(),
                                                                            executableResizeInfoAndExplanations,
                                                                            executableTargets);

            // This is required for executing the action in the automatic mode
            mergedActionBuilder.setExecutable(true);

            atomicActionResultBuilder.atomicAction(mergedActionBuilder.build());
            logger.trace("Created Executable Atomic ActionDTO {} with target {} containing total resizes {}",
                    mergedActionBuilder::getId, aggregatedAction::targetName,
                    () -> mergedActionBuilder.getInfo().getAtomicResize().getResizesCount());
        }

        // ---- Non-executable atomic action that aggregates the commodity resizes that are non-executable
        if (!nonExecutableResizeInfoAndExplanations.isEmpty()) {
            Action.Builder nonExecutableMergedAction = createAtomicResizeAction(aggregatedAction.targetName(),
                                                                                aggregatedAction.targetEntity(),
                                                                                nonExecutableResizeInfoAndExplanations,
                                                                                nonExecutableTargets);
            // This is used to set the recommend mode on the action
            nonExecutableMergedAction.setExecutable(false);

            atomicActionResultBuilder.nonExecutableAtomicAction(nonExecutableMergedAction.build());

            logger.trace("Created Non-Executable Atomic ActionDTO {} with target {} containing total resizes {}",
                    nonExecutableMergedAction::getId, aggregatedAction::targetName,
                    () -> nonExecutableMergedAction.getInfo().getAtomicResize().getResizesCount());
        }

        // -- The complete aggregation result
        Map<ActionEntity, List<ActionDTO.Action>> deduplicatedActionMap
                = aggregatedAction.deDupedActionsMap().values().stream()
                .collect(Collectors.toMap(value -> value.targetEntity(), value -> value.actions()));

        AtomicActionResult atomicActionResult = atomicActionResultBuilder
                .aggregationTarget(aggregatedAction.targetEntity())
                .deDuplicatedActions(deduplicatedActionMap)
                .mergedActions(aggregatedAction.actionsWithoutDeDuplicationTarget())
                .build();

        atomicActionResult.atomicAction().ifPresent( atomicAction ->
                logger.trace("executable atomic {}: number of resizes {}",
                () -> atomicAction.getId(),
                () -> atomicAction.getInfo().getAtomicResize().getResizesCount()));

        atomicActionResult.nonExecutableAtomicAction().ifPresent( atomicAction ->
            logger.trace("non-executable atomic {}: number of resizes {}",
                    () -> atomicAction.getId(),
                    () -> atomicAction.getInfo().getAtomicResize().getResizesCount()));


        logger.debug("{}: merged executable {} actions to {} resize items on {} targets, "
                        + "merged non-executable {} actions to {} resize items on {} targets",
                aggregatedAction::targetName,
                () -> executableActions,   //total number of executable actions merged
                () -> getAtomicResizeCount(atomicActionResult, true),    //total of internal resize infos
                () -> executableTargets.size(), //total number of executable action targets
                () -> nonExecutableActions,   //total number of non-executable actions merged
                () -> getAtomicResizeCount(atomicActionResult, false), //total of internal resize infos
                () -> nonExecutableTargets.size()  //total number of non-executable action targets
        );

        return Optional.of(atomicActionResult);
    }

    private int getAtomicResizeCount(AtomicActionResult atomicActionResult, boolean executable) {
        ActionDTO.Action action = null;
        if (executable && atomicActionResult.atomicAction().isPresent()) {
            action = atomicActionResult.atomicAction().get();
        } else if (!executable && atomicActionResult.nonExecutableAtomicAction().isPresent()) {
            action = atomicActionResult.nonExecutableAtomicAction().get();
        }
        if (action == null) {
            return 0;
        }
        if (action.getInfo().hasAtomicResize()) {
            return  action.getInfo().getAtomicResize().getResizesCount();
        }
        return 0;
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
     * Select resizes that can be executed using the executable action ids.
     *
     * <p>For real time topology, the execution mode of a resize is determined either by the policy setting
     * or based on a related action on a provider that can cause this action to be blocked making it non-executable.
     *
     * @param deDupedActions            {@link AggregatedAction.DeDupedActions} which comprises all resize actions
     *                                  that will be de-duplicated to a single de-duplication target entity
     * @param resizeInfoAndExplanations List of {@link ResizeInfoAndExplanation} for the single de-duplicate target
     *
     * @return List of ResizeInfoAndExplanation that will be part of the executable aggregated action
     */
    List<ResizeInfoAndExplanation> selectExecutableResizes(@Nonnull DeDupedActions deDupedActions,
                                                                 @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        List<ResizeInfoAndExplanation> executableResizeInfoAndExplanation =
                selectResizeInfoAndExplanation(executableActions,
                        deDupedActions.actions(), resizeInfoAndExplanations);
        int totalActions = deDupedActions.actions().size();
        logger.trace("{}: de-duplicated {} executable of {} actions to {} resizes",
                deDupedActions::targetName, () -> executableActions.size(),
                () -> totalActions, () -> executableResizeInfoAndExplanation.size());
        return executableResizeInfoAndExplanation;
    }

    /**
     * Select resizes that cannot be executed using the non-executable action ids.
     *
     * <p>For real time topology, the execution mode of a resize is determined either by the policy setting
     * or based on a related action on a provider that can cause this action to be blocked making it non-executable.
     *
     * @param deDupedActions            {@link AggregatedAction.DeDupedActions} which comprises all resize actions
     *                                  that will be de-duplicated to a single de-duplication target entity
     * @param resizeInfoAndExplanations List of {@link ResizeInfoAndExplanation} for the single de-duplicate target
     *
     * @return List of ResizeInfoAndExplanation that will be part of the non-executable aggregated action
     */
    List<ResizeInfoAndExplanation> selectNonExecutableResizes(@Nonnull DeDupedActions deDupedActions,
                                                           @Nonnull List<ResizeInfoAndExplanation> resizeInfoAndExplanations) {
        List<ResizeInfoAndExplanation> nonExecutableResizeInfoAndExplanation =
                selectResizeInfoAndExplanation(nonExecutableActions,
                        deDupedActions.actions(), resizeInfoAndExplanations);
        int totalActions = deDupedActions.actions().size();
        logger.trace("{}: de-duplicated {} non-executable of {} actions to {} resizes",
                deDupedActions::targetName, () -> nonExecutableActions.size(),
                () -> totalActions, () -> nonExecutableResizeInfoAndExplanation.size());
        return nonExecutableResizeInfoAndExplanation;
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
        // TODO: if selectedCommTypes is empty ??
        if (selectedCommTypes.isEmpty()) {
            return new ArrayList<>();
        }
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
     * Return the set of action OIDs of the original resizes that are executable.
     *
     * <p>For real time topologies, this is will be based on the action policy setting on the entity.
     *
     * @return the set of action OIDs of executable resize actions
     */
    protected Set<Long> getExecutableActionIds() {
        // Set of action ID of individual container resizes not in recommend mode.
        return aggregatedAction.actionViews.entrySet().stream()
                .filter(entry -> entry.getValue().getMode() != ActionMode.RECOMMEND)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Return the set of action OIDs of the original resizes that are non-executable.
     *
     * <p>For real time topologies, this is will be based on the action policy setting on the entity.
     * Some resize actions can be non-executable if they are blocked by a related provider action.
     *
     * @return the set of action OIDs of non-executable resize actions
     */
    protected Set<Long> getNonExecutableActionIds() {
        Set<Long> nonExecutableActions = aggregatedAction.getAllActions().stream()
                .filter(a -> a.hasExecutable() && a.getExecutable() == false)
                .map(a -> a.getId())
                .collect(Collectors.toSet());
        // Set of action ID of individual container resizes in recommend mode.
        Set<Long> recommendModeActions = aggregatedAction.actionViews.entrySet().stream()
                .filter(entry -> entry.getValue().getMode() == ActionMode.RECOMMEND)
                .map(Entry::getKey)
                .collect(Collectors.toSet());

        recommendModeActions.addAll(nonExecutableActions);

        return recommendModeActions;
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
    @Nonnull
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

        final float newCapacity =
            getResizeNewCapacity(origResize.getOldCapacity(), origResize.getNewCapacity(), actionList);

        // ResizeInfo object for atomic resize by de-duplicating the list of given actions
        ActionDTO.ResizeInfo.Builder resizeInfoBuilder =
                ActionDTO.ResizeInfo.newBuilder()
                        .setTarget(targetEntity)
                        .addAllSourceEntities(originalEntities)
                        .setCommodityType(resizeCommType)
                        .setCommodityAttribute(origResize.getCommodityAttribute())
                        .setOldCapacity(origResize.getOldCapacity())
                        .setNewCapacity(newCapacity);

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

    /**
     * Get resize new capacity based on given action list. In a homogeneous environment, new capacity
     * values are always consistent in actionList. While in a heterogeneous environment, with move
     * actions, it could be possible that resize actions of the same consistent scaling group have
     * different new capacity. In this case, to be conservative and avoid unexpected performance issue
     * after executing the action, always use the closet new capacity to the old capacity, which means
     * to resize up to the smallest possible value or to resize down to the largest possible value.
     * Note that old capacity values of a given action list are always consistent.
     *
     * @param oldCapacity Given old capacity of consistent scaling actions.
     * @param newCapacity Given new capacity of the first resize action in the action list.
     * @param actionList  Given resize action list from which final new capacity is extracted.
     * @return Final new capacity of action list to be used as new capacity of the constructed atomic
     * resize action. Final new capacity is the closet new capacity to old capacity from action list.
     */
    private float getResizeNewCapacity(final float oldCapacity, float newCapacity,
                                       @Nonnull final List<Action> actionList) {
        float minDiff = Math.abs(newCapacity - oldCapacity);
        for (Action action : actionList) {
            float capacity = action.getInfo().getResize().getNewCapacity();
            float diff = Math.abs(capacity - oldCapacity);
            if (diff < minDiff) {
                minDiff = diff;
                newCapacity = capacity;
            }
        }
        return newCapacity;
    }

    // Create a list of merged resize info and explanation for the given set of actions, one per commodity
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

