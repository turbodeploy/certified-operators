package com.vmturbo.action.orchestrator.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionEntity;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.ResizeMergeSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Merges a group of resize actions on different entities and
 * creates a Atomic Merged resize action for execution.
 */
public class AtomicResizeMerger implements AtomicActionMerger {

    private static final Logger logger = LogManager.getLogger();
    private final AtomicActionSpecsCache atomicActionSpecsCache;

    /**
     * Constructor for AtomicResizeTranslator.
     *
     * @param atomicActionSpecsCache cache with the action merge specs
     */
    public AtomicResizeMerger(AtomicActionSpecsCache atomicActionSpecsCache) {
        this.atomicActionSpecsCache = atomicActionSpecsCache;
    }

    /**
     * Handles resize actions for entity which has an action merge spec from its probe.
     * @param action Action to check.
     * @return boolean indicating if the action will merged by this atomic action translator
     */
    @Override
    public boolean appliesTo(@Nonnull final Action action) {

        final ActionInfo actionInfo = action.getInfo();
        boolean resizeAction = actionInfo.hasResize();
        if (!resizeAction) {
            return false;
        }
        // current entity in the action
        long entityOid = action.getInfo().getResize().getTarget().getId();

        // is there a resize merge spec for this entity
        if (!atomicActionSpecsCache.hasAtomicActionSpec(ActionType.RESIZE, entityOid)) {
            return false;
        }

        Resize resizeInfo = action.getInfo().getResize();
        CommodityDTO.CommodityType resizeCommType = CommodityDTO.CommodityType
                                                .forNumber(resizeInfo.getCommodityType().getType());

        AtomicActionSpec specsInfo = atomicActionSpecsCache.getAtomicResizeSpec(entityOid);

        return specsInfo.getResizeSpec().getCommodityDataList().stream()
                .anyMatch(commData -> commData.getCommodityType() == resizeCommType);
    }

    /**
     * Convenience class to hold the actions for de-duplication target.
     * De-duplication entity is the single target of a bunch of duplicate actions
     * for all the entities in a scaling group.
     */
    static class DeDupedActions {
        final ActionEntity deDupTarget;
        final String targetName;

        // Set of entity actions for the entities in the scaling/deployment group
        // grouped by commodity type
        Map<CommodityType, List<Action>> actionsByComm;

        DeDupedActions(ActionEntity deDupTarget, String targetName) {
            this.deDupTarget = deDupTarget;
            this.targetName = targetName;
            actionsByComm = new HashMap<>();
        }

        void addAction(CommodityType commType, Action action) {
            actionsByComm.computeIfAbsent(commType, v -> new ArrayList<>()).add(action);
        }

        List<Action> getAllActions() {
            List<Action> actions = actionsByComm.entrySet().stream()
                    .map(item -> item.getValue()).collect(Collectors.toList())
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

            return actions;
        }
    }

    /**
     * Convenience class to hold actions for an aggregation entity.
     */
    static class AggregatedAction {
        final ActionEntity aggregationTarget;
        final String targetName;
        Map<Long, DeDupedActions> deDupedActionsMap;

        AggregatedAction(ActionEntity aggregationTarget, String targetName) {
            this.aggregationTarget = aggregationTarget;
            this.targetName = targetName;
            deDupedActionsMap = new HashMap<>();
        }

        DeDupedActions getDeDupedActions(Long entityOid) {
            return deDupedActionsMap.get(entityOid);
        }

        void setDeDupedActions(DeDupedActions deDupedActions) {
            deDupedActionsMap.put(deDupedActions.deDupTarget.getId(), deDupedActions);
        }
    }

    /**
     * Merge and create Atomic resize action based on the {@link AtomicActionSpec} for the entities.
     * Actions are first grouped by de-duplication entity and then by aggregation entity.
     *  In the example of ,
     *  WorkloadController W1 --> owns -> ContainerSpecs CS1 --> aggregates --> Containers C1, C2, C3
     *  WorkloadController W1 --> owns -> ContainerSpecs CS2 --> aggregates --> Containers C4, C5, C6
     *  actions for C1, C2, C3, C4, C5, C6 are first grouped by container specs CS1, CS2
     *  and then by W1.
     *  The resizes for C1:Comm1, C1:Comm2, C2:Comm1, C2:Comm2, C3:Comm1, C3:Comm2 are merged to
     *  create ResizeInfo per commodity with CS1 as the target entity which will be resized,
     *  2 resize infos in total.
     *  The resizes for C4:Comm1, C4:Comm2, C5:Comm1, C5:Comm2, C6:Comm1, C6:Comm2 are merged
     *  to create another ResizeInfo per commodity with CS2 as the target entity
     *  which will be resized, 2 resize infos in total.
     *  These 4 resizes are merged to create one Atomic action with W1 as the execution target.
     *  In the example of
     *  WorkloadController W1 --> owns --> Containers C1, C2, C3
     *  The resizes for C1:Comm1, C1:Comm2, C2:Comm1, C2:Comm2, C3:Comm1, C3:Comm2 are each
     *  converted to its own ResizeInfo objects, 6 resizes in total.
     *  These resizes are then merged to create one atomic action with W1 as the execution target.
     *
     * @param actionsToMerge    actions that will be merged to create a new Atomic action
     *
     * @return map containing the new atomic Action and the list of actions that were merged
     */
    @Override
    public Map<Action.Builder, List<Action>> merge(@Nonnull final List<Action> actionsToMerge) {
        Map<Action.Builder, List<Action>> result = new HashMap<>();

        //Map containing entity that will aggregate and execute actions for the given list of entities
        Map<Long, AggregatedAction> aggregatedActionMap = new HashMap<>();

        // Organizing the actions first by de-duplication target and then by execution target
        // to create the AggregatedAction objects
        for (Action resizeAction : actionsToMerge) {
            // original resize info
            final ActionInfo actionInfo = resizeAction.getInfo();
            long entityOid = actionInfo.getResize().getTarget().getId();

            Resize resizeInfo = resizeAction.getInfo().getResize();
            ActionEntity originalEntity = resizeInfo.getTarget();
            CommodityDTO.CommodityType resizeCommType = CommodityDTO.CommodityType
                    .forNumber(resizeInfo.getCommodityType().getType());

            // atomic action spec associated with this entity
            AtomicActionSpec specsInfo = atomicActionSpecsCache.getAtomicResizeSpec(entityOid);
            ResizeMergeSpec resizeSpec = specsInfo.getResizeSpec();

            // Execution target for the entity resize
            AtomicActionEntity aggregateActionEntity = specsInfo.getAggregateEntity();
            ActionEntity executionEntity = aggregateActionEntity.getEntity();

            // De-duplication target, if available
            Optional<AtomicActionEntity> mergeActionEntity
                    = resizeSpec.hasDeDuplicationTarget()
                            ? Optional.of(resizeSpec.getDeDuplicationTarget()) : Optional.empty();

            // Entity that will be the target of the resize of behalf of this entity and other entities
            // in the scaling group. If not provided, the original action entity is the target
            ActionEntity deDupEntity;
            if (mergeActionEntity.isPresent()) {
                deDupEntity = mergeActionEntity.get().getEntity();
            } else {
                deDupEntity = originalEntity;
            }

            AggregatedAction aggregatedAction = null;
            // Create AggregatedAction if it does not exist
            if (!aggregatedActionMap.containsKey(executionEntity.getId())) {
                aggregatedAction = new AggregatedAction(executionEntity, aggregateActionEntity.getEntityName());
                aggregatedActionMap.put(executionEntity.getId(), aggregatedAction);
            }
            aggregatedAction = aggregatedActionMap.get(executionEntity.getId());

            DeDupedActions deDupedActions = aggregatedAction.getDeDupedActions(deDupEntity.getId());
            // Create DeDupedActions if it does not exist
            if (deDupedActions == null) {
                deDupedActions = new DeDupedActions(deDupEntity,
                                            mergeActionEntity.isPresent()
                                                    ? mergeActionEntity.get().getEntityName()
                                                    :  originalEntity.toString());
                aggregatedAction.setDeDupedActions(deDupedActions);
            }
            deDupedActions.addAction(resizeInfo.getCommodityType(), resizeAction);
        }

        // Create atomic action per AggregationAction
        for (Long aggregateTargetOid : aggregatedActionMap.keySet()) {
            AggregatedAction aggregatedAction = aggregatedActionMap.get(aggregateTargetOid);

            AtomicResizeBuilder atomicResizeBuilder = new AtomicResizeBuilder(aggregatedAction);

            Action.Builder atomicAction = atomicResizeBuilder.createMergedAction();

            List<Action> mergedActions = new ArrayList<>();
            for (Long deDupId : aggregatedAction.deDupedActionsMap.keySet()) {
                DeDupedActions deDupedActions = aggregatedAction.deDupedActionsMap.get(deDupId);
                mergedActions.addAll(deDupedActions.getAllActions());
            }

            result.put(atomicAction, mergedActions);
        }

        return result;
    }

    /**
     * Builder to create atomic resize action per aggregation target.
     */
    static class AtomicResizeBuilder {

        private AggregatedAction aggregatedAction;
        private final Logger logger = LogManager.getLogger();

        private Map<Long, List<ActionDTO.ResizeInfo>> resizeInfoMap = new HashMap<>();
        private Map<Long, List<AtomicResizeExplanation.ResizeExplanationPerEntity>> resizeInfoExplanationMap
                = new HashMap<>();

        /**
         * Constructor for new builder.
         *
         * @param aggregatedAction  AggregatedAction containing the relevant information
         *                              for creating the atomic action
         */
        AtomicResizeBuilder(AggregatedAction aggregatedAction) {
            this.aggregatedAction = aggregatedAction;
        }

        /**
         * Create the atomic action.
         *
         * @return {@link Action.Builder}
         */
        private Action.Builder createMergedAction() {
            Set<Long> deDupTargetIds = aggregatedAction.deDupedActionsMap.keySet();

            List<ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity>
                    explanationList = new ArrayList<>();

              // First create the resize info objects per de-dup target
            for (Long deDupTarget : deDupTargetIds) {
                DeDupedActions deDupedActions = aggregatedAction.deDupedActionsMap.get(deDupTarget);
                resizeInfoAndExplanation(deDupedActions);
            }

            // One atomic resize per aggregate target
            AtomicResize.Builder atomicResizeInfoBuilder = ActionDTO.AtomicResize.newBuilder();
            // Atomic Action Target is the Aggregation entity
            atomicResizeInfoBuilder.setExecutionTarget(aggregatedAction.aggregationTarget);

            // Collect all the resize and explanation info for the atomic action
            for (Long deDupTarget : deDupTargetIds) {
                DeDupedActions deDupedActions = aggregatedAction.deDupedActionsMap.get(deDupTarget);
                List<ActionDTO.ResizeInfo> resizeInfos = resizeInfoMap.get(deDupTarget);
                atomicResizeInfoBuilder.addAllResizes(resizeInfos);
                logger.trace("{}: merges {} resizes after de-dup to {}",
                        aggregatedAction.targetName,
                        resizeInfos.size(), deDupedActions.targetName);

                explanationList.addAll(resizeInfoExplanationMap.get(deDupTarget));
            }

            Explanation explanation = Explanation.newBuilder().setAtomicResize(
                    AtomicResizeExplanation.newBuilder().setMergeGroupId(
                            aggregatedAction.targetName)
                            .addAllPerEntityExplanation(explanationList)
                            .addAllEntityIds(deDupTargetIds).build()).build();

            Action.Builder mergedAction = Action.newBuilder()
                    .setId(IdentityGenerator.next())
                    .setExplanation(explanation)
                    .setDeprecatedImportance(1.0)
                    .setExecutable(true)
                    .setInfo(ActionInfo.newBuilder()
                            .setAtomicResize(atomicResizeInfoBuilder.build())
                            .build());

            logger.trace("Created Atomic ActionDTO {} with target {} containing total resizes {}",
                    mergedAction.getId(), aggregatedAction.targetName,
                    atomicResizeInfoBuilder.getResizesCount());

            return mergedAction;
        }

        /**
         * Create the {@link ActionDTO.ResizeInfo} for each de-duplication entity.
         *
         * @param deDupedActions DeDupedActions containing the data for creating the resize info
         */
        private void resizeInfoAndExplanation(DeDupedActions deDupedActions) {
            Map<CommodityType, List<Action>> actionsByCommMap = deDupedActions.actionsByComm;
            Long deDupEntityOid = deDupedActions.deDupTarget.getId();

            // Create one resize info all the de-duplicated actions per commodity
            for (CommodityType resizeCommType : actionsByCommMap.keySet()) {
                String targetName = deDupedActions.targetName;
                logger.trace("{} : creating one resizeInfo for commodity {} for {} resizes",
                        targetName, resizeCommType, actionsByCommMap.get(resizeCommType).size());

                // source entities
                List<ActionEntity> originalEntities = actionsByCommMap.get(resizeCommType).stream()
                        .map(action -> action.getInfo().getResize().getTarget())
                        .collect(Collectors.toList());
                Set<Long> entityIds = originalEntities.stream().map(a -> a.getId())
                        .collect(Collectors.toSet());

                // target entity for the resize is the de-duplication entity
                ActionEntity targetEntity = deDupedActions.deDupTarget;

                Action origAction = actionsByCommMap.get(resizeCommType).iterator().next();
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
                resizeInfoMap.computeIfAbsent(deDupEntityOid,
                                                v -> new ArrayList<>()).add(resizeInfo);

                // Explanation for this resize
                AtomicResizeExplanation.ResizeExplanationPerEntity explanationPerEntity
                        = AtomicResizeExplanation.ResizeExplanationPerEntity.newBuilder()
                        .setEntityId(targetName)
                        .addAllResizeEntityIds(entityIds)
                        .setPerCommodityExplanation(
                                ResizeExplanationPerEntity.ResizeExplanationPerCommodity.newBuilder()
                                        .setCommodityType(resizeCommType))
                        .build();
                resizeInfoExplanationMap.computeIfAbsent(deDupEntityOid, v -> new ArrayList<>())
                        .add(explanationPerEntity);
            }
        }
    }
}
