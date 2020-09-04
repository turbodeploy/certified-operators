package com.vmturbo.action.orchestrator.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionEntity;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.ResizeMergeSpec;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Examines a group of resize actions on different entities using the atomic actions specs and
 * produces {@link AggregatedAction} to create merged resize action that will be executed atomically
 * by a target entity.
 */
public class AtomicResizeMerger implements AtomicActionMerger {

    private static final Logger logger = LogManager.getLogger();
    private final AtomicActionSpecsCache atomicActionSpecsCache;

    /**
     * Constructor for AtomicResizeMerger.
     *
     * @param atomicActionSpecsCache cache with the action merge specs
     */
    public AtomicResizeMerger(AtomicActionSpecsCache atomicActionSpecsCache) {
        this.atomicActionSpecsCache = atomicActionSpecsCache;
    }

    /**
     * Handles resize actions for entity which has an action merge spec from its probe.
     *
     * @param action Action to check.
     * @return boolean indicating if the action will merged by this atomic action merger
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
     * Create a map of {@link AggregatedAction} each containing a group of market actions that will
     * be de-duplicated and merged to be executed by the single entity.
     * Actions are merged based on the {@link AtomicActionSpec} for the entities.
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
     * @param actionsToMerge  actions that will be merged to create new atomic actions
     * @return Map of OID of the aggregation target and the AggregatedAction.
     *          AggregatedAction  will be used to create the atomic action that
     *          will be executed by the corresponding aggregation target.
     */
    @Override
    public Map<Long, AggregatedAction> mergeActions(@Nonnull final List<Action> actionsToMerge) {
        //Map containing entity that will aggregate and execute actions for the given list of entities
        Map<Long, AggregatedAction> aggregatedActionMap = new HashMap<>();

        // Organizing the actions first by de-duplication target and then by execution target
        // to create the AggregatedAction objects
        for (Action resizeAction : actionsToMerge) {
            // original resize info
            final ActionInfo actionInfo = resizeAction.getInfo();
            long entityOid = actionInfo.getResize().getTarget().getId();

            // atomic action spec associated with this entity
            AtomicActionSpec specsInfo = atomicActionSpecsCache.getAtomicResizeSpec(entityOid);
            ResizeMergeSpec resizeSpec = specsInfo.getResizeSpec();

            // Execution target for the entity resize
            AtomicActionEntity aggregateActionEntity = specsInfo.getAggregateEntity();
            ActionEntity executionEntity = aggregateActionEntity.getEntity();

            // De-duplication target, if available
            Optional<AtomicActionEntity> deDuplicationActionEntity
                    = resizeSpec.hasDeDuplicationTarget()
                    ? Optional.of(resizeSpec.getDeDuplicationTarget()) : Optional.empty();

            AggregatedAction aggregatedAction;
            // Create AggregatedAction if it does not exist
            if (!aggregatedActionMap.containsKey(executionEntity.getId())) {
                aggregatedAction = new AggregatedAction(ActionTypeCase.ATOMICRESIZE, executionEntity,
                                                            aggregateActionEntity.getEntityName());
                aggregatedActionMap.put(executionEntity.getId(), aggregatedAction);
            }
            aggregatedAction = aggregatedActionMap.get(executionEntity.getId());

            aggregatedAction.addAction(resizeAction, deDuplicationActionEntity);
        }

        return aggregatedActionMap;
    }
}
