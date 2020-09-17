package com.vmturbo.action.orchestrator.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionEntity;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;

/**
 * Convenience class to hold market recommended actions that should be merged
 * by an aggregation entity.
 * Aggregation entity is the single target for executing the grouped actions atomically
 * for all the entities that are controlled by the aggregation entity.
 * AggregatedAction is created by {@link AtomicActionMerger} when a new action plan is received
 * by the Action Orchestrator.
 *
 * <p>Example for WorkloadController W1 that
 * --> owns -> ContainerSpec CS1 --> aggregates --> Containers C1, C2, C3
 * and also
 * --> owns -> ContainerSpec CS2 --> aggregates --> Containers C4, C5, C6
 * and also
 * ---> aggregates ---> Containers C10, C11 (without container spec)
 *
 * Note: In container model, you can't have a WorkloadController that directly aggregates
 * containers with no intermediate ContainerSpec.
 * This example is simply to demonstrate that an Aggregated action can merge actions for entities
 * associated with de-duplication targets or without.
 *
 * If there are duplicated resize actions {A1::VCPU, A2::VMEM} on C1, {A3::VCPU,A4::VMEM} on C2
 * and duplicated A7::VCPU, A8::VCPU on C4 and C5 respectively
 * and also resize action A10::VMEM, A11::VCPU, A20::VMEM, A21::VCPU on C10, C20,
 * then the AggregatedAction will be as follows -
 *
 *               \---------     AggregatedAction - WorkloadController W1    -----------|
 *              /                      /                          \                     \
 *             /                      /                           \                     \
 *            /                      /                            \                     \
 *  DeDuplication - CS1          DeDuplication - CS2        Merge - C10            Merge - C11
 *           |                            |                       |                     |
 *           |                            |                       |                     |
 *           |                            |                       |                     |
 *   Actions - {A1,A2,A3, A4}     Actions - {A7, A8}      Actions - {A10, A11}  Actions - {A20, A21}
 *
 * The resulting Action DTO is depicted in {@link AtomicActionBuilder}.
 * </p>
 *
 */
public class AggregatedAction {

    private final Logger logger = LogManager.getLogger();

    private final ActionTypeCase actionType;
    private final ActionEntity aggregationTarget;
    private final String targetName;

    // Map containing all the actions for entities belonging to the same scaling group
    // that be should be first de-duplicated to a single resize
    private final Map<Long, DeDupedActions> deDupedActionsMap;

    // List of actions without de-duplication target but should be executed
    // atomically by the aggregation target
    private final List<Action> aggregateOnlyActions;

    final Map<Long, ActionView> actionViews;

    /**
     * Add the {@link ActionView} for the given market action.
     *
     * @param actionId action id
     * @param actionView action view
     */
    public void updateActionView(long actionId, ActionView actionView) {
        actionViews.put(actionId, actionView);
    }

    /**
     * Constructor to create an object representing all the actions that will be grouped
     * together for atomic execution by a single target entity that controls the target entities
     * of the individual market actions.
     *
     * @param actionType {@link ActionTypeCase} representing the action type for the atomic action DTO
     *                                     that will be created from this aggregation action object
     * @param aggregationTarget {@link ActionEntity} that will execute the aggregated actions
     * @param targetName display name for the aggregation action target entity
     */
    public AggregatedAction(ActionTypeCase actionType, ActionEntity aggregationTarget, String targetName) {
        this.actionType = actionType;
        this.aggregationTarget = aggregationTarget;
        this.targetName = targetName;
        deDupedActionsMap = new HashMap<>();
        aggregateOnlyActions = new ArrayList<>();
        actionViews = new HashMap<>();
    }

    /**
     * The action type of the merged action DTO that is created from this AggregateAction.
     *
     * @return {@link ActionTypeCase}
     */
    public ActionTypeCase getActionTypeCase() {
        return actionType;
    }

    /**
     * The entity that will execute the resulting merged action DTO that is created
     * from this AggregateAction.
     *
     * @return {@link ActionEntity} that will execute the atomic action
     */
    public ActionEntity targetEntity() {
        return aggregationTarget;
    }

    /**
     * Display name for entity that will execute the resulting merged action DTO that is created
     * from this AggregateAction.
     *
     * @return display name for the {@link ActionEntity} that will execute the atomic action
     */
    public String targetName() {
        return targetName;
    }

    /**
     * OIDs of the entities that are involved in the resulting merged action DTO
     * that is created from this AggregateAction.
     * This includes the OIDs for the aggregation target, the de-duplicated action target
     * and the target entities of the actions that are not de-duplicated but only merged.
     *
     * @return list of OIDs of the involved action entities.
     */
    public List<Long> getActionEntities() {
        List<Long> atomicActionEntities = new ArrayList<>();
        atomicActionEntities.add(aggregationTarget.getId());
        atomicActionEntities.addAll(deDupedActionsMap.keySet());
        aggregateOnlyActions.stream().forEach( action -> {
                try {
                    atomicActionEntities.add(ActionDTOUtil.getPrimaryEntityId(action));
                } catch (UnsupportedActionException uae) {

                }
            }
        );

       return atomicActionEntities;
    }

    /**
     * Returns the list of market recommended actions that were grouped together
     * for executing atomically by the action DTO that is created from this AggregateAction.
     * This includes the actions that were de-duplicated and also the actions that were
     * not de-duplicated.
     *
     * @return list of original market {@link Action}s that are merged for execution
     */
    public List<Action> getAllActions() {
        List<Action> allActions = deDupedActionsMap.entrySet().stream()
                .flatMap(entry -> entry.getValue().actions.stream())
                .collect(Collectors.toList());
        allActions.addAll(aggregateOnlyActions);

        return allActions;
    }

    /**
     * Returns the list of market recommended actions merged together but <b>not de-duplicated</b>
     * for executing atomically by the action DTO that is created
     * from this AggregateAction.
     *
     * @return the list of market recommended actions merged together
     */
    public  List<Action> aggregateOnlyActions() {
        return aggregateOnlyActions;
    }

    /**
     * Returns the map containing the OID of the de-duplication target entity to the
     * DeDupedActions object containing the list of market recommended actions that will be
     * <b>de-duplicated</b> for executing atomically by the action DTO that is created
     * from this AggregateAction.
     *
     * @return map containing the OIDs of the de-duplication target entities and the associated
     *
     */
    public Map<Long, DeDupedActions> deDupedActionsMap() {
        return deDupedActionsMap;
    }

    /**
     * Add a market action to this aggregated action object.
     *
     * @param action market recommended {@link Action}
     * @param deDeuplicationTarget Optional containing the {@AtomicActionEntity} representing
     *                             the de-duplication entity if associated with the market action entity
     */
    public void addAction(Action action,  Optional<AtomicActionEntity> deDeuplicationTarget) {
        if (deDeuplicationTarget.isPresent()) {
            ActionEntity deDupEntity = deDeuplicationTarget.get().getEntity();

            DeDupedActions deDupedActions = deDupedActionsMap.get(deDupEntity.getId());
            if (deDupedActions == null) {
                deDupedActions = new DeDupedActions(deDupEntity, deDeuplicationTarget.get().getEntityName());
                deDupedActionsMap.put(deDupEntity.getId(), deDupedActions);
            }

            deDupedActions.addAction(action);
        } else {
                aggregateOnlyActions.add(action);
        }
    }

    /**
     * Convenience class to hold the actions for a de-duplication target.
     * De-duplication entity is the single target for executing the duplicate actions
     * for all of the entities in a scaling group.
     */
    public static class DeDupedActions {
        private static final Logger logger = LogManager.getLogger();

        private final ActionEntity deDupTarget;
        private final String targetName;

        // Set of entity actions for the entities in the scaling/deployment group
        private final List<Action> actions;

        /**
         * Constructor to create an object representing the group of duplicate actions
         * for the entities in a scaling/deployment group to be converted to a single execution unit.
         *
         * @param deDupTarget  {@link ActionEntity} that will execute the duplicated actions
         * @param targetName display name for the de-duplication action target entity
         */
        public DeDupedActions(ActionEntity deDupTarget, String targetName) {
            this.deDupTarget = deDupTarget;
            this.targetName = targetName;
            actions = new ArrayList<>();
        }

        /**
         * The entity on which the de-duplicated action will be executed.
         *
         * @return {@link ActionEntity} that will execute the de-duplicated action
         */
        public ActionEntity targetEntity() {
            return deDupTarget;
        }

        /**
         * Display name for the entity on which the de-duplicated action will be executed.
         *
         * @return display name for the {@link ActionEntity} that will execute the de-duplicated action
         */
        public String targetName() {
            return targetName;
        }

        /**
         * Add a duplicate market action to this de-duplicated action object.
         *
         * @param action market recommended {@link Action}
         */
        public void addAction(Action action) {
            actions.add(action);
        }

        /**
         * Return the list of market actions associated with this de-duplication action object.
         *
         * @return list of original market recommended {@link Action}s that are de-duplicated
         */
        public List<Action> actions() {
            return actions;
        }
    }
}
