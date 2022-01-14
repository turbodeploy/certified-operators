package com.vmturbo.action.orchestrator.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockingRelation;
import com.vmturbo.common.protobuf.action.ActionDTO.CausingRelation;
import com.vmturbo.common.protobuf.action.ActionDTO.MarketRelatedAction;
import com.vmturbo.common.protobuf.action.ActionDTO.RelatedAction;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;

/**
 * Builds and injects new RelatedAction DTOs for market and atomic actions.
 * This based on the MarketRelatedActions received in the ActionPlan.
 *
 * <p>An action can be blocked or caused by one or more action. We call this the 'Impacted action'.
 * An impacted action is associated with {@link MarketRelatedAction} which contains the action id of
 * the action 'impacting' it. This could be a list if there is more than one action impacting an action and
 * is created when the action is created by the market or the atomic action factory.
 * RelatedActionsInjector will convert each of the MarketRelatedAction data to {@link RelatedAction}
 * containing the durable Recommendation OID of the impacting action. These are then set in the
 * {@link Action} object so the related actions associated with an action are available outside the action
 * orchestrator component via the API calls.
 *
 * <p>RelatedActionsInjector will also convert each of the MarketRelatedAction data associated with the impacted action to
 * {@link RelatedAction} for the impacting actions. The RelatedAction for impacting actions will point to the
 * durable Recommendation ids of the impacted actions.
 * This way the two-way relationship between the impacted and impacting actions are both available for users.
 */
public class RelatedActionsInjector {
    private static final Logger logger = LogManager.getLogger();

    // Map of action ID and its list of related actions for market actions
    // Provided by the market ActionPlan
    private final Map<Long, List<MarketRelatedAction>> marketActionsRelationsMap;

    // Map of action ID and its list of related actions for atomic actions
    // Provided by the atomic action factory
    private final Map<Long, List<MarketRelatedAction>> atomicActionsRelationsMap;

    private final Map<Long, Map<Long, ActionDTO.RelatedAction>> atomicActionsReverseRelations;

    private final Map<Long, Action> actions;

    private final EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot;

    /**
     * Constructor for RelatedActionsInjector.
     *
     * @param marketActionsRelationsMap Map of action ID and its list of related actions for market actions,
     *                                  provided by the market ActionPlan
     * @param atomicActionsRelationsMap  Map of action ID and its list of related actions for atomic actions,
     *                                   provided by the atomic action factory
     * @param atomicActionsReverseRelations Map of blocking Action ID and its corresponding reverse blocking
     *                                      related action
     * @param actionStore               LiveActionStore containing the Action objects for all actions
     * @param entitiesAndSettingsSnapshot Given {@link EntitiesAndSettingsSnapshot}
     */
    public RelatedActionsInjector(@Nonnull final Map<Long, List<MarketRelatedAction>> marketActionsRelationsMap,
                                  @Nonnull final Map<Long, List<MarketRelatedAction>> atomicActionsRelationsMap,
                                  @Nonnull final Map<Long, Map<Long, ActionDTO.RelatedAction>> atomicActionsReverseRelations,
                                  @Nonnull final LiveActionStore actionStore,
                                  @Nonnull final EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot) {
        this.marketActionsRelationsMap  = marketActionsRelationsMap;
        this.atomicActionsRelationsMap = atomicActionsRelationsMap;
        this.atomicActionsReverseRelations = atomicActionsReverseRelations;
        // a copy of action from the action store
        actions = actionStore.getActions();
        // Cache of entities.
        this.entitiesAndSettingsSnapshot = entitiesAndSettingsSnapshot;
    }

    /**
     * Get the action object associated with the given market action.
     *
     * @param actionId  market action id
     *
     * @return  Action
     */
    private Optional<Action> getAction(long actionId) {
        if (actions.containsKey(actionId)) {
            return Optional.of(actions.get(actionId));
        }
        return Optional.empty();
    }

    /**
     * Result of conversion of {@link MarketRelatedAction}'s associated with a specific Action to {@link RelatedAction}s.
     *
     * <p>It contains the list of RelatedAction for the actions impacting (blocking or causing) this action and is directly
     * converted from the given list of MarketRelatedActions.
     * It also consists of the RelatedActions for each of the impacting actions. This is presented as a map of the
     * impacting action OID and the RelatedAction pointing to the OID of the impacted action.
     * Consider this  MarketRelatedAction for one atomic resize(1L) - it refers to another resize action(2L)
     * that is blocking it.
     *
     * <p>Impacted Action RelatedAction:<br>
     * related_actions {
     *   recommendation_id: [Durable OID for 2L]
     *   action_entity {
     *     id: 707538826076166
     *     type: 40
     *     environment_type: HYBRID
     *   }
     *   blocking_relation {
     *     resize {
     *       commodity_type {
     *         type: 106
     *       }
     *     }
     *   }
     * }
     *
     * <p>This is converted to RelatedAction where the instance id of the related action is replaced by the durable
     * Recommendation OID.
     *
     * <p>In addition, RelatedAction for the impacting action(2L) refers to this atomic resize action will also be created.
     *
     * <p>Impacting Action RelatedAction:
     * <br>related_actions {
     *   recommendation_id: [Durable OID for 1L]
     *   action_entity {
     *     id: 707538826076533
     *     type: 66
     *     environment_type: HYBRID
     *   }
     *   blocking_relation {
     *     resize {
     *       commodity_type {
     *         type: 43
     *       }
     *     }
     *   }
     * }
     *
     */
    @Value.Immutable
    public interface SymmetricRelatedActionsResult {
        /**
         * Action object for the action associated with one of more MarketRelatedActions that impact this action.
         *
         * @return Action
         */
        Action actionWithRelatedAction();

        /**
         * List of RelatedActions for the action with the MarketRelatedActions.
         *
         * @return list of RelatedActions directly converted from the list of MarketRelatedActions.
         */
        List<RelatedAction> impactingRelatedActions();

        /**
         * Action object for the actions impacting the action with MarketRelatedActions
         * and the RelatedAction pointing to this action being impacted.
         *
         * @return  Map of actions impacting the action with MarketRelatedActions and
         *          the counterpart RelatedAction pointing to the action they are impacting.
         */
        Map<Action, RelatedAction> impactingActionToImpactedRelatedActions();
    }

    /**
     * Create and inject RelatedActions data associated with actions.
     *
     * <p>First, create RelatedActions using the list of MarketRelatedActions impacting the actions.
     * Also creates the RelatedActions for the impacting actions pointing to the impacted action.
     *
     * <p>Controller1 and Controller2 resizes are blocked by 2 namespace actions NS1, NS2.
     * There are 2 MarketRelatedActions for both  Controller1 and Controller2 -
     * Controller1::related_actions: {
     *     instance_id: NS1
     * }
     * Controller1:: related_actions: {
     *      instance_id: NS2
     * }
     * Controller2::related_actions: {
     *     instance_id: NS1
     * }
     * Controller2:: related_actions: {
     *      instance_id: NS2
     * }
     *
     * <p>Each of the MarketRelatedAction will be converted to RelatedActions for Controller1 and Controller2.
     * Also 2 RelatedActions will be created for both NS1 and NS2, pointing to Controller1 and Controller2.
     * NS1::related_actions: {
     *     instance_id: Controller1
     * }
     * NS1:: related_actions: {
     *      instance_id: Controller2
     * }
     * NS2::related_actions: {
     *     instance_id: Controller1
     * }
     * NS2:: related_actions: {
     *      instance_id: Controller2
     * }
     */
    public void injectSymmetricRelatedActions() {
        Map<Action, List<RelatedAction>> relatedActionsByImpactingAction = new HashMap<>();

        // MarketRelatedActions impacting all market and atomic actions.
        Map<Long, List<MarketRelatedAction>> marketRelatedActionsMap = new HashMap<>(marketActionsRelationsMap);
        marketRelatedActionsMap.putAll(atomicActionsRelationsMap);

        List<SymmetricRelatedActionsResult> relatedActionsResultList
                = toSymmetricRelatedActions(marketRelatedActionsMap);
        relatedActionsResultList.stream()
                .forEach(symmRelatedActionsResult -> {
                    // Set the RelatedActions for the impacted action that was originally associated
                    // with list of the MarketRelatedActions it is being impacted by
                    Action action = symmRelatedActionsResult.actionWithRelatedAction();
                    action.setRelatedActions(symmRelatedActionsResult.impactingRelatedActions());

                    // Set RelatedAction for the impacting actions
                    Map<Action, RelatedAction> impactingActionRelatedActionsMap
                            = symmRelatedActionsResult.impactingActionToImpactedRelatedActions();
                    for (Action impactingAction : impactingActionRelatedActionsMap.keySet()) {
                        RelatedAction ra = impactingActionRelatedActionsMap.get(impactingAction);
                        // Impacting action can impact many other actions, so need to collect RelatedAction from
                        // each of those actions
                        relatedActionsByImpactingAction.computeIfAbsent(impactingAction, v -> new ArrayList<>()).add(ra);
                    }
                });

        // Set the RelatedActions for the impacting actions
        relatedActionsByImpactingAction.entrySet().stream().forEach(entry -> {
            Action action = entry.getKey();
            action.setRelatedActions(entry.getValue());
        });
    }

    private List<SymmetricRelatedActionsResult> toSymmetricRelatedActions(@Nonnull Map<Long, List<MarketRelatedAction>> marketRelatedActionsMap) {
        List<SymmetricRelatedActionsResult> allResult
                = marketRelatedActionsMap.keySet().stream()
                    .filter(impactedActionId -> getAction(impactedActionId).isPresent()) // Merged container actions will be deleted from the action store, so this is ok
                    .map(impactedActionId -> symmetricRelatedActions(getAction(impactedActionId).get(),
                                                                marketRelatedActionsMap.get(impactedActionId)))
                    .collect(Collectors.toList());

        return allResult;
    }

    /**
     * Create RelatedActions using the associated market related actions for a given action.
     *
     * @param impactedAction Action object for the impacted action
     * @param impactingRelatedActions   list of MarketRelatedAction impacting this action
     *
     * @return  {@link SymmetricRelatedActionsResult} containing the RelatedActions
     *          for the impacted and the impacting actions.
     */
    @Nonnull
    private SymmetricRelatedActionsResult symmetricRelatedActions(Action impactedAction,
                                                                  List<MarketRelatedAction> impactingRelatedActions) {
        // One or more action impacting (blocking or causing) this action
        // - convert each MarketRelatedAction to corresponding RelatedAction
        // One BlockingRelation/CausingRelation is created for each of the namespace resizes
        // blocking a controller resize action or each of the container pod suspension
        // causing a vm suspension action
        List<RelatedAction> updatedImpactingRelatedActions = impactingRelatedActions.stream()
                .map(ra -> toImpactingRelatedAction(ra))
                .collect(Collectors.toList());

        // Create the related action for each of the actions impacting the action above
        // One BlockedRelation/CausedRelation pointing to this action is created
        // for each of the Namespace action blocking this WC action
        // or for each of the Pod action caused by the VM suspension
        Map<Action, RelatedAction> impactedActionToRelatedActionMap = new HashMap<>();
        impactingRelatedActions.stream().forEach(ra -> {
            // The action that is impacting  - we want to create the reverse BlockedRelation/CausedRelation for this
            long impactingActionId = ra.getActionId();   // namespace/pod action
            Optional<Action> impactingAction = getAction(impactingActionId);
            if (impactingAction.isPresent()) {
                try {
                    Action action = impactingAction.get();   // namespace/pod action
                    RelatedAction impactedRelatedAction = toImpactedRelatedAction(ra, impactedAction);
                    impactedActionToRelatedActionMap.put(action, impactedRelatedAction);
                } catch (UnsupportedActionException e) {
                    e.printStackTrace();
                }
            }
        });

        ImmutableSymmetricRelatedActionsResult.Builder symmetricRelatedActionResult
                = ImmutableSymmetricRelatedActionsResult.builder()
                    .actionWithRelatedAction(impactedAction)       // action for which is being impacted
                    .impactingRelatedActions(updatedImpactingRelatedActions)   // all its impacting actions
                    .impactingActionToImpactedRelatedActions(impactedActionToRelatedActionMap);     // reverse impacted relation for the actions impacting this action

        return symmetricRelatedActionResult.build();
    }

    /**
     * Convert from an Impacting MarketRelatedAction to a RelatedAction.
     *
     * @param marketRelatedAction  MarketRelatedAction pointing to an action impacting another action
     *
     * @return RelatedAction for the Impacting action containing the durable recommendation OID for that action
     */
    @Nonnull
    private RelatedAction toImpactingRelatedAction(MarketRelatedAction marketRelatedAction) {
        Long actionId = marketRelatedAction.getActionId();
        // Impacting action using the action id from ra to get the recommendation OID
        // Example Action object of the Namespace action
        Optional<Action> impactingAction = getAction(actionId);  //get and save
        if (!impactingAction.isPresent()) {
            throw new IllegalArgumentException("Need durable recommendation OID for action " + actionId);
        }

        RelatedAction.Builder relatedAction = RelatedAction.newBuilder()
                .setActionEntity(marketRelatedAction.getActionEntity())    // Namespace entity
                .setRecommendationId(impactingAction.get().getRecommendationOid()) // Namespace action OID
                .setDescription(impactingAction.get().getDescription());
        entitiesAndSettingsSnapshot.getEntityFromOid(marketRelatedAction.getActionEntity().getId())
                .map(ActionPartialEntity::getDisplayName).ifPresent(relatedAction::setActionEntityDisplayName);

        MarketRelatedAction.ActionRelationTypeCase relationType = marketRelatedAction.getActionRelationTypeCase();
        switch (relationType) {
            case BLOCKED_BY_RELATION:
                relatedAction.setBlockedByRelation(ActionDTO.BlockedByRelation.newBuilder()
                                                .mergeFrom(marketRelatedAction.getBlockedByRelation()));
                break;
            case CAUSED_BY_RELATION:
                relatedAction.setCausedByRelation(ActionDTO.CausedByRelation.newBuilder()
                        .mergeFrom(marketRelatedAction.getCausedByRelation()));
                break;
        }

        return relatedAction.build();
    }

    /**
     * Create an impacted MarketRelatedAction for a given RelatedAction.
     * Given a BlockedBy MarketRelatedAction create a Blocking RelatedAction.
     * Given a CausedBy MarketRelatedAction create a Causing RelatedAction.
     *
     * @param marketImpactingAction   MarketRelatedAction pointing to an action impacting another action
     * @param impactedAction Action object for the impacted action
     *
     * @return counterpart RelatedAction
     * @throws UnsupportedActionException if the action entity for the counterpart related action is not found
     */
    @Nonnull
    private RelatedAction toImpactedRelatedAction(MarketRelatedAction marketImpactingAction, Action impactedAction)
            throws UnsupportedActionException {
        ActionDTO.ActionEntity impactedEntity
                    = ActionDTOUtil.getPrimaryEntity(impactedAction.getRecommendation());
        RelatedAction.Builder relatedAction = RelatedAction.newBuilder()
                    .setActionEntity(impactedEntity)  // this is the blocked action entity - WC entity
                    .setRecommendationId(impactedAction.getRecommendationOid()) // WC action being blocked
                    .setDescription(impactedAction.getDescription());
        entitiesAndSettingsSnapshot.getEntityFromOid(impactedEntity.getId()).map(ActionPartialEntity::getDisplayName)
                .ifPresent(relatedAction::setActionEntityDisplayName);

        MarketRelatedAction.ActionRelationTypeCase relationType = marketImpactingAction.getActionRelationTypeCase();
        switch (relationType) {
            case BLOCKED_BY_RELATION:
                if (atomicActionsReverseRelations.containsKey(marketImpactingAction.getActionId())) {
                    Map<Long, RelatedAction> raMap
                            = atomicActionsReverseRelations.get(marketImpactingAction.getActionId());
                    long blockedActionId = impactedAction.getId();
                    if (raMap.containsKey(blockedActionId)) {
                        RelatedAction ra = raMap.get(blockedActionId);
                        relatedAction.setBlockingRelation(BlockingRelation.newBuilder()
                                .setResize(BlockingRelation.BlockingResize.newBuilder()
                                        .setCommodityType(ra.getBlockingRelation().getResize().getCommodityType())));
                    }
                } else if (marketImpactingAction.getBlockedByRelation().hasResize()) {
                    relatedAction.setBlockingRelation(BlockingRelation.newBuilder()
                            .setResize(BlockingRelation.BlockingResize.newBuilder()
                                    .setCommodityType(marketImpactingAction.getBlockedByRelation().getResize().getCommodityType())));
                }

                break;
            case CAUSED_BY_RELATION:
                if (marketImpactingAction.getCausedByRelation().hasProvision()) {
                        relatedAction.setCausingRelation(CausingRelation.newBuilder()
                                .setProvision(CausingRelation.CausingProvision.newBuilder()));
                } else if (marketImpactingAction.getCausedByRelation().hasSuspension()) {
                        relatedAction.setCausingRelation(CausingRelation.newBuilder()
                                .setSuspension(CausingRelation.CausingSuspension.newBuilder()));
                }
                break;
        }
        return relatedAction.build();
    }
}
