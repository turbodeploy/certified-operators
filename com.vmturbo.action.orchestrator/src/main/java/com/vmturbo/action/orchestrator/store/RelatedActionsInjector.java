package com.vmturbo.action.orchestrator.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
 *
 * <p>RelatedActionsInjector will convert each of the MarketRelatedAction data to {@link RelatedAction}
 * containing the durable Recommendation OID of the impacting action. These are then set in the
 * {@link Action} object so the related actions associated with an action are available outside the action
 * orchestrator component via the API calls.
 *
 * <p>RelatedActionsInjector will also convert each of the MarketRelatedAction data associated with the impacted action
 * to {@link RelatedAction} for the impacting actions. The RelatedAction for impacting actions will point to the
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

    // Map of pre-created RelatedAction for the actions impacting the atomic actions
    // Provided by the atomic action factory
    private final Map<Long, Map<Long, ActionDTO.RelatedAction>> atomicActionsReverseRelations;

    // a copy of action from the action store
    private final Map<Long, Action> actions;

    /**
     * Map of action re-recommended ID to initial ID.
     */
    private final Map<Long, Long> reRecommendedActionIdMap;

    private final EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot;

    // Fetch the ActionView objects from the action store corresponding to current action plan IDs
    // from the marketActionsRelationsMap and the atomicActionsRelationsMap.
    // We have to look up first using the current action plan id or the initial action id from the reRecommendedActionIdMap.
    // The same action ids are looked up several times during the RelatedActions conversion process.
    // For example, a workload controller resize (1L) is blocked by 4 namespaces resizes (3L, 4L, 5L, 6L).
    // and another workload controller resize (2L) is blocked by 4 namespaces resizes (3L, 4L, 5L, 6L).
    // While creating RelatedActions for both WC::1L and WC::2L, we have to look up Action Views for actions 3L, 4L, 5L, 6L
    // Similarly, for creating RelatedAction for namespace::3L, we have to look up Action Views for WC resizes 1L, 2L.
    // These maps are created to cache the Action View for the action as they are being looked up
    // to avoid multiple action store lookups.
    private final Map<Long, Action> impactedActionsCache;
    private final Map<Long, Action> impactingActionsCache;

    // List of action ids whose action view cannot be retrieved from the action store using the current action plan id
    private final List<Long> missingImpactedActions = new ArrayList<>();
    private final List<Long> missingImpactingActions = new ArrayList<>();

    private final InjectorSummary injectorSummary;

    /**
     * Constructor for RelatedActionsInjector.
     *
     * @param actionPlanId  Action Plan Id for which the RelatedActionsInjector is invoked
     * @param marketActionsRelationsMap Map of action ID and its list of related actions for market actions,
     *                                  provided by the market ActionPlan
     * @param atomicActionsRelationsMap  Map of action ID and its list of related actions for atomic actions,
     *                                   provided by the atomic action factory
     * @param atomicActionsReverseRelations Map of impacting Action IDs to the pre-created RelatedActions containing
     *                                      the relevant data of the impacted atomic actions
     * @param actionStore               LiveActionStore containing the Action objects for all actions
     * @param entitiesAndSettingsSnapshot Given {@link EntitiesAndSettingsSnapshot}
     */
    public RelatedActionsInjector(long actionPlanId,
                                  @Nonnull final Map<Long, List<MarketRelatedAction>> marketActionsRelationsMap,
                                  @Nonnull final Map<Long, List<MarketRelatedAction>> atomicActionsRelationsMap,
                                  @Nonnull final Map<Long, Map<Long, ActionDTO.RelatedAction>> atomicActionsReverseRelations,
                                  @Nonnull final LiveActionStore actionStore,
                                  @Nonnull final EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot) {
        this.marketActionsRelationsMap  = marketActionsRelationsMap;
        this.atomicActionsRelationsMap = atomicActionsRelationsMap;
        this.atomicActionsReverseRelations = atomicActionsReverseRelations;
        // a copy of action from the action store
        actions = actionStore.getActions();
        reRecommendedActionIdMap = actionStore.getReRecommendedIdMap();
        // Cache of entities.
        this.entitiesAndSettingsSnapshot = entitiesAndSettingsSnapshot;
        this.injectorSummary = new InjectorSummary(actionPlanId);
        // cache of impacted and impacting action views keyed by the current action plan ids
        this.impactedActionsCache = new HashMap<>();
        this.impactingActionsCache = new HashMap<>();
    }

    /**
     * Get the action object associated with the current action id.
     *
     * <p>If action is not found based on given action ID, check if this is a re-recommended
     * action and get the stored action from initial action ID based on reRecommendedActionIdMap.
     *
     * @param actionId  market action id
     *
     * @return  Optional containing the Action if found or null if not
     */
    private Optional<Action> getAction(long actionId) {
        Action action = actions.get(actionId);
        if (action == null) {
            // If action is not found based on given action ID, check if this is a re-recommended
            // action and get the stored action from initial action ID based on reRecommendedActionIdMap.
            Long origActionId = reRecommendedActionIdMap.get(actionId);
            return Optional.ofNullable(actions.get(origActionId));
        }
        return Optional.of(action);
    }

    /**
     * Check if the Action view object for an impacted action exists given its action id in the current ActionPlan.
     * When the {@link Action} is found for the first time, it is saved in the #impactedActionsCache.
     *
     * <p>The same action ids are looked up several times during the RelatedActions conversion process.
     * <br>For example, consider a workload controller resize (1L) being blocked by 4 namespaces resizes (3L, 4L, 5L, 6L).
     * and another workload controller resize (2L) blocked by the same 4 namespaces resizes (3L, 4L, 5L, 6L).
     * While creating RelatedActions for both WC::1L and WC::2L, we have to look up Action Views for actions 3L, 4L, 5L, 6L
     * Similarly, for creating RelatedAction for namespace::3L, we have to look up Action Views for WC resizes 1L, 2L.
     * Therefore, the Action View for the action as they are being looked up is cached to avoid multiple action store lookups.
     *
     * <p>The action fetching process is two steps at least because the action view will be fetched either using the
     * current action id or its original action id depending on if the action is re-recommended or not.
     *
     * <p>LiveActionStore saves action using the action id of the action when it is first recommended by the market.
     * Subsequent re-recommendations are also re-saved using the original action id during the UpdateRecommendation
     * action pipeline stage. The durable RecommendationId is used to match the action from the action store
     * and the re-recommended action in the new action plan.
     *
     * <p>MarketRelatedActions contain action ids from the action plan that is being currently processed by the
     * ActionPipeline. If the action is re-recommended, this action id is different from when it was first recommended.
     * Hence, we may not be able to locate the Action object from the action store using the current action ids.
     * And will need to use the {@link #reRecommendedActionIdMap} to obtain the original action ids in order to locate
     * the action view.
     *
     * <p>This method first tries to locate the action view using the current action id first by looking
     * in the {@link #impactedActionsCache}. Otherwise, it uses the {@link #getAction(long)} method to fetch the action
     * using the current action id or using the original action id as obtained from the {@link #reRecommendedActionIdMap}.
     *
     * @param impactedActionCurrId  Id of the impacted action in the current action plan
     *
     * @returns True of the {@link Action} is found and saved, false if not.
     */
    private boolean findAndCacheImpactedAction(long impactedActionCurrId) {
        if (impactedActionsCache.containsKey(impactedActionCurrId)) {   // pre-fetched action
            return true;
        } else {
            Optional<Action> impactedAction = getAction(impactedActionCurrId);
            if (!impactedAction.isPresent()) {
                // Merged container actions will be deleted from the action store,
                // so this is expected when processing actions from the market
                missingImpactedActions.add(impactedActionCurrId);
                return false;
            } else {
                impactedActionsCache.put(impactedActionCurrId, impactedAction.get());
                return true;
            }
        }
    }

    /**
     * Check if the Action view object for an impacting action exists given its action id in the current ActionPlan.
     * See javadoc for {@link #findAndCacheImpactedAction(long)} for more details.
     *
     * @param impactingActionCurrId  Id of the impacting action in the current action plan
     *
     * @returns True of the {@link Action} is found and saved, false if not.
     */
    private boolean findAndCacheImpactingAction(long impactingActionCurrId) {
        if (impactingActionsCache.containsKey(impactingActionCurrId)) {   // pre-fetched action
            return true;
        } else {
            Optional<Action> impactingAction = getAction(impactingActionCurrId);
            if (!impactingAction.isPresent()) {
                logger.info("Action is not found for impacting action with current id {}",
                        impactingActionCurrId);
                missingImpactingActions.add(impactingActionCurrId);
                return false;
            } else {
                impactingActionsCache.put(impactingActionCurrId, impactingAction.get());
                return true;
            }
        }
    }

    /**
     * Get the {@link Action} from the impacted actions cache.
     * <b>It is necessary to use the {@link #findAndCacheImpactedAction(long)} to determine,
     * if the action view is found before calling this method</b>
     *
     * @param impactedActionCurrId   Id of the impacted action in the current action plan
     *
     * @return {@link Action} view for the impacted action.
     */
    private Action getCachedImpactedAction(long impactedActionCurrId) {
        return impactedActionsCache.get(impactedActionCurrId);
    }

    /**
     * Get the {@link Action} from the impacting actions cache.
     * <b>It is necessary to use the {@link #findAndCacheImpactingAction(long)} to determine,
     * if the action view is found before calling this method</b>
     *
     * @param impactingActionCurrId   Id of the impacting action in the current action plan
     *
     * @return {@link Action} view for the impacting action.
     */
    private Action getCachedImpactingAction(long impactingActionCurrId) {
        return impactingActionsCache.get(impactingActionCurrId);
    }

    /**
     * Result of conversion of {@link MarketRelatedAction}'s associated with a specific Action to {@link RelatedAction}s.
     *
     * <p>It contains the list of RelatedAction for the actions impacting (blocking or causing) this action and
     * is direct one-to-one conversion from the given list of the BLOCKED_BY/CAUSED_BY MarketRelatedActions,
     * albeit with durable OIDs instead of the action plan ids.
     * In addition, the result also consists of the BLOCKING/CAUSING RelatedActions for each of the impacting actions.
     * The action ids for impacting actions are obtained from the BLOCKED_BY/CAUSED_BY section of the MarketRelatedActions.
     * The RelatedActions for all the impacting actions are presented as a map of the impacting action OID and
     * the BLOCKED/CAUSED RelatedAction pointing to the OID of the impacted action.
     *
     * <p>Consider a MarketRelatedAction for one atomic resize(1L) - being blocked by another resize action(2L)
     * This is first converted to RelatedAction where the instance id of the blocked action is replaced by the durable
     * Recommendation OID.
     *
     * <p>Impacted Action RelatedAction:<br>
     * related_actions {
     *   recommendation_id: [Durable OID for 2L]    // id of the impacting action
     *   action_entity {
     *     id: 707538826076166
     *     type: 40
     *     environment_type: HYBRID
     *   }
     *   blocked_by_relation|caused_by_relation {
     *     ...
     *   }
     * }
     *
     * <p>In addition, RelatedAction for the impacting action(2L) refering to the atomic resize actionit blocks
     * will also be created.
     *
     * <p>Impacting Action RelatedAction:<br>
     * related_actions {
     *   recommendation_id: [Durable OID for 1L]    //id of the impacted action
     *   action_entity {
     *     id: 707538826076533
     *     type: 66
     *     environment_type: HYBRID
     *   }
     *   blocking_relation|causing_relation {
     *     ...
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
     * <p>First, create RelatedActions from the list of MarketRelatedActions impacting the actions. This is one-to-one
     * conversion of CausedByRelation|BlockedByRelation MarketRelatedActions
     * to CausedByRelation|BlockedByRelation RelatedActions and contains the OID of the impacting action.
     * Also create the CausingRelation|BlockingRelation RelatedActions for the impacting actions pointing
     * to the impacted action OID.
     *
     * <p>Controller1 and Controller2 resizes are blocked by 2 namespace actions NS1, NS2.
     * There are 2 MarketRelatedActions for both  Controller1 and Controller2 -
     * Controller1::MarketRelatedAction: {
     *     action_id: NS1
     * }
     * Controller1::MarketRelatedAction: {
     *      action_id: NS2
     * }
     * Controller2::MarketRelatedAction: {
     *     action_id: NS1
     * }
     * Controller2::MarketRelatedAction: {
     *      action_id: NS2
     * }
     *
     * <p>These MarketRelatedActions are converted as follows to RelatedActions for the controller as well as the
     * namespace actions.
     * <br>For the Controller resizes:<br>
     * Controller1::RelatedAction : {
     *     recommendation_id: durable OID of NS1
     * }
     * Controller1::RelatedAction : {
     *      recommendation_id: durable OID of NS2
     * }
     * Controller2::RelatedAction : {
     *     recommendation_id: durable OID of NS1
     * }
     * Controller2::RelatedAction: {
     *      recommendation_id: durable OID of NS2
     * }
     *
     * <br>For the Namespace resizes:<br>
     * NS1::RelatedAction: {
     *     recommendation_id: Controller1
     * }
     * NS1::RelatedAction: {
     *      recommendation_id: Controller2
     * }
     * NS2::RelatedAction: {
     *     recommendation_id: Controller1
     * }
     * NS2::RelatedAction: {
     *      recommendation_id: Controller2
     * }
     */
    public InjectorSummary injectSymmetricRelatedActions() {
        // MarketRelatedActions impacting all market and atomic actions.
        injectorSummary.setInitialImpactedMarketActions(marketActionsRelationsMap.size());
        injectorSummary.setInitialImpactedAtomicActions(atomicActionsRelationsMap.size());

        // Step1: Conversion of the MarketRelatedActions to RelatedActions for both the market and atomic actions
        List<SymmetricRelatedActionsResult> relatedActionsResultListForMarketActions
                = toSymmetricRelatedActions(marketActionsRelationsMap);
        injectorSummary.setMarketImpactedActionsWithRelatedActions(relatedActionsResultListForMarketActions.size());
        logger.info("Created {} Related actions for impacted {} market actions",
                relatedActionsResultListForMarketActions.size(), marketActionsRelationsMap.size());

        List<SymmetricRelatedActionsResult> relatedActionsResultListForAtomicActions
                = toSymmetricRelatedActions(atomicActionsRelationsMap);
        injectorSummary.setAtomicImpactedActionsWithRelatedActions(relatedActionsResultListForAtomicActions.size());
        logger.info("Created {} Related actions for impacted {} atomic actions",
                relatedActionsResultListForAtomicActions.size(), atomicActionsRelationsMap.size());

        injectorSummary.setMissingImpactedActionIds(missingImpactedActions);
        injectorSummary.setMissingImpactingActionIds(missingImpactingActions);

        // Step 2: Associating ActionView objects with RelatedActions data
        List<SymmetricRelatedActionsResult> relatedActionsResultList
                = new ArrayList<>(relatedActionsResultListForMarketActions);
        relatedActionsResultList.addAll(relatedActionsResultListForAtomicActions);

        Map<Action, List<RelatedAction>> relatedActionsByImpactingAction = new HashMap<>();
        relatedActionsResultList.stream()
                .forEach(symRelatedActionsResult -> {   // result is per impacted action
                    // 1. Set the RelatedActions for the impacted action
                    // that was originally associated with the list of the MarketRelatedActions
                    Action action = symRelatedActionsResult.actionWithRelatedAction();
                    action.setRelatedActions(symRelatedActionsResult.impactingRelatedActions());

                    // 2. Set RelatedAction for the impacting actions
                    // Before setting RelatedActions for the impacting actions, first we have to collect the RelatedAction
                    // from each of the impacted action
                    Map<Action, RelatedAction> impactingActionRelatedActionsMap
                            = symRelatedActionsResult.impactingActionToImpactedRelatedActions();
                    for (Action impactingAction : impactingActionRelatedActionsMap.keySet()) {
                        RelatedAction ra = impactingActionRelatedActionsMap.get(impactingAction);
                        relatedActionsByImpactingAction.computeIfAbsent(impactingAction, v -> new ArrayList<>()).add(ra);
                    }
                });

        // Set the RelatedActions for the impacting actions
        relatedActionsByImpactingAction.entrySet().stream().forEach(entry -> {
            Action action = entry.getKey();
            action.setRelatedActions(entry.getValue());
        });

        injectorSummary.setTotalImpactingActionsWithRelatedActions(relatedActionsByImpactingAction.size());

        if (logger.isTraceEnabled()) {
            StringBuilder statusSummary = new StringBuilder();
            for (Long impactedActionId : injectorSummary.getImpactedActionIdToMissingImpactingActionIds().keySet()) {
                List<Long> missingIds = injectorSummary.getImpactedActionIdToMissingImpactingActionIds().get(impactedActionId);
                if (!missingIds.isEmpty()) {
                    statusSummary.append("Missing Impacting action ids by Impacted action id: " + missingIds)
                            .append("\nActual Impacting action ids by Impacted action id: "
                                    + injectorSummary.getImpactedActionIdToAllImpactingActionIds().get(impactedActionId));

                }
            }
            logger.trace(statusSummary.toString());
        }

        return injectorSummary;
    }

    /**
     *  Conversion of the MarketRelatedActions to RelatedActions.
     * @param marketRelatedActionsMap Map of impacted action id and its MarketRelatedActions containing the action id
     *                                of the impacting action
     *
     * @return SymmetricRelatedActionsResult containing the RelatedActions for impacted and impacting actions.
     */
    private List<SymmetricRelatedActionsResult> toSymmetricRelatedActions(@Nonnull Map<Long, List<MarketRelatedAction>> marketRelatedActionsMap) {
        List<SymmetricRelatedActionsResult> allResult = marketRelatedActionsMap.keySet().stream()
                .filter(impactedActionId -> findAndCacheImpactedAction(impactedActionId)) // find the impacted action view first
                .map(impactedActionId -> {
                    try {
                        return  symmetricRelatedActions(impactedActionId, getCachedImpactedAction(impactedActionId),
                                                        marketRelatedActionsMap.get(impactedActionId));
                    } catch (Exception ex) {
                        logger.error("Exception while creating related actions for action with id {} : {}",
                                impactedActionId, ex);
                        return null;
                    }
                })
                .filter(Objects::nonNull)   // filter null result
                .collect(Collectors.toList());

        return allResult;
    }

    /**
     * Create RelatedActions using the associated MarketRelatedActions for a given impacted action.
     *
     * @param currentImpactedActionId  ID of the impacted action in the current action plan
     * @param impactedAction {@link Action} object for the impacted action containing the durable recommendation ID
     * @param impactingRelatedActions   list of {@link MarketRelatedAction} impacting this action
     *
     * @return  {@link SymmetricRelatedActionsResult} containing the RelatedActions
     *          for the impacted and the impacting actions.
     * @exception   IllegalArgumentException if the action view for impacted action is null
     */
    @Nonnull
    private SymmetricRelatedActionsResult symmetricRelatedActions(long currentImpactedActionId,
                                                                  @Nonnull Action impactedAction,
                                                                  List<MarketRelatedAction> impactingRelatedActions) {
        // This should not happen - since the action view was pre-fetched before invoking the get
        if (impactedAction == null) {
            throw new IllegalArgumentException("Null action view for impacted action with id"
                    + currentImpactedActionId + " in action plan " + injectorSummary.actionPlanId);
        }

        // Fetch and validate the action views of the impacting actions using their current ids
        // from the list of the given MarketRelatedActions
        List<Long> missingImpactingActions = new ArrayList<>();
        impactingRelatedActions.stream().forEach(ra -> {
            if (!findAndCacheImpactingAction(ra.getActionId())) {
                missingImpactingActions.add(ra.getActionId());
            }
        });
        if (!missingImpactingActions.isEmpty()) {
            injectorSummary.setImpactedActionIdToMissingImpactingActionIds(currentImpactedActionId, missingImpactingActions);
            List<Long> actionIdsForImpactingRelatedActions = impactingRelatedActions.stream()
                        .map(ra -> ra.getActionId()).collect(Collectors.toList());
            injectorSummary.setImpactedActionIdToAllImpactingActionIds(currentImpactedActionId, actionIdsForImpactingRelatedActions);
        }

        // From MarketRelatedAction -> RelatedAction which contain the data of the impacting action
        // This RelatedAction will be associated with the 'impacted' action
        // One BlockingRelation/CausingRelation is created for each of the namespace resizes
        // blocking a controller resize action or each of the container pod suspension
        // causing a vm suspension action
        List<RelatedAction> updatedImpactingRelatedActions = impactingRelatedActions.stream()
                .filter(marketRa -> findAndCacheImpactingAction(marketRa.getActionId()))
                .map(marketRa -> {
                    try {
                        return toImpactingRelatedAction(marketRa, getCachedImpactingAction(marketRa.getActionId()));
                    } catch (Exception ex) {
                        logger.error("Exception while creating impacting related actions for {} : {}",
                                currentImpactedActionId, ex);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // From MarketRelatedAction -> RelatedAction which contain the data of the impacted action
        // This RelatedAction will be associated with the 'impacting' action
        // One BlockedRelation/CausedRelation pointing to this action is created
        // for each of the Namespace action blocking this WC action
        // or for each of the Pod action caused by the VM suspension
        Map<Action, RelatedAction> impactedActionToRelatedActionMap = new HashMap<>();
        impactingRelatedActions.stream()
                .filter(marketImpactingRelatedAction -> findAndCacheImpactingAction(marketImpactingRelatedAction.getActionId()))
                .forEach(marketImpactingRelatedAction -> {
                    // The action that is impacting (impactingAction=namespace/pod action)
                    long impactingActionId = marketImpactingRelatedAction.getActionId();    //current namespace/pod action id
                    Action impactingAction = getCachedImpactingAction(impactingActionId);

                try {
                    // Check if there is a pre-created RelatedAction for this impacting action - true for atomic actions
                    // RelatedAction for namespace actions is partially created when the workload controller atomic actions are created
                    //atomicActionsReverseRelations ==> {namespace action id -> {map of wc action id, wc related action containing wc action id}}
                    Optional<RelatedAction> preCreatedRelatedAction = Optional.empty();
                    if (atomicActionsReverseRelations.containsKey(impactingActionId)) {
                        Map<Long, RelatedAction> preCreatedRAMap //map of current wc action id (impacted action), wc related action containing wc action id
                                = atomicActionsReverseRelations.get(impactingActionId);
                        if (preCreatedRAMap.containsKey(currentImpactedActionId)) {
                            preCreatedRelatedAction = Optional.of(preCreatedRAMap.get(currentImpactedActionId));
                        } else {
                            logger.warn("cannot find initial action id {} for impacted action, "
                                            + "while creating related action for impacting action {}",
                                    currentImpactedActionId, impactingActionId);
                        }
                    }

                    RelatedAction impactedRelatedAction = toImpactedRelatedAction(marketImpactingRelatedAction,
                                                                            preCreatedRelatedAction, impactedAction);
                    impactedActionToRelatedActionMap.put(impactingAction, impactedRelatedAction);    // namespace action, and its wc ra
                } catch (UnsupportedActionException e) {
                    e.printStackTrace();
                } catch (IllegalArgumentException ex) {
                    logger.error("Exception while creating impacted related actions for {}: "
                                    + "Null action view for impacting action with id {} in action plan {}",
                            impactingActionId, currentImpactedActionId, injectorSummary.actionPlanId);
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
     * This is a direct conversion of the MarketRelatedAction protobuf message for the impacted action
     * to RelatedAction protobuf message but the market action ids are replaced by the durable recommendation OIDs
     * from the action store.
     *
     * @param marketRelatedAction  MarketRelatedAction pointing to an action impacting another action
     * @param impactingAction Non-null Impacting {@link Action} pre-fetched from the action store
     *                        using the action id from MarketRelatedAction
     *
     * @return RelatedAction for the Impacting action containing the durable recommendation OID for that action
     */
    @Nullable
    private RelatedAction toImpactingRelatedAction(MarketRelatedAction marketRelatedAction, // namespace ra, namespace action
                                                   @Nonnull Action impactingAction) {
        // This should not happen - since the action view was pre-fetched before invoking the get
        if (impactingAction == null) {
            throw new IllegalArgumentException("Null action view for impacting action with id"
                    + marketRelatedAction.getActionId() + " in action plan " + injectorSummary.actionPlanId);
        }

        // impactingAction=namespace/pod action, impactedAction = workload controller action
        RelatedAction.Builder relatedAction = RelatedAction.newBuilder()
                .setActionEntity(marketRelatedAction.getActionEntity())    // Namespace entity
                .setRecommendationId(impactingAction.getRecommendationOid()) // Namespace action OID
                .setDescription(impactingAction.getDescription());
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
     * <p>Example:
     * ImpactedAction: workload controller resize.
     * Impacting MarketRelatedAction: MarketRelatedAction pointing to namespace action id as the action
     * impacting the workload controller resize above.
     * Returns: RelatedAction pointing to the workload controller as the impacted action by the namespace action
     * from the MarketRelatedAction above.
     * </p>
     *
     * @param marketImpactingAction   MarketRelatedAction pointing to an action impacting another action
     * @param impactedAction Action object for the impacted action
     *
     * @return counterpart RelatedAction
     * @throws UnsupportedActionException if the action entity for the counterpart related action is not found
     */
    @Nonnull
    private RelatedAction toImpactedRelatedAction(MarketRelatedAction marketImpactingAction,            //namespace ra
                                                  Optional<RelatedAction> preCreatedRelatedAction,      //pre-created  wc ra
                                                  @Nonnull Action impactedAction)                              //namespace ra, wc action
            throws UnsupportedActionException {
        // This should not happen - since the action view was pre-fetched before invoking the get
        if (impactedAction == null) {
            throw new IllegalArgumentException("Null action view for impacted action");
        }

        ActionDTO.ActionEntity impactedEntity   // WC or container
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
                if (preCreatedRelatedAction.isPresent()) {
                    RelatedAction ra = preCreatedRelatedAction.get();
                    relatedAction.setBlockingRelation(BlockingRelation.newBuilder()
                            .setResize(BlockingRelation.BlockingResize.newBuilder()
                                    .setCommodityType(ra.getBlockingRelation().getResize().getCommodityType())));
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

    /**
     * Summary of the numbers of actions handled.
     */
    public static class InjectorSummary {

        private final long actionPlanId;
        private int initialImpactedMarketActions = 0;
        private int initialImpactedAtomicActions = 0;
        private int marketImpactedActionsWithRelatedActions = 0;
        private int atomicImpactedActionsWithRelatedActions = 0;
        private int totalImpactingActionsWithRelatedActions = 0;
        private Map<Long, List<Long>> impactedActionIdToMissingImpactingActionIds = new HashMap<>();
        private Map<Long, List<Long>> impactedActionIdToAllImpactingActionIds = new HashMap<>();
        private List<Long> missingImpactedActionIds = new ArrayList<>();
        private List<Long> missingImpactingActionIds = new ArrayList<>();

        private InjectorSummary(long actionPlanId) {
            this.actionPlanId = actionPlanId;
        }

        public long getActionPlanId() {
            return this.actionPlanId;
        }

        public int getInitialImpactedMarketActions() {
            return initialImpactedMarketActions;
        }

        public int getInitialImpactedAtomicActions() {
            return initialImpactedAtomicActions;
        }

        public int getMarketImpactedActionsWithRelatedActions() {
            return marketImpactedActionsWithRelatedActions;
        }

        public int getAtomicImpactedActionsWithRelatedActions() {
            return atomicImpactedActionsWithRelatedActions;
        }

        public int getTotalImpactingActionsWithRelatedActions() {
            return totalImpactingActionsWithRelatedActions;
        }

        private void setInitialImpactedMarketActions(int initialImpactedMarketActions) {
            this.initialImpactedMarketActions = initialImpactedMarketActions;
        }

        private void setInitialImpactedAtomicActions(int initialImpactedAtomicActions) {
            this.initialImpactedAtomicActions = initialImpactedAtomicActions;
        }

        public void setTotalImpactingActionsWithRelatedActions(int totalImpactingActionsWithRelatedActions) {
            this.totalImpactingActionsWithRelatedActions = totalImpactingActionsWithRelatedActions;
        }

        public void setMarketImpactedActionsWithRelatedActions(int marketImpactedActionsWithRelatedActions) {
            this.marketImpactedActionsWithRelatedActions = marketImpactedActionsWithRelatedActions;
        }

        public void setAtomicImpactedActionsWithRelatedActions(int atomicImpactedActionsWithRelatedActions) {
            this.atomicImpactedActionsWithRelatedActions = atomicImpactedActionsWithRelatedActions;
        }

        private void setImpactedActionIdToMissingImpactingActionIds(long impactingActionId,
                                                                    List<Long> missingImpactingActionIds) {
            impactedActionIdToMissingImpactingActionIds.put(impactingActionId, missingImpactingActionIds);
        }

        private void setImpactedActionIdToAllImpactingActionIds(long impactingActionId,
                                                                    List<Long> allImpactingActionIds) {
            impactedActionIdToAllImpactingActionIds.put(impactingActionId, allImpactingActionIds);
        }

        public Map<Long, List<Long>> getImpactedActionIdToMissingImpactingActionIds() {
            return impactedActionIdToMissingImpactingActionIds;
        }

        public Map<Long, List<Long>> getImpactedActionIdToAllImpactingActionIds() {
            return impactedActionIdToAllImpactingActionIds;
        }

        private void setMissingImpactedActionIds(List<Long> missingImpactedActionIds) {
            this.missingImpactedActionIds = missingImpactedActionIds;
        }

        public List<Long> getMissingImpactedActionIds() {
            return missingImpactedActionIds;
        }

        public void setMissingImpactingActionIds(List<Long> missingImpactingActionIds) {
            this.missingImpactingActionIds = missingImpactingActionIds;
        }

        public List<Long> getMissingImpactingActionIds() {
            return this.missingImpactingActionIds;
        }
    }
}
