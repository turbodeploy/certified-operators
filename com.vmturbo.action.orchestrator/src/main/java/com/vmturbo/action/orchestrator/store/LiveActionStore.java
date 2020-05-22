package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.LiveActions.RecommendationTracker;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * {@inheritDoc}
 *
 * Stores actions mainly in memory except executed (SUCCEEDED and FAILED) actions
 * For use with actions from the live market (sometimes also called "real time").
 */
@ThreadSafe
public class LiveActionStore implements ActionStore {
    private static final Logger logger = LogManager.getLogger();

    private final LiveActions actions;

    private final ActionHistoryDao actionHistoryDao;

    /**
     * Map containing the id of the host that went in maintenance and the corresponding topology id.
     */
    private final Map<Long, Long> hostsInMaintenance;

    /**
     * Lock protecting population of the store. Within a single "population" of the store we may
     * want to hold and release the actions lock, but we don't want to allow other "population"
     * operations to take place in the meantime.
     */
    private static final Object storePopulationLock = new Object();

    private final IActionFactory actionFactory;

    private final long topologyContextId;

    private final EntitySeverityCache severityCache;

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    public static final String STORE_TYPE_NAME = "Live";

    private final ActionTargetSelector actionTargetSelector;

    private final ProbeCapabilityCache probeCapabilityCache;

    private final LiveActionsStatistician actionsStatistician;

    private final ActionTranslator actionTranslator;

    private final UserSessionContext userSessionContext;

    private final LicenseCheckClient licenseCheckClient;

    private final Clock clock;

    private static final int queryTimeWindowForLastExecutedActionsMins = 60;

    /**
     * A mutable (real-time) action is considered visible (from outside the Action Orchestrator's perspective)
     * if it's not disabled. We shouldn't consider the action state or executability as before.
     *
     * <p>Non-visible actions can still be valuable for debugging
     * purposes, but they shouldn't be exposed externally.
     *
     * @param actionView The {@link ActionView} to test for visibility.
     * @return True if the spec is visible to the UI, false otherwise.
     */
    public static final Predicate<ActionView> VISIBILITY_PREDICATE = actionView ->
        actionView.getVisibilityLevel().checkVisibility(true);

    /**
     * Create a new {@link ActionStore} for storing live actions (actions generated by the realtime market).
     */
    public LiveActionStore(@Nonnull final IActionFactory actionFactory,
                           final long topologyContextId,
                           @Nonnull final ActionTargetSelector actionTargetSelector,
                           @Nonnull final ProbeCapabilityCache probeCapabilityCache,
                           @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache,
                           @Nonnull final ActionHistoryDao actionHistoryDao,
                           @Nonnull final LiveActionsStatistician liveActionsStatistician,
                           @Nonnull final ActionTranslator actionTranslator,
                           @Nonnull final Clock clock,
                           @Nonnull final UserSessionContext userSessionContext,
                           @Nonnull final LicenseCheckClient licenseCheckClient) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.topologyContextId = topologyContextId;
        this.severityCache = new EntitySeverityCache();
        this.actionTargetSelector = actionTargetSelector;
        this.probeCapabilityCache = probeCapabilityCache;
        this.entitySettingsCache = entitySettingsCache;
        this.actionHistoryDao = actionHistoryDao;
        this.clock = clock;
        this.actions = new LiveActions(actionHistoryDao, clock, userSessionContext);
        this.actionsStatistician = Objects.requireNonNull(liveActionsStatistician);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.licenseCheckClient = Objects.requireNonNull(licenseCheckClient);
        this.hostsInMaintenance = new HashMap<>();
    }

    /**
     * {@inheritDoc}
     *
     * All recommendations in the {@link ActionPlan} that correspond to a READY action in the store
     * or are new to the store will be added in the same order they share in the {@link ActionPlan}.
     * The order of actions in the plan that correspond to an action in the store is not guaranteed.
     * <p/>
     * Any actions in the {@link ActionPlan} that match an action already in the store retain the ID
     * of the recommendation in the store, rather than the new recommendation ID.
     * <p/>
     * All recommendations in the {@link ActionPlan} that do not match an action already in the store
     * are added to the store as an {@link Action}.
     * <p/>
     * All QUEUED and IN_PROGRESS {@link Action}s are retained in the store.
     * <p/>
     * All CLEARED, SUCCEEDED, and FAILED {@link Action}s are removed from the store.
     * <p/>
     * All currently READY {@link Action}s are transitioned to CLEARED and removed from the store
     * unless they are re-recommended in the new {@link ActionPlan}.
     * <p/>
     * Consider the following scenarios where the store has one plan in it and the plan has one recommendation in it:
     * <ul>
     *     <li>PRE-STORE:  Move VM1: HostA -> HostB (IN_PROGRESS)</li>
     *     <li>PLAN:       Move VM1: HostA -> HostB</li>
     *     <li>POST-STORE: Move VM1: HostA -> HostB (IN_PROGRESS)</li>
     * </ul>
     * The market re-recommended an IN_PROGRESS action. In this case, there were 0 READY actions in the
     * store at the end of the populate call even though there was 1 ActionInfo in the plan.
     * <ul>
     *     <li>PRE-STORE:  Move VM1: HostA -> HostB (SUCCEEDED)</li>
     *     <li>PLAN:       Move VM1: HostA -> HostB</li>
     *     <li>POST-STORE: empty</li>
     * </ul>
     * The market re-recommended an action that already ran and succeeded.
     * <ul>
     *     <li>PRE-STORE:  Move VM1: HostA -> HostB (READY)</li>
     *     <li>PLAN:       Move VM1: HostA -> HostB</li>
     *     <li>POST-STORE: Move VM1: HostA -> HostB (READY)</li>
     * </ul>
     * The market re-recommended an action that was undecided.
     * <ul>
     *     <li>PRE-STORE:  Move VM1: HostA -> HostB (READY)</li>
     *     <li>PLAN:       empty</li>
     *     <li>POST-STORE: empty</li>
     * </ul>
     * The market did not re-recommend a READY action in the store.
     */
    @Override
    public boolean populateRecommendedActions(@Nonnull final ActionPlan actionPlan) {

        synchronized (storePopulationLock) {
            if (actionPlan.getInfo().hasBuyRi()) {
                return populateBuyRIActions(actionPlan);
            }

            final TopologyInfo sourceTopologyInfo =
                    actionPlan.getInfo().getMarket().getSourceTopologyInfo();

            final Long topologyContextId = sourceTopologyInfo.getTopologyContextId();

            final Long topologyId = sourceTopologyInfo.getTopologyId();

            final EntitiesAndSettingsSnapshot snapshot =
                    entitySettingsCache.newSnapshot(ActionDTOUtil.getInvolvedEntityIds(actionPlan.getActionList()),
                            Collections.emptySet(), topologyContextId, topologyId);

            // This call requires some computation and an RPC call, so do it outside of the
            // action lock.
            Collection<ActionDTO.Action> actionsWithAdditionalInfo =
                actionsWithAdditionalInfo(actionPlan, snapshot);

            // RecommendationTracker to accelerate lookups of recommendations.
            RecommendationTracker recommendations = new RecommendationTracker();

            // Apply addition and removal to the internal store atomically.
            final List<ActionView> completedSinceLastPopulate = new ArrayList<>();
            final List<Long> actionsToRemove = new ArrayList<>();

            actions.doForEachMarketAction(action -> {
                // Only retain IN-PROGRESS, QUEUED and READY actions which are re-recommended.
                switch (action.getState()) {
                    case IN_PROGRESS:
                    case QUEUED:
                        recommendations.add(action);
                        break;
                    case READY:
                        recommendations.add(action);
                        actionsToRemove.add(action.getId());
                        break;
                    case SUCCEEDED:
                    case FAILED:
                        completedSinceLastPopulate.add(action);
                        actionsToRemove.add(action.getId());
                        break;
                    default:
                        actionsToRemove.add(action.getId());
                }
            });


            // We are still holding the population lock, so
            // the actions map shouldn't get modified in the meantime.
            //
            // Actions may change state due to user behaviour, but that's fine.
            //
            // We now build up the list of actions to add, and apply translations.

            final long planId = actionPlan.getId();
            final MutableInt newActionCounts = new MutableInt(0);
            final Set<Long> newActionIds = Sets.newHashSet();
            // TODO (marco, July 16 2019): We can do the translation before we do the support
            // level resolution. In this way we wouldn't need to go to the repository for entities
            // that fail translation.
            final Stream<Action> translatedReadyActions = actionTranslator.translate(actionsWithAdditionalInfo.stream()
                .map(recommendedAction -> {
                    final Optional<Action> existingActionOpt = recommendations.take(recommendedAction.getInfo());
                    final Action action;
                    if (existingActionOpt.isPresent()) {
                        action = existingActionOpt.get();

                        // If we are re-using an existing action, we should update the recommendation
                        // so other properties that may have changed (e.g. importance, executability)
                        // reflect the most recent recommendation from the market. However, we only
                        // do this for "READY" actions. An IN_PROGRESS or QUEUED action is considered
                        // "fixed" until it either succeeds or fails.
                        // TODO (roman, Oct 31 2018): If a QUEUED action becomes non-executable, it
                        // may be worth clearing it.
                        if (action.getState() == ActionState.READY) {
                            action.updateRecommendation(recommendedAction);
                        }
                    } else {
                        newActionCounts.getAndIncrement();
                        action = actionFactory.newAction(recommendedAction, planId);
                        newActionIds.add(action.getId());
                    }

                    if (action.getState() == ActionState.READY) {
                        return action;
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull), snapshot);

            final MutableInt removedCount = new MutableInt(0);
            final List<Action> translatedActionsToAdd = new ArrayList<>();

            // This actually drains the stream defined above, updating the recommendation, creating
            // new actions, and so on.
            translatedReadyActions.forEach(action -> {
                if (action.getTranslationStatus() == TranslationStatus.TRANSLATION_FAILED) {
                    removedCount.increment();
                    // Make sure to remove the actions with failed translations.
                    // We don't send NotRecommendedEvent-s because the action is still recommended
                    // but it's useless.
                    actionsToRemove.add(action.getId());
                    logger.trace("Removed action {} with failed translation. Full action: {}",
                            action.getId(), action);
                } else {
                    translatedActionsToAdd.add(action);
                }
            });


            // We don't explicitly clear actions that were not successfully translated.
            if (removedCount.intValue() > 0) {
                logger.warn("Dropped {} actions due to failed translations.", removedCount);
            }


            // Some of these may be noops - if we're re-adding an action that was already in
            // the map from a previous action plan.
            // THis also updates the snapshot in the entity settings cache.
            actions.updateMarketActions(actionsToRemove, translatedActionsToAdd, snapshot);

            logger.info("Number of Re-Recommended actions={}, Newly created actions={}",
                (actionPlan.getActionCount() - newActionCounts.intValue()),
                newActionCounts);

            // Clear READY or QUEUED actions that were not re-recommended. If they were
            // re-recommended, they would have been removed from the RecommendationTracker
            // above.
            StreamSupport.stream(recommendations.spliterator(), false)
                .filter(action -> (action.getState() == ActionState.READY
                    || action.getState() == ActionState.QUEUED))
                .forEach(action -> action.receive(new NotRecommendedEvent(planId)));

            // Record the action stats.
            // TODO (roman, Nov 15 2018): For actions completed since the last snapshot, it may make
            // sense to use the last snapshot's time instead of the current snapshot's time.
            // Not doing it for now because of the extra complexity - and it's not clear if anyone
            // cares if the counts are off by ~10 minutes.
            actionsStatistician.recordActionStats(sourceTopologyInfo,
                // Only record user-visible actions.
                Stream.concat(completedSinceLastPopulate.stream(),
                        // Need to make a copy because it's not safe to iterate otherwise.
                        actions.copy().values().stream())
                    .filter(VISIBILITY_PREDICATE), newActionIds);
            hostsInMaintenance.entrySet().removeIf(entry -> (
                sourceTopologyInfo.getTopologyId() > entry.getValue()));
            clearActionsAffectingEntities(hostsInMaintenance.keySet());

        }

        return true;
    }

    /**
     * Add additional info to the actions, such as support level and pre-requisites of an action.
     *
     * @param actionPlan an action plan containing actions
     * @param snapshot the snapshot of entities
     * @return a collection of actions with additional information added to them
     */
    @Nonnull
    private Collection<ActionDTO.Action> actionsWithAdditionalInfo(
            @Nonnull final ActionPlan actionPlan,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        try (DataMetricTimer timer = Metrics.SUPPORT_LEVEL_CALCULATION.startTimer()) {
            // Attempt to fully refresh the cache - this gets the most up-to-date target and
            // probe information from the topology processor.
            //
            // Note - we don't REALLY need to do this, because the cache tries to stay up to date by
            // listening for probe registrations and target additions/removals. But fully refreshing
            // it is cheap, so we do it to be safe.
            probeCapabilityCache.fullRefresh();

            // Remove actions which have been already executed recently. Executed actions could be re-recommended by market,
            // if the discovery has not happened and TP broadcasts the old topology.
            // NOTE: A corner case which is not handled: If the discovery is delayed for a long time, then just looking at the last n minutes
            // in the action_history DB may not be enough. We need to know the "freshness" of the discovered results. But this facility is
            // not currently available. So we don't handle this case.
            final LocalDateTime startDate = LocalDateTime.now(clock).minusMinutes(queryTimeWindowForLastExecutedActionsMins);
            final LocalDateTime endDate = LocalDateTime.now(clock);
            List<ActionView> lastSuccessfullyExecutedActions = new ArrayList<>();
            try {
                lastSuccessfullyExecutedActions = actionHistoryDao.getActionHistoryByDate(startDate, endDate);
            } catch (DataAccessException dae) {
                // We continue on DB exception as we don't want to block actions.
                logger.warn("Error while fetching last executed actions from action history", dae);
            }

            RecommendationTracker lastExecutedRecommendationsTracker  = new RecommendationTracker();
            lastSuccessfullyExecutedActions.stream()
                    .filter(actionView -> actionView.getState().equals(ActionState.SUCCEEDED))
                    .forEach(actionView ->
                        lastExecutedRecommendationsTracker.add(actionFactory.newAction(
                            actionView.getRecommendation(), actionView.getActionPlanId())));
            List<ActionDTO.Action> newActions =
                    actionPlan.getActionList()
                            .stream()
                            .filter(action -> {
                                Optional<Action> filteredAction =
                                        lastExecutedRecommendationsTracker.take(action.getInfo());
                                if (filteredAction.isPresent()) {
                                    logger.debug("Skipping action: {} as it has already been executed", action);
                                    return false;
                                } else {
                                    return true;
                                }
                            })
                            .collect(Collectors.toList());

            final Map<Long, ActionTargetInfo> actionAndTargetInfo =
                actionTargetSelector.getTargetsForActions(newActions.stream(), snapshot);

            // Increment the relevant counters.
            final Map<SupportLevel, Long> actionsBySupportLevel =
                actionAndTargetInfo.values().stream()
                    .collect(Collectors.groupingBy(ActionTargetInfo::supportingLevel, Collectors.counting()));
            logger.info("Action support counts: {}", actionsBySupportLevel);

            // First, zero out all the values.
            Metrics.SUPPORT_LEVELS.getLabeledMetrics().values()
                .forEach(gaugeData -> gaugeData.setData(0.0));
            actionsBySupportLevel.forEach((supportLevel, numActions) -> {
                Metrics.SUPPORT_LEVELS.labels(supportLevel.name()).setData((double)numActions);
            });

            return Collections2.transform(newActions, action -> {
                final SupportLevel supportLevel = Optional.ofNullable(
                    actionAndTargetInfo.get(action.getId()))
                    .map(ActionTargetInfo::supportingLevel)
                    .orElse(SupportLevel.UNSUPPORTED);
                final Set<Prerequisite> prerequisites = Optional.ofNullable(
                    actionAndTargetInfo.get(action.getId()))
                    .map(ActionTargetInfo::prerequisites)
                    .orElse(Collections.emptySet());

                // If there are any updates to the action, update and rebuild it.
                if (action.getSupportingLevel() != supportLevel || !prerequisites.isEmpty()) {
                    return action.toBuilder()
                        .setSupportingLevel(supportLevel)
                        .addAllPrerequisite(prerequisites)
                        .build();
                } else {
                    return action;
                }
            });
        }
    }

    private boolean populateBuyRIActions(@Nonnull ActionPlan actionPlan) {
        final long planId = actionPlan.getId();
        // (Oct 24 2019): When processing BuyRI actions we always use the realtime snapshot.
        // This is necessary because in a pure BuyRI plan we don't have a plan-specific source
        // topology. This is safe because we only need the names of the related regions and tiers,
        // which don't change between realtime and plan.
        final EntitiesAndSettingsSnapshot snapshot = entitySettingsCache.newSnapshot(
                ActionDTOUtil.getInvolvedEntityIds(actionPlan.getActionList()),
                Collections.emptySet(), topologyContextId);

        // All RI translations should be passthrough, but we do it here anyway for consistency
        // with the "normal" action case.
        actions.replaceRiActions(actionTranslator.translate(
            actionPlan.getActionList().stream().map(
                recommendedAction -> actionFactory.newAction(recommendedAction, planId)), snapshot));
        actions.updateBuyRIActions(snapshot);
        logger.info("Number of buy RI actions={}", actionPlan.getActionCount());
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return actions.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean allowsExecution() {
        // if the license is invalid, then disallow execution.
        return licenseCheckClient.hasValidNonExpiredLicense();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<Action> getAction(long actionId) {
        return actions.getAction(actionId);
    }

    @Nonnull
    @Override
    public Map<Long, Action> getActions() {
        return actions.copy();
    }

    @Nonnull
    @Override
    public Map<ActionPlanType, Collection<Action>> getActionsByActionPlanType() {
        return actions.getActionsByPlanType();
    }

    @Nonnull
    @Override
    public Optional<ActionView> getActionView(long actionId) {
        // The map operation is necessary because of how Java handles generics via type erasure.
        // An Optional<Action> is not directly assignable to an Optional<ActionView> even though an
        // Action is an ActionView.
        return getAction(actionId)
            .map(Function.identity());
    }


    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public QueryableActionViews getActionViews() {
        return actions;
    }

    @Override
    public boolean overwriteActions(@Nonnull final Map<ActionPlanType, List<Action>> newActions) {
        // To be 100% correct we should probably add a method to overwrite both action types
        // atomically, but we only use this method when loading diags so it's ok to do this.
        newActions.forEach((actionType, actionsOfType) -> {
            switch (actionType) {
                case MARKET:
                    actions.replaceMarketActions(actionsOfType.stream());
                    break;
                case BUY_RI:
                    actions.replaceRiActions(actionsOfType.stream());
                    break;
            }
        });

        logger.info("Successfully overwrote actions in the store with {} new actions.", actions.size());
        return true;
    }

    /**
     * {@inheritDoc}
     * The {@link LiveActionStore} does not permit this operation.
     */
    @Override
    public boolean clear() {
        throw new IllegalStateException("Actions for the live market context " +
            topologyContextId + " may not be deleted.");
    }

    @Override
    public long getTopologyContextId() {
        return topologyContextId;
    }

    @Override
    @Nonnull
    public EntitySeverityCache getEntitySeverityCache() {
        return severityCache;
    }

    @Override
    @Nonnull
    public Predicate<ActionView> getVisibilityPredicate() {
        return VISIBILITY_PREDICATE;
    }

    @Override
    @Nonnull
    public String getStoreTypeName() {
        return STORE_TYPE_NAME;
    }

    /**
     * Set a host that went into maintenance and the timestamp of when this event occurred.
     * Then clear the actions targeting those hosts.
     *
     * @param hostIds ids of hosts that went into maintenance.
     * @param topologyId corresponding topology id.
     */
    public void clearActionsOnHosts(Set<Long> hostIds,
                                      long topologyId) {

        for (Long hostId: hostIds) {
            this.hostsInMaintenance.put(hostId, topologyId);
        }
        clearActionsAffectingEntities(hostIds);
    }

    /**
     * Collects actions that are targeting an entity and then deletes them. After removing the
     * actions from the live action store it refreshes the severity cache. This needs to happen
     * in two steps to ensure thread safety.
     *
     * @param entityIds the affected entities.
     */
    private void clearActionsAffectingEntities(@Nonnull final Set<Long> entityIds) {
        if (entityIds.isEmpty()) {
            return;
        }
        Set<Long> affectedActions = new HashSet<>();
        actions.doForEachMarketAction((action) -> {
            if (actionAffectsEntity(entityIds, action)) {
                affectedActions.add(action.getId());
            }
        });
        if (affectedActions.isEmpty()) {
            return;
        }
        actions.deleteActionsById(affectedActions);
        severityCache.refresh(this);
    }

    /**
     * Find actions that are targeting and entity and move actions that have that entity as a
     * destination.
     * @param entityIds the affected entities.
     * @param action the action to check.
     * @return true if the action affects the entity
     */
    private boolean actionAffectsEntity(@Nonnull final Set<Long> entityIds,
                                                 @Nonnull Action action) {
        ActionInfo actionInfo = action.getRecommendation().getInfo();
        if (actionInfo.hasMove()) {
            for (ChangeProvider changeProvider : actionInfo.getMove().getChangesList()) {
                if (entityIds.contains(changeProvider.getDestination().getId())) {
                    return true;
                }
            }
        }
        try {
            if (entityIds.contains(ActionDTOUtil.getPrimaryEntity(action.getTranslationResultOrOriginal()).getId())) {
              return true;
            }
        } catch (UnsupportedActionException e) {
            logger.warn("Unable to get primary entity for action {}", action);
        }
        return false;
    }

    private static class Metrics {
        private static final DataMetricGauge SUPPORT_LEVELS = DataMetricGauge.builder()
            .withName("ao_live_action_support_level_gauge")
            .withHelp("Current number of actions of various support levels in the live action store.")
            .withLabelNames("support_level")
            .build();

        private static final DataMetricSummary SUPPORT_LEVEL_CALCULATION = DataMetricSummary.builder()
            .withName("ao_live_action_support_level_calculation_seconds")
            .withHelp("Time taken to calculate support levels in the live action store.")
            .build()
            .register();
    }

}
