package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.store.AtomicActionFactory.AtomicActionResult;
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
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.identity.IdentityService;
import com.vmturbo.identity.exceptions.IdentityServiceException;
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

    private final AtomicActionFactory atomicActionFactory;

    private final LiveActionsStatistician actionsStatistician;

    private final ActionTranslator actionTranslator;

    private final UserSessionContext userSessionContext;

    private final LicenseCheckClient licenseCheckClient;

    private final IdentityService<ActionInfo> actionIdentityService;

    private final Clock clock;

    private static final int queryTimeWindowForLastExecutedActionsMins = 60;

    private final EntitiesWithNewStateCache entitiesWithNewStateCache;
    private final ActionAuditSender actionAuditSender;

    private final AcceptedActionsDAO acceptedActionStore;

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
     * Create a new {@link ActionStore} for storing live actions (actions generated by the realtime
     * market).
     *
     * @param actionFactory the action factory
     * @param actionIdentityService identity service to fetch OIDs for actions
     * @param topologyContextId the topology context id
     * @param actionTargetSelector selects which target/probe to execute each action against
     * @param probeCapabilityCache gets the target-specific action capabilities
     * @param entitySettingsCache an entity snapshot factory used for creating entity snapshot
     * @param actionHistoryDao dao layer working with executed actions
     * @param liveActionsStatistician works with action stats
     * @param actionTranslator the action translator class
     * @param atomicActionFactory  the atomic action factory class
     * @param clock the {@link Clock}
     * @param userSessionContext the user session context
     * @param involvedEntitiesExpander used for expanding entities and determining how involved
     *                                 entities should be filtered.
     * @param acceptedActionsStore dao layer working with accepted actions
     * @param actionAuditSender action audit sender to receive new generated actions
     */
    public LiveActionStore(@Nonnull final IActionFactory actionFactory,
                           final long topologyContextId,
                           @Nonnull final SupplyChainServiceBlockingStub supplyChainService,
                           @Nonnull final RepositoryServiceBlockingStub repositoryService,
                           @Nonnull final ActionTargetSelector actionTargetSelector,
                           @Nonnull final ProbeCapabilityCache probeCapabilityCache,
                           @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache,
                           @Nonnull final ActionHistoryDao actionHistoryDao,
                           @Nonnull final LiveActionsStatistician liveActionsStatistician,
                           @Nonnull final ActionTranslator actionTranslator,
                           @Nonnull final AtomicActionFactory atomicActionFactory,
                           @Nonnull final Clock clock,
                           @Nonnull final UserSessionContext userSessionContext,
                           @Nonnull final LicenseCheckClient licenseCheckClient,
                           @Nonnull final AcceptedActionsDAO acceptedActionsStore,
                           @Nonnull final IdentityService<ActionInfo> actionIdentityService,
                           @Nonnull final InvolvedEntitiesExpander involvedEntitiesExpander,
                           @Nonnull final ActionAuditSender actionAuditSender
    ) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.topologyContextId = topologyContextId;
        this.severityCache = new EntitySeverityCache(repositoryService, true);
        this.actionTargetSelector = actionTargetSelector;
        this.probeCapabilityCache = probeCapabilityCache;
        this.entitySettingsCache = entitySettingsCache;
        this.actionHistoryDao = actionHistoryDao;
        this.clock = clock;
        this.actions = new LiveActions(
            actionHistoryDao, acceptedActionsStore, clock, userSessionContext,
            involvedEntitiesExpander);
        this.actionsStatistician = Objects.requireNonNull(liveActionsStatistician);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.atomicActionFactory = Objects.requireNonNull(atomicActionFactory);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.licenseCheckClient = Objects.requireNonNull(licenseCheckClient);
        this.actionIdentityService = Objects.requireNonNull(actionIdentityService);
        this.entitiesWithNewStateCache = new EntitiesWithNewStateCache(actions);
        this.acceptedActionStore = acceptedActionsStore;
        this.actionAuditSender = Objects.requireNonNull(actionAuditSender);
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
     *
     * @throws InterruptedException if current thread has been interrupted
     */
    @Override
    public boolean populateRecommendedActions(@Nonnull final ActionPlan actionPlan)
            throws InterruptedException {

        synchronized (storePopulationLock) {
            if (actionPlan.getInfo().hasBuyRi()) {
                return populateBuyRIActions(actionPlan);
            }

            final TopologyInfo sourceTopologyInfo =
                actionPlan.getInfo().getMarket().getSourceTopologyInfo();

            final Long topologyContextId = sourceTopologyInfo.getTopologyContextId();

            final Long topologyId = sourceTopologyInfo.getTopologyId();

            // Check if the atomic action factory contains specs to create atomic actions
            // for the actions received, create atomic actions if the specs are received
            // from the toplogy processor
            List<ActionDTO.Action> atomicActions = Collections.emptyList();
            Map<Long, ActionDTO.Action> mergedActions = new HashMap<>();
            Collection<Long> atomicActionEntities = Collections.emptyList();

            if (atomicActionFactory.canMerge()) {
                List<AtomicActionResult> atomicActionResults = atomicActionFactory.merge(actionPlan.getActionList());
                logger.info("Created {} atomic actions", atomicActionResults.size());

                atomicActions =
                        atomicActionResults.stream()
                                .map(atomicActionResult -> atomicActionResult.atomicAction())
                                .collect(Collectors.toList());

                for (AtomicActionResult result : atomicActionResults) {
                    result.marketActions().stream().forEach(action -> {
                        mergedActions.put(action.getId(), action);
                    });
                }
                atomicActionEntities = ActionDTOUtil.getInvolvedEntityIds(atomicActions);
            }

            Collection<Long> actionEntities = ActionDTOUtil.getInvolvedEntityIds(actionPlan.getActionList());
            // adding the target entities from atomic actions to the snapshot
            Set<Long> involvedEntities = Stream.of(atomicActionEntities, actionEntities)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            final EntitiesAndSettingsSnapshot snapshot = entitySettingsCache.newSnapshot(involvedEntities,
                            Collections.emptySet(), topologyContextId, topologyId);

            List<ActionDTO.Action> allActions = Stream.of(actionPlan.getActionList(), atomicActions)
                                                        .flatMap(Collection::stream)
                                                        .collect(Collectors.toList());

            // This call requires some computation and an RPC call, so do it outside of the
            // action lock.
            final List<ActionDTO.Action> allActionsWithAdditionalInfo;
            try {
                allActionsWithAdditionalInfo =
                        new ArrayList<>(actionsWithAdditionalInfo(allActions, mergedActions, snapshot));
            } catch (IdentityServiceException e) {
                logger.error("Error retrieving OIDs for actions", e);
                return false;
            }

            if (logger.isDebugEnabled()) {
                allActionsWithAdditionalInfo.stream().forEach(
                        entry -> {
                            if (entry.getInfo().hasResize()) {
                                logger.debug("entity {} action {} has support level {}",
                                        entry.getInfo().getResize().getTarget().getId(),
                                        entry.getId(), entry.getSupportingLevel());
                            }
                        }
                );
            }

            // RecommendationTracker to accelerate lookups of recommendations.
            RecommendationTracker recommendations = new RecommendationTracker();

            // Apply addition and removal to the internal store atomically.
            final List<ActionView> completedSinceLastPopulate = new ArrayList<>();
            final List<Long> actionsToRemove = new ArrayList<>();

            actions.doForEachMarketAction(action -> {
                // Only retain IN-PROGRESS, QUEUED, ACCEPTED and READY actions which are re-recommended.
                switch (action.getState()) {
                    case IN_PROGRESS:
                    case QUEUED:
                        recommendations.add(action);
                        break;
                    case READY:
                    case ACCEPTED:
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
            final Iterator<Long> recommendationOids;
            try {
                recommendationOids = actionIdentityService.getOidsForObjects(
                        Lists.transform(allActionsWithAdditionalInfo, ActionDTO.Action::getInfo))
                        .iterator();
            } catch (IdentityServiceException e) {
                logger.error("Error retrieving OIDs for actions", e);
                return false;
            }
            final List<Action> actionsToTranslate =
                    new ArrayList<>(allActionsWithAdditionalInfo.size());
            for (ActionDTO.Action recommendedAction : allActionsWithAdditionalInfo) {
                final long recommendationOid = recommendationOids.next();
                final Optional<Action> existingActionOpt = recommendations.take(recommendationOid);
                final Action action;
                if (existingActionOpt.isPresent()) {
                    action = existingActionOpt.get();

                    // If we are re-using an existing action, we should update the recommendation
                    // so other properties that may have changed (e.g. importance, executability)
                    // reflect the most recent recommendation from the market. However, we only
                    // do this for "READY" or "ACCEPTED" actions. An IN_PROGRESS or QUEUED action
                    // is considered "fixed" until it either succeeds or fails.
                    // TODO (roman, Oct 31 2018): If a QUEUED action becomes non-executable, it
                    // may be worth clearing it.
                    if (action.getState() == ActionState.READY || action.getState() == ActionState.ACCEPTED) {
                        action.updateRecommendation(recommendedAction);
                    }
                } else {
                    newActionCounts.getAndIncrement();
                    action = actionFactory.newAction(recommendedAction, planId, recommendationOid);
                    newActionIds.add(action.getId());
                }
                final ActionState actionState = action.getState();
                if (actionState == ActionState.READY || actionState == ActionState.ACCEPTED) {
                    actionsToTranslate.add(action);
                }
            }
            final Stream<Action> translatedReadyActions =
                    actionTranslator.translate(actionsToTranslate.stream(), snapshot);

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
            actions.updateMarketActions(actionsToRemove, translatedActionsToAdd, snapshot, actionTargetSelector);

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
            final int deletedActions =
                entitiesWithNewStateCache.clearActionsAndUpdateCache(sourceTopologyInfo.getTopologyId());
            final List<ActionView> newActions = newActionIds.stream().map(actions::get)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            auditOnGeneration(newActions);
            if (deletedActions > 0) {
                severityCache.refresh(this);
            }

        }

        return true;
    }

    private void auditOnGeneration(@Nonnull Collection<ActionView> newActions)
            throws InterruptedException {
        try {
            actionAuditSender.sendActionEvents(newActions);
        } catch (CommunicationException e) {
            logger.warn(
                    "Failed sending audit event \"on generation event\" for actions " + newActions,
                    e);
        }
    }

    /**
     * Add additional info to the actions, such as support level and pre-requisites of an action.
     *
     * @param allActions action plan and atomic actions
     * @param mergedActions actions that were merged to create atomic actions
     * @param snapshot the snapshot of entities
     * @return a collection of actions with additional information added to them
     * @throws IdentityServiceException if exceptions occurred while
     */
    @Nonnull
    private Collection<ActionDTO.Action> actionsWithAdditionalInfo(
            @Nonnull final List<ActionDTO.Action> allActions,
            Map<Long, ActionDTO.Action> mergedActions,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) throws IdentityServiceException {
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
                                actionView.getRecommendation(), actionView.getActionPlanId(),
                                actionView.getRecommendationOid())));
            final Iterator<Long> oidIterator = actionIdentityService.getOidsForObjects(
                            allActions
                            .stream()
                            .map(ActionDTO.Action::getInfo)
                            .collect(Collectors.toList())).iterator();
            final List<ActionDTO.Action> newActions = new ArrayList<>(allActions.size());
            for (ActionDTO.Action action: allActions) {
                final Long recommendationOid = oidIterator.next();
                final Optional<Action> filteredAction =
                        lastExecutedRecommendationsTracker.take(recommendationOid);
                if (filteredAction.isPresent()) {
                    logger.debug("Skipping action: {} as it has already been executed", action);
                } else {
                    newActions.add(action);
                }
            }
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
                ActionDTO.Action marketAction = mergedActions.get(action.getId());
                if (marketAction != null) {
                    // mark merged actions as recommend only
                    return marketAction.toBuilder()
                            .setSupportingLevel(SupportLevel.SHOW_ONLY)
                            .build();
                } else if (action.getSupportingLevel() != supportLevel || !prerequisites.isEmpty()) {
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

        final Iterator<Long> recommendationOids;
        try {
            recommendationOids = actionIdentityService.getOidsForObjects(
                    Lists.transform(actionPlan.getActionList(), ActionDTO.Action::getInfo))
                    .iterator();
        } catch (IdentityServiceException e) {
            logger.error("Failed assigning OIDs to actions from plan " + actionPlan.getId(), e);
            return false;
        }
        final List<Action> actionsFromPlan = new ArrayList<>(actionPlan.getActionCount());
        for (ActionDTO.Action recommendedAction: actionPlan.getActionList()) {
            final long recommendationOid = recommendationOids.next();
            final Action action = actionFactory.newAction(recommendedAction, planId, recommendationOid);
            actionsFromPlan.add(action);
        }
        // All RI translations should be passthrough, but we do it here anyway for consistency
        // with the "normal" action case.
        actions.replaceRiActions(actionTranslator.translate(actionsFromPlan.stream(), snapshot));
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
     * Updates the enitities with new state cache.
     *
     * @param entitiesWithNewState ids of hosts that went into maintenance.
     */
    public void updateActionsBasedOnNewStates(@Nonnull final EntitiesWithNewState entitiesWithNewState) {

        entitiesWithNewStateCache.updateHostsWithNewState(entitiesWithNewState);
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
