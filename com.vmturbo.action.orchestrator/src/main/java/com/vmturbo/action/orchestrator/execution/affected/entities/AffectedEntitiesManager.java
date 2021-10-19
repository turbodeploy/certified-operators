package com.vmturbo.action.orchestrator.execution.affected.entities;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionStateMachine;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.ActionEffectType;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.AffectedEntitiesTimeoutConfig;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class responsible for mapping from ActionState to EntityActionInfoState,
 * initializing the cache,
 * and translating action state changes to updates to the cache,
 * and figuring out what the affected entities of an action are.
 *
 * <p>This can no longer exist in Topology Processor because it has no idea
 * about how to reconcile multiple steps (PRE/REPLACE/POST) of the action.
 * Topology Processor is only able to see each Action Step as a separate action.</p>
 */
public class AffectedEntitiesManager implements ActionStateMachine.ActionEventListener {

    private static final Logger logger = LogManager.getLogger();
    private final ActionHistoryDao actionHistoryDao;
    private final Clock clock;
    private final long entitiesInCoolDownPeriodCacheSizeInMsec;
    private final EntitiesInCoolDownPeriodCache entitiesInCoolDownPeriodCache;

    private static final Map<ActionEffectType, Set<ActionTypeCase>> ACTION_TYPES_BY_AFFECTION_TYPE =
            ImmutableMap.<ActionEffectType, Set<ActionTypeCase>>builder()
                    .put(ActionEffectType.NON_CONTROLLABLE, ImmutableSet.of(
                            ActionTypeCase.MOVE))
                    .put(ActionEffectType.NON_SUSPENDABLE, ImmutableSet.of(
                            ActionTypeCase.ACTIVATE,
                            ActionTypeCase.PROVISION))
                    .put(ActionEffectType.INELIGIBLE_SCALE, ImmutableSet.of(
                            ActionTypeCase.SCALE))
                    .put(ActionEffectType.INELIGIBLE_RESIZE_DOWN, ImmutableSet.of(
                            ActionTypeCase.RESIZE,
                            ActionTypeCase.ATOMICRESIZE))
                    .build();

    private static final Set<ActionTypeCase> RELEVANT_ACTION_TYPES = ACTION_TYPES_BY_AFFECTION_TYPE.values().stream()
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

    private static final Set<Integer> CHANGE_PROVIDER_CONTROLLABLE_SUPPORTED_ENTITIES = ImmutableSet.of(
            EntityType.VIRTUAL_MACHINE_VALUE, EntityType.VIRTUAL_VOLUME_VALUE,
            EntityType.DATABASE_SERVER_VALUE);

    /**
     * Use the history dao for initializing the cache and the cache will use the clock for timing.
     * Failure to initialize the cache will throw a runtime exception which will fail application startup.
     *
     * @param actionHistoryDao the DAO for working with completed actions
     * @param clock the clock to track the time. Useful for injecting your own timing in unit tests.
     * @param entitiesInCoolDownPeriodCacheSizeInMins the time range of data the cache should keep in memory measured in
     *                                                minutes.
     * @param entitiesInCoolDownPeriodCache the cache used for storing the entity entities affected by actions used
     *                                      by controllable flags.
     */
    public AffectedEntitiesManager(
            @Nonnull ActionHistoryDao actionHistoryDao,
            @Nonnull Clock clock,
            final int entitiesInCoolDownPeriodCacheSizeInMins,
            @Nonnull final EntitiesInCoolDownPeriodCache entitiesInCoolDownPeriodCache) {
        this.actionHistoryDao = actionHistoryDao;
        this.clock = clock;
        this.entitiesInCoolDownPeriodCacheSizeInMsec = TimeUnit.MINUTES.toMillis(entitiesInCoolDownPeriodCacheSizeInMins);
        this.entitiesInCoolDownPeriodCache = entitiesInCoolDownPeriodCache;
        initializeTheCache(entitiesInCoolDownPeriodCacheSizeInMins);
    }

    /**
     * Get the affected actions for the provided ActionEffectTypes within the provided
     * timeout configs.
     *
     * @param requestedAffectedEntities the affected actions to retrieve.
     * @return the affected actions for the provided ActionEffectTypes within the provided
     *      * timeout configs.
     */
    public Map<ActionEffectType, Set<Long>> getAllAffectedEntities(
            @Nonnull Map<ActionEffectType, AffectedEntitiesTimeoutConfig> requestedAffectedEntities) {
        entitiesInCoolDownPeriodCache.removeOldEntries();

        Map<ActionEffectType, Set<Long>> result = new HashMap<>();
        for (Map.Entry<ActionEffectType, AffectedEntitiesTimeoutConfig> entry: requestedAffectedEntities.entrySet()) {
            final ActionEffectType actionEffectType = entry.getKey();
            final AffectedEntitiesTimeoutConfig timeoutConfig = entry.getValue();

            if (timeoutConfig.getInProgressActionCoolDownMsec() > entitiesInCoolDownPeriodCacheSizeInMsec
                    || timeoutConfig.getCompletedActionCoolDownMsec() > entitiesInCoolDownPeriodCacheSizeInMsec) {
                logger.error("timeout config from topology processor"
                        + " (ActionAffectType={}, AffectedEntitiesTimeoutConfig={})"
                        + " is longer than the cache size entitiesInCoolDownPeriodCacheSizeInMsec={}",
                        () -> actionEffectType,
                        () -> timeoutConfig,
                        () -> entitiesInCoolDownPeriodCacheSizeInMsec);
            }

            final Set<ActionTypeCase> actionTypes = ACTION_TYPES_BY_AFFECTION_TYPE.get(entry.getKey());
            final long inProgressActionCoolDownMsec = timeoutConfig.getInProgressActionCoolDownMsec();
            final long completedActionCoolDownMsec = timeoutConfig.getCompletedActionCoolDownMsec();
            result.put(actionEffectType,
                entitiesInCoolDownPeriodCache.getAffectedEntitiesByActionType(
                    actionTypes,
                    inProgressActionCoolDownMsec,
                        completedActionCoolDownMsec));
        }
        return result;
    }

    @Override
    public void onActionEvent(
            @Nonnull final ActionView actionView,
            @Nonnull final ActionState preState,
            @Nonnull final ActionState postState,
            @Nonnull final ActionEvent event,
            final boolean performedTransition) {
        updateActionIfRelevant(actionView, postState, null);
    }

    private void updateActionIfRelevant(
            @Nonnull ActionView actionView,
            @Nonnull ActionState state,
            @Nullable LocalDateTime updateTime) {
        final Optional<EntityActionInfoState> actionState = EntityActionInfoState.getState(state);
        boolean updated = false;
        if (actionView.getRecommendation() != null
                && actionView.getRecommendation().hasInfo()
                && RELEVANT_ACTION_TYPES.contains(actionView.getRecommendation().getInfo().getActionTypeCase())
                && actionState.isPresent()) {
            Set<Long> affectedEntities = getControllableAffectedEntities(
                actionView.getId(),
                actionView.getRecommendation().getInfo());
            // no point in storing this action since there are no affected entities.
            if (!affectedEntities.isEmpty()) {
                entitiesInCoolDownPeriodCache.insertOrUpdate(
                    actionView.getId(),
                    actionView.getRecommendation().getInfo().getActionTypeCase(),
                    actionState.get(),
                    affectedEntities,
                    updateTime);
                updated = true;
            }
        }
        if (updated) {
            logger.trace(
                "Action {} with state {} recorded to the cache.",
                () -> actionView,
                () -> state);
        } else {
            logger.trace(
                "Action {} with state {} is not used by AffectedEntitiesManager.",
                () -> actionView,
                () -> state);
        }
    }

    /**
     * Move and scale use the primary entity, source, and destination as the effected entity.
     * All other action types use the primary entity as the only effected entity.
     *
     * @param actionId the id of the action.
     * @param actionInfo the Action dto to extract the affected entities from.
     * @return the affected entities.
     */
    private static Set<Long> getControllableAffectedEntities(
            long actionId,
            @Nonnull ActionInfo actionInfo) {
        switch (actionInfo.getActionTypeCase()) {
            case MOVE:
            case SCALE:
                return getChangeProviderControllableAffectedEntities(actionId, actionInfo);
            default:
                try {
                    return Optional.ofNullable(ActionDTOUtil.getPrimaryEntity(actionId, actionInfo, true))
                            .map(ActionEntity::getId)
                            .map(Collections::singleton)
                            .orElse(Collections.emptySet());
                } catch (UnsupportedActionException unsupportedActionException) {
                    logger.error("Could not extract the affected entities from the action."
                                    + " The action might not be formatted correctly. "
                                    + actionInfo,
                            unsupportedActionException);
                    return Collections.emptySet();
                }

        }
    }

    /**
     * This code is borrowed from ChangeProviderContext.getControllableAffectedEntities().
     *
     * @param actionId the id of the action.
     * @param actionInfo the Action dto to extract the affected entities from.
     * @return the affected entities.
     */
    @Nonnull
    private static Set<Long> getChangeProviderControllableAffectedEntities(
            long actionId,
            @Nonnull ActionInfo actionInfo) {
        final Set<Long> affectedEntities = new HashSet<>();
        final ActionEntity targetEntity;
        try {
            targetEntity = ActionDTOUtil.getPrimaryEntity(actionId, actionInfo, true);
            affectedEntities.add(targetEntity.getId());
        } catch (UnsupportedActionException unsupportedActionException) {
            logger.error("Could not extract the affected entities from the action."
                    + " The action might not be formatted correctly. "
                    + actionInfo,
                    unsupportedActionException);
            return Collections.emptySet();
        }

        // right now, only support controllable flag for VM Move/Scale and DB server/Virtual Volume Scale actions.
        if (targetEntity.hasType() && !CHANGE_PROVIDER_CONTROLLABLE_SUPPORTED_ENTITIES.contains(
                targetEntity.getType())) {
            logger.warn(
                    "Ignore controllable logic for action with entity type: " + targetEntity.getType());
            return Collections.emptySet();
        }

        // Include all providers affected by the move
        for (ChangeProvider changeProvider : ActionDTOUtil.getChangeProviderList(actionInfo)) {
            if (changeProvider.hasSource()) {
                affectedEntities.add(changeProvider.getSource().getId());
            }
            if (changeProvider.hasDestination()) {
                affectedEntities.add(changeProvider.getDestination().getId());
            }
        }
        return affectedEntities;
    }

    private void initializeTheCache(final int entitiesInCoolDownPeriodCacheSizeInMins) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final List<ActionView> completedActions = actionHistoryDao.getActionHistoryByDate(
                LocalDateTime.now(clock).minusMinutes(entitiesInCoolDownPeriodCacheSizeInMins),
                LocalDateTime.now(clock));
        completedActions.stream()
            // Only SUCCEEDED actions are used for controllable flags.
            .filter(action -> action.getState() == ActionState.SUCCEEDED)
            // SUCCEEDED action should have an executable step, but check just in case.
            .filter(action -> action.getCurrentExecutableStep().isPresent())
            // SUCCEEDED action should have a completion time, but check just in case.
            .filter(action -> action.getCurrentExecutableStep().get().getCompletionTime().isPresent())
            .forEach(action -> updateActionIfRelevant(
                    action,
                    ActionState.SUCCEEDED,
                    action.getCurrentExecutableStep().get().getCompletionTime().get()));
        stopWatch.stop();
        logger.info("Finished initializeTheCache for {} completed actions in {}",
                () -> completedActions.size(),
                () -> stopWatch.toString());
    }
}
