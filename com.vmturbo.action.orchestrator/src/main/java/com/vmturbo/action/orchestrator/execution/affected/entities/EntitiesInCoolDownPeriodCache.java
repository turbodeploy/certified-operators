package com.vmturbo.action.orchestrator.execution.affected.entities;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

/**
 * In-memory cache for affected entities.
 * Only handles inserting or updating EntityActionInfo and expiring old entries.
 */
public class EntitiesInCoolDownPeriodCache {

    private final Clock clock;
    // action id -> entity action info
    private final Map<Long, EntityActionInfo> entityActionInfoByActionId = new HashMap<>();

    private final long entitiesInCoolDownPeriodCacheSizeInMsec;

    /**
     * Affected entities cache.
     *
     * @param clock the clock to track the time
     * @param entitiesInCoolDownPeriodCacheSizeInMins how long we should keep entries in the cache for
     */
    public EntitiesInCoolDownPeriodCache(
            @Nonnull final Clock clock,
            final int entitiesInCoolDownPeriodCacheSizeInMins) {
        this.clock = Objects.requireNonNull(clock);
        this.entitiesInCoolDownPeriodCacheSizeInMsec = TimeUnit.MINUTES.toMillis(
                entitiesInCoolDownPeriodCacheSizeInMins);
    }

    /**
     * Insert or update an action into the affected entities cache.
     *
     * @param actionId the action id
     * @param actionType the action type
     * @param actionState the action state
     * @param affectedEntities the list of the affected entities
     */
    public synchronized void insertOrUpdate(
            long actionId,
            @Nonnull ActionTypeCase actionType,
            @Nonnull EntityActionInfoState actionState,
            @Nonnull Set<Long> affectedEntities) {
        insertOrUpdate(actionId, actionType, actionState, affectedEntities, null);
    }

    /**
     * Insert or update an action into the affected entities cache.
     *
     * @param actionId the action id
     * @param actionType the action type
     * @param actionState the action state
     * @param affectedEntities the list of the affected entities
     * @param lastUpdatedDatetime the datetime that the action was last updated. Unfortunately,
     *                            we are stuck using local date time since ActionHistoryDao uses ExecutableStep,
     *                            which only exposes LocalDateTime.
     */
    public synchronized void insertOrUpdate(
            long actionId,
            @Nonnull ActionTypeCase actionType,
            @Nonnull EntityActionInfoState actionState,
            @Nonnull Set<Long> affectedEntities,
            @Nullable LocalDateTime lastUpdatedDatetime) {
        if (lastUpdatedDatetime == null) {
            lastUpdatedDatetime = LocalDateTime.now(clock);
        }
        if (entityActionInfoByActionId.containsKey(actionId)) {
            EntityActionInfo existingEntityActionInfo = entityActionInfoByActionId.get(actionId);
            existingEntityActionInfo.setState(actionState);
            existingEntityActionInfo.setLastUpdateTime(lastUpdatedDatetime);
        } else {
            final EntityActionInfo entityActionInfo = new EntityActionInfo(actionId, actionType,
                    actionState, affectedEntities, lastUpdatedDatetime);
            entityActionInfoByActionId.put(actionId, entityActionInfo);
        }
    }

    /**
     * Get the affected actions for the provided action types within the provided
     * timeout config.
     *
     * @param affectedActionTypes the action types to search for.
     * @param inProgressActionCoolDownMsec how long since the last in progress message.
     * @param succeededActionsCoolDownMsec how long since the action succeeded.
     * @return the affected actions for the provided action types within the provided
     *         timeout config.
     */
    public synchronized Set<Long> getAffectedEntitiesByActionType(
            Set<ActionTypeCase> affectedActionTypes,
            long inProgressActionCoolDownMsec,
            long succeededActionsCoolDownMsec) {

        return entityActionInfoByActionId.values().stream()
                .filter(entityAction -> affectedActionTypes.contains(entityAction.getActionType()))
                .filter(Objects::nonNull)
                .filter(entityActionInfo -> isControllableLogicStillApplied(
                        entityActionInfo,
                        inProgressActionCoolDownMsec,
                        succeededActionsCoolDownMsec))
                .map(EntityActionInfo::getAffectedEntities)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Instead of using a thread pool, we allow the consumer to control when clean up is triggered, se we can
     * steal their thread.
     */
    public synchronized void removeOldEntries() {
        final Set<Long> actionIdsToRemove = entityActionInfoByActionId.values().stream()
                .filter(this::isTooOld)
                .map(EntityActionInfo::getActionId)
                .collect(Collectors.toSet());
        actionIdsToRemove.forEach(entityActionInfoByActionId::remove);
    }

    private boolean isTooOld(final EntityActionInfo entityActionInfo) {
        LocalDateTime currentDate = LocalDateTime.now(clock);
        LocalDateTime lastUpdateToAction = entityActionInfo.getLastUpdateTime();
        return Duration.between(lastUpdateToAction, currentDate).toMillis() > entitiesInCoolDownPeriodCacheSizeInMsec;
    }

    private boolean isControllableLogicStillApplied(
            final EntityActionInfo entityActionInfo,
            final long inProgressActionCoolDownMsec,
            final long succeededActionsCoolDownMsec) {
        final EntityActionInfoState actionState = entityActionInfo.getState();
        final LocalDateTime lastUpdateTime = entityActionInfo.getLastUpdateTime();
        if (actionState == EntityActionInfoState.SUCCEEDED) {
            return Duration.between(lastUpdateTime, LocalDateTime.now(clock)).toMillis()
                    <= succeededActionsCoolDownMsec;
        } else if (actionState == EntityActionInfoState.IN_PROGRESS) {
            return Duration.between(lastUpdateTime, LocalDateTime.now(clock)).toMillis()
                    <= inProgressActionCoolDownMsec;
        } else if (actionState == EntityActionInfoState.FAILED) {
            return false;
        }
        // affection is actual only in cases if action is in-progress or succeeded and cool down period is not expired
        return false;
    }
}

