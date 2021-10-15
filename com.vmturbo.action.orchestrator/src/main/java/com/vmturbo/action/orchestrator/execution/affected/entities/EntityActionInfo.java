package com.vmturbo.action.orchestrator.execution.affected.entities;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

/**
 * Contains all the information needed for maintain entity action info.
 */
public class EntityActionInfo {
    private final long actionId;
    private final ActionTypeCase actionType;
    private final Set<Long> affectedEntities;
    private EntityActionInfoState state;

    /**
     * Unfortunately, we are stuck using local date time since ActionHistoryDao uses ExecutableStep,
     * which only exposes LocalDateTime. Internally ExecutableStep uses the millis since epoch then
     * converts to local date time when you request the time. As a result, it will be safe to use
     * LocalDateTime.now(clock).
     */
    private LocalDateTime lastUpdateTime;

    /**
     * Creates an instance of entity action info.
     *
     * @param actionId the id of the action.
     * @param actionType the type of the action.
     * @param state the current state of the action.
     * @param affectedEntities the entities affected by the action.
     * @param updateTime the most recent datetime the action was updated.
     */
    public EntityActionInfo(
            final long actionId,
            @Nonnull final ActionTypeCase actionType,
            @Nonnull final EntityActionInfoState state,
            @Nonnull final Set<Long> affectedEntities,
            @Nonnull final LocalDateTime updateTime) {
        this.actionId = actionId;
        this.actionType = actionType;
        this.state = state;
        this.affectedEntities = affectedEntities;
        this.lastUpdateTime = updateTime;
    }

    /**
     * The type of the action.
     * @return type of the action.
     */
    public ActionTypeCase getActionType() {
        return actionType;
    }

    /**
     * The state of the action.
     * @return the state of the action.
     */
    public EntityActionInfoState getState() {
        return state;
    }

    /**
     * Returns the affected entities by this action.
     *
     * @return the affected entities by this action.
     */
    public Set<Long> getAffectedEntities() {
        return affectedEntities;
    }

    /**
     * Returns the last time we received an update about this action.
     *
     * @return the last time we received an update about this action.
     */
    public LocalDateTime getLastUpdateTime() {
        return lastUpdateTime;
    }

    /**
     * Sets the state.
     *
     * @param state the state.
     */
    public void setState(final EntityActionInfoState state) {
        this.state = state;
    }

    /**
     * Sets the last update time.
     *
     * @param lastUpdateTime the last update time.
     */
    public void setLastUpdateTime(final LocalDateTime lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    /**
     * Returns the action id.
     *
     * @return the action id.
     */
    public long getActionId() {
        return actionId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EntityActionInfo that = (EntityActionInfo)o;
        return actionId == that.actionId && Objects.equals(actionType, that.actionType) && Objects.equals(affectedEntities,
                that.affectedEntities) && Objects.equals(state, that.state)
                && Objects.equals(lastUpdateTime, that.lastUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actionId, actionType, affectedEntities, state, lastUpdateTime);
    }
}
