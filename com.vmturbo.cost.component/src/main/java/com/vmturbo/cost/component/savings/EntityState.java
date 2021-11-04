package com.vmturbo.cost.component.savings;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Class to encapsulate the entity states that need to be persisted or to be passed from the
 * savings calculator to EntitySavingsTracker.
 */
public class EntityState {
    /**
     * Used to JSONify fields.
     */
    private static final Gson GSON = createGson();

    /**
     * OID of entity.
     */
    private final long entityId;

    /**
     * Calculator will set the deletePending flag to true if the entity is deleted.
     * The tracker will delete the state after recording the stats for this entity.
     * This value is marked transient and does not need to be persisted because only states of
     * active entities are tracked.
     */
    private transient boolean deletePending;

    /**
     * The time that the next active action this entity state will expire.  Entities with expired
     * actions must run at the next available time regardless of whether the entity has pending
     * events to process, in order to process the expired actions.
     */
    private transient long nextExpirationTime;

    /**
     * Power state of the entity. 1 is powered on; 0 is powered off.
     */
    private long powerFactor;

    /**
     * Contains current cost info in the recommendation active for an entity.
     */
    private EntityPriceChange currentRecommendation;

    /**
     * List of values reflecting the impact of actions.
     */
    private List<Double> actionList;

    /**
     * Last executed action information.
     */
    private @Nonnull Optional<ActionEntry> lastExecutedAction;

    /**
     * Realized savings.
     */
    private Double realizedSavings;

    /**
     * Realized investments.
     */
    private Double realizedInvestments;

    /**
     * Missed savings.
     */
    private Double missedSavings;

    /**
     * Missed investments.
     */
    private Double missedInvestments;

    /**
     * List of expiration times for the actions in the action list.
     */
    private List<Long> expirationList;

    /**
     * Boolean flag to indicate this state was updated as a result of an event.
     * The flag is used to indicate that this state will need to be processed again in the next
     * period even if there will be no events detected for this entity in the next period.
     */
    private transient boolean updated;

    /**
     * Constructor.
     *
     * @param entityId entity ID
     * @param currentRecommendation the entity price change associated with the current
     *      recommendation.
     */
    public EntityState(long entityId, @Nonnull EntityPriceChange currentRecommendation) {
        this.entityId = entityId;
        this.deletePending = false;
        this.powerFactor = 1L;
        this.currentRecommendation = currentRecommendation;

        // Initialize the current action list and their expiration times. Not scaled by
        // period length.
        this.actionList = new ArrayList<>();
        this.expirationList = new ArrayList<>();
        this.lastExecutedAction = Optional.empty();
    }

    public long getEntityId() {
        return entityId;
    }

    public long getPowerFactor() {
        return powerFactor;
    }

    public void setPowerFactor(long powerFactor) {
        this.powerFactor = powerFactor;
    }

    @Nullable
    public EntityPriceChange getCurrentRecommendation() {
        return currentRecommendation;
    }

    public void setCurrentRecommendation(@Nonnull EntityPriceChange currentRecommendation) {
        this.currentRecommendation = currentRecommendation;
    }

    public boolean isDeletePending() {
        return this.deletePending;
    }

    public void setDeletePending(boolean active) {
        this.deletePending = active;
    }

    @Nonnull
    public List<Double> getActionList() {
        return actionList;
    }

    public void setActionList(final List<Double> actionList) {
        this.actionList = actionList;
    }

    public Double getRealizedSavings() {
        return realizedSavings;
    }

    public void setRealizedSavings(final Double realizedSavings) {
        this.realizedSavings = realizedSavings;
    }

    public Double getRealizedInvestments() {
        return realizedInvestments;
    }

    public void setRealizedInvestments(final Double realizedInvestments) {
        this.realizedInvestments = realizedInvestments;
    }

    public Double getMissedSavings() {
        return missedSavings;
    }

    public void setMissedSavings(final Double missedSavings) {
        this.missedSavings = missedSavings;
    }

    public Double getMissedInvestments() {
        return missedInvestments;
    }

    public void setMissedInvestments(final Double missedInvestments) {
        this.missedInvestments = missedInvestments;
    }

    public boolean isUpdated() {
        return updated;
    }

    public void setUpdated(final boolean updated) {
        this.updated = updated;
    }

    /**
     * Return the current expiration list.
     *
     * @return the expiration list
     */
    @Nonnull
    public List<Long> getExpirationList() {
        if (expirationList == null) {
            expirationList = new ArrayList<>();
        }
        return expirationList;
    }

    public void setExpirationList(@Nonnull final List<Long> expirationList) {
        this.expirationList = expirationList;
    }

    public void setNextExpirationTime(long expirationTime) {
        this.nextExpirationTime = expirationTime;
    }

    public long getNextExpirationTime() {
        return nextExpirationTime;
    }

    private static Gson createGson() {
        return (new GsonBuilder())
                .registerTypeAdapterFactory(new GsonAdaptersEntityPriceChange())
                .registerTypeAdapterFactory(new GsonAdaptersActionEntry())
                .create();
    }

    /**
     * De-serialize string to this object.
     *
     * @param jsonSerialized String read from DB to make into this object.
     * @return StateInfo object made out of JSON string.
     */
    public static EntityState fromJson(@Nonnull final String jsonSerialized) {
        return GSON.fromJson(jsonSerialized, EntityState.class);
    }

    /**
     * Serialize this StateInfo object into JSON string, for saving to DB.
     *
     * @return JSON string.
     */
    public String toJson() {
        return GSON.toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EntityState that = (EntityState)o;
        return entityId == that.entityId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityId);
    }

    @Nullable
    public Optional<ActionEntry> getLastExecutedAction() {
        return lastExecutedAction;
    }

    public void setLastExecutedAction(@Nonnull Optional<ActionEntry> lastExecutedAction) {
        this.lastExecutedAction = lastExecutedAction;
    }
}
