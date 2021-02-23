package com.vmturbo.cost.component.savings;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

/**
 * Class to encapsulate the entity states that need to be persisted or to be passed from the
 * savings calculator to EntitySavingsTracker.
 */
public class EntityState {
    /**
     * OID of entity.
     */
    private long entityId;

    /**
     * Calculator will set the deletePending flag to true if the entity is deleted.
     * The tracker will delete the state after recording the stats for this entity.
     * This value is marked transient and does not need to be persisted because only states of
     * active entities are tracked.
     */
    private transient boolean deletePending;

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

    // Savings or investments of a period are transient values because they are persisted in the
    // form of stats.
    /**
     * Realized savings.
     */
    private transient Double realizedSavings;

    /**
     * Realized investments.
     */
    private transient Double realizedInvestments;

    /**
     * Missed savings.
     */
    private transient Double missedSavings;

    /**
     * Missed investments.
     */
    private transient Double missedInvestments;

    EntityState(long entityId) {
        this.entityId = entityId;
        this.deletePending = false;
        this.powerFactor = 1L;

        // The current realized and missed values. Not scaled by period length.
        this.actionList = new ArrayList<>();
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

    public void setCurrentRecommendation(@Nullable EntityPriceChange currentRecommendation) {
        this.currentRecommendation = currentRecommendation;
    }

    public boolean isDeletePending() {
        return this.deletePending;
    }

    public void setDeletePending(boolean active) {
        this.deletePending = active;
    }

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
}
