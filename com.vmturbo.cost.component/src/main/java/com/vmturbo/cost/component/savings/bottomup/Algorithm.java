package com.vmturbo.cost.component.savings.bottomup;

import java.util.Deque;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.math.DoubleMath;
import com.google.gson.annotations.SerializedName;

import com.vmturbo.cost.component.savings.EntityState;
import com.vmturbo.cost.component.savings.temold.ProviderInfo;

/**
 * Algorithm interface.
 */
public interface Algorithm {
    /**
     * Add an action delta (price change) to the active action list.
     *
     * @param timestamp timestamp of the action related to the delta.
     * @param delta amount of the price change.
     * @param expirationTimestamp time when the action will expire.
     */
    void addAction(long timestamp, double delta, long expirationTimestamp);

    /**
     * Remove an action delta (price change) from the active action list. This must be the first
     * action in the action list.  If the timestamp does match, the removal will be ignored.
     *
     * @param expirationTimestamp time when the action will expire.
     */
    void removeActionsOnOrBefore(long expirationTimestamp);

    /**
     * Close out the current interval.  This resets periodic values and prepares for the next interval.
     *
     * @param periodStartTime time that the period started.
     * @param periodEndTime time that the period ended.
     */
    void endPeriod(long periodStartTime, long periodEndTime);

    /**
     * Close out the current segment in preparation for a transition to a new one.
     *
     * @param timestamp time that the segment ends.
     */
    void endSegment(long timestamp);

    /**
     * Read entity state that is needed to run the algorithm into local state.
     *
     * @param timestamp period timestamp
     * @param entityState entity state being tracked
     */
    void initState(long timestamp, EntityState entityState);

    /**
     * Return the entity ID associated with this algorithm state.
     *
     * @return the entity ID
     */
    Long getEntityOid();

    /**
     * Return the realized savings and investments for this period.
     *
     * @return realized SavingsInvestments for the period.
     */
    SavingsInvestments getRealized();

    /**
     * Return the missed savings and investments for this period.
     *
     * @return missed SavingsInvestments for the period.
     */
    SavingsInvestments getMissed();

    /**
     * Set the current recommendation. Only the related price change is needed.
     *
     * @param recommendation recommendation to save
     */
    void setCurrentRecommendation(EntityPriceChange recommendation);

    /**
     * Get the current active recommendation.
     *
     * @return the current active recommendation, or null if there is none.
     */
    @Nonnull
    EntityPriceChange getCurrentRecommendation();

    /**
     * Mark/unmark the associated entity for removal.
     *
     * @param deletePending true to mark the entity for removal after the current stats period,
     * else false.
     */
    void setDeletePending(boolean deletePending);

    /**
     * Return the deletion pending status.
     *
     * @return true if the associated entity state should be deleted after the current stats period.
     */
    boolean getDeletePending();

    /**
     * Set the current power factor.
     *
     * @param powerFactor To set the power status to on, use 1.  To set the power status to off,
     * use 0.  Any other value will yield interesting (but not necessarily incorrect) results.
     */
    void setPowerFactor(long powerFactor);

    /**
     * Return the current power factor.  This factor is multiplied by the current savings and
     * investment to get the amount that is accumulated for any given segment.
     *
     * @return the current power factor.
     */
    long getPowerFactor();

    /**
     * Return the delta list.
     *
     * @return the delta list
     */
    Deque<Delta> getDeltaList();

    /**
     * Get the next action expiration time.
     *
     * @return the expiration time of the next action, if present.  If there are no active actions,
     *          the expiration time of 12-31-9999 23:59:59 will be returned instead.
     */
    long getNextExpirationTime();

    /**
     * Clear the action list and related expiration list.
     *
     * @return the expiration list before clearing.
     */
    Deque<Delta> clearActionState();

    /**
     * Get the last execution action.
     *
     * @return last executed action.
     */
    @Nonnull
    Optional<ActionEntry> getLastExecutedAction();

    /**
     * Set the last execution action.
     *
     * @param lastExecutedAction last executed action.
     */
    void setLastExecutedAction(ActionEntry lastExecutedAction);

    /**
     * Remove the last action from the action and expiration lists.  After the action is removed,
     * the periodic savings and next expiration time are updated.
     */
    void removeLastAction();

    /**
     * Get the current provider OID.  We learn this from past events:
     * - If there is an ACTIVE current recommendation, use its source OID
     * - Else if there is a last executed action, use its destination OID
     * - Else, use the inactive recommendation's source OID. Entity state
     *   that was created before action revert was implemented can return
     *   a null current provider.  All entity state created after the
     *   feature was added are guaranteed to have a non-null provider.
     *
     * @return the entity's current provider, or null if it cannot be determined.
     */
    @Nullable
    default Long getCurrentProvider() {
        EntityPriceChange currentRecommendation = getCurrentRecommendation();
        if (currentRecommendation == EntityPriceChange.EMPTY) {
            return null;
        }
        if (currentRecommendation.active()) {
            return currentRecommendation.getSourceOid();
        }
        if (getLastExecutedAction().isPresent()) {
            return getLastExecutedAction().get().getDestinationOid();
        }
        return currentRecommendation.getSourceOid();
    }

    /**
     * Get provider info. (e.g. provider OID, commodity capacities, etc.)
     *
     * @return provider info
     */
    @Nonnull
    ProviderInfo getProviderInfo();

    /**
     * Set provider info.
     *
     * @param providerInfo provider info
     */
    void setProviderInfo(ProviderInfo providerInfo);

    /**
     * Group of savings and investments together.
     */
    class SavingsInvestments {
        double savings;
        double investments;

        SavingsInvestments() {
        }

        SavingsInvestments(double savings, double investments) {
            this.savings = savings;
            this.investments = investments;
        }

        public double getSavings() {
            return savings;
        }

        public double getInvestments() {
            return investments;
        }

        @Override
        public String toString() {
            return String.format("Savings = %f, Investments = %f", savings, investments);
        }
    }

    /**
     * Helper class to capture a price change related to a tracked action.
     */
    class Delta {
        /**
         * Time in milliseconds when the delta was created.
         */
        @SerializedName("t")
        public long timestamp;

        /**
         * Difference in cost. Negative is savings, positive is investment.
         */
        @SerializedName("d")
        public double delta;

        /**
         * Time in milliseconds when the delta expires.
         */
        @SerializedName("e")
        public long expiration;

        /**
         * Helper class to describe an action delta.
         *
         * @param timestamp timestamp that the delta was realized
         * @param delta cost difference
         * @param expiration timestamp that the delta will expire
         */
        public Delta(long timestamp, double delta, long expiration) {
            this.timestamp = timestamp;
            this.delta = delta;
            this.expiration = expiration;
        }

        /**
         * Compare two deltas.
         *
         * @param other Delta to compare this with
         * @return true if the deltas are equal
         */
        public boolean equals(Delta other) {
            return this.timestamp == other.timestamp && DoubleMath.fuzzyEquals(this.delta, other.delta, .0001d)
                    && this.expiration == other.expiration;
        }
    }
}

