package com.vmturbo.cost.component.savings;

import java.util.List;

/**
 * Algorithm interface.
 */
public interface Algorithm {
    /**
     * Add an action delta (price change) to the active action list.
     *
     * @param delta amount of the price change.
     * @param expirationTimestamp time when the action will expire.
     */
    void addAction(double delta, long expirationTimestamp);

    /**
     * Remove an action delta (price change) from the active action list. This must be the first
     * action in the action list.  If the timestamp does match, the removal will be ignored.
     *
     * @param expirationTimestamp time when the action will expire.
     * @return true if the action was removed.
     */
    boolean removeAction(long expirationTimestamp);

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
     * Return the action/delta list.
     *
     * @return the action list
     */
    List<Double> getActionList();

    /**
     * Return the expiration times list.
     *
     * @return the expiration times list
     */
    List<Long> getExpirationList();

    /**
     * Get the next action expiration time.
     *
     * @return the expiration time of the next action, if present.  If there are no active actions,
     *          the expiration time of 12-31-9999 23:59:59 will be returned instead.
     */
    long getNextExpirationTime();

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

}

