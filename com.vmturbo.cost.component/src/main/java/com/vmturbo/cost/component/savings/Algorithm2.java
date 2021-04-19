package com.vmturbo.cost.component.savings;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Implementation of algorithm-2.
 */
public class Algorithm2 implements Algorithm {
    /**
     * Logger.
     */
    private final Long entityOid;
    private long segmentStart;
    private long powerFactor;
    private EntityPriceChange currentRecommendation;


    // Internal state maintained by the algorithm
    private double savings;
    private double investment;
    // Matching lists that hold the price change (delta) of a tracked action and that action's
    // expiration time.  These are queues so that we can easily pop the expired deltas/timestamps
    // off of the lists.
    private final Queue<Double> actionList;
    private final Queue<Long> expirationList;
    private final SavingsInvestments periodicRealized;
    private final SavingsInvestments periodicMissed;
    private boolean deletePending;

    /**
     * Constructor for the algorithm state.  This implements Algorithm-2.
     *
     * @param entityOid OID of the associated entity being tracked.
     * @param segmentStart timestamp for the initial segment
     */
    public Algorithm2(Long entityOid, long segmentStart) {
        this.entityOid = entityOid;
        this.deletePending = false;
        this.segmentStart = segmentStart;
        this.powerFactor = 1;
        this.actionList = new LinkedList<>();
        this.expirationList = new LinkedList<>();
        this.savings = 0d;
        this.investment = 0d;

        // The current realized and missed values accumulated this period.  Not scaled by
        // period length.
        this.periodicRealized = new SavingsInvestments();
        this.periodicMissed = new SavingsInvestments();
    }

    /**
     * Return the sign of the argument. Different from Math.signum in that 0 returns positive
     * instead of 0.
     *
     * @param n value to check
     * @return -1 if n is negative, else 1
     */
    private static int signOf(double n) {
        return n < 0d ? -1 : 1;
    }

    /**
     * Subtract delta from current.  If the result of the subtraction has a different sign than
     * that of current, then return zero instead.
     *
     * @param current current value
     * @param delta amount to subtract
     * @return result of current - delta.  If the result of the subtraction has a different sign
     *          than that of current, then return zero instead.
     */
    private double subtractUpToZero(double current, double delta) {
        double result = current - delta;
        return signOf(result) == signOf(current) ? result : 0d;
    }

    /**
     * Add an action delta (price change) to the active action list.
     *
     * @param delta amount of the price change.
     * @param expirationTimestamp time when the action will expire.
     */
    public void addAction(double delta, long expirationTimestamp) {
        actionList.add(delta);
        expirationList.add(expirationTimestamp);
        applyDelta(delta);
    }

    /**
     * Remove an action delta (price change) from the active action list. This must be the first
     * action in the action list.  If the timestamp does match, the removal will be ignored.
     *
     * @param expirationTimestamp time when the action will expire.
     * @return true if the action was removed.
     */
    public boolean removeAction(long expirationTimestamp) {
        if (expirationList.peek() == expirationTimestamp) {
            actionList.poll();
            expirationList.poll();
            recalculateSavings();
            return true;
        }
        return false;
    }

    /**
     * Apply a price change to the current savings and investment values.  The result cannot pass
     * through zero.  If that happens, the result is clipped to zero.  A positive delta represents
     * an investment and a negative delta represents savings.
     *
     * @param delta amount to apply.
     */
    private void applyDelta(double delta) {
        if (delta < 0) {
            this.savings += -delta;
            this.investment = subtractUpToZero(this.investment, -delta);
        } else {
            this.investment += delta;
            this.savings = subtractUpToZero(this.savings, delta);
        }
    }

    /**
     * Return the savings amount associated with a delta.  If the delta > 0, then delta
     * represents an investment, and zero is returned.
     *
     * @param delta value to convert.
     * @return savings amount if delta < 0, else 0.
     */
    private static double deltaToSavings(double delta) {
        return -Math.min(0d, delta);
    }

    /**
     * Return the investment amount associated with a delta.  If the delta < 0, then delta
     * represents savings, and zero is returned.
     *
     * @param delta value to convert.
     * @return investment amount if delta > 0, else 0.
     */
    private static double deltaToInvestment(double delta) {
        return Math.max(0d, delta);
    }

    /**
     * Close out the current segment in preparation for a transition to a new one.
     *
     * @param timestamp time that the segment ends.
     */
    public void endSegment(long timestamp) {
        long segmentLength = (timestamp - segmentStart) * powerFactor;
        segmentStart = timestamp;

        SavingsInvestments result = new SavingsInvestments(savings, investment);
        periodicRealized.savings += result.getSavings() * segmentLength;
        periodicRealized.investments += result.getInvestments() * segmentLength;
        // If there's an active recommendation, accumulate missed savings/investments.
        if (currentRecommendation != null) {
            double delta = currentRecommendation.getDelta() * segmentLength;
            periodicMissed.savings += deltaToSavings(delta);
            periodicMissed.investments += deltaToInvestment(delta);
        }
    }

    /**
     * Close out the current interval.  This resets periodic values and prepares for the next interval.
     *
     * @param periodStartTime time that the period started.
     * @param periodEndTime time that the period ended.
     */
    public void endPeriod(long periodStartTime, long periodEndTime) {
        // Close out the final segment of the period.
        endSegment(periodEndTime);
    }

    /**
     * Set the current power factor.
     *
     * @param powerFactor To set the power status to on, use 1.  To set the power status to off,
     * use 0.  Any other value will yield interesting (but not necessarily incorrect) results.
     */
    @Override
    public void setPowerFactor(long powerFactor) {
        this.powerFactor = powerFactor;
    }

    /**
     * Return the current power factor.  This factor is multiplied by the current savings and
     * investment to get the amount that is accumulated for any given segment.
     *
     * @return the current power factor.
     */
    @Override
    public long getPowerFactor() {
        return powerFactor;
    }

    /**
     * Set the current recommendation. Only the related price change is needed.
     *
     * @param recommendation recommendation to save
     */
    @Override
    public void setCurrentRecommendation(EntityPriceChange recommendation) {
        this.currentRecommendation = recommendation;
    }

    /**
     * Get the current active recommendation.
     *
     * @return the current active recommendation, or null if there is none.
     */
    @Override
    public EntityPriceChange getCurrentRecommendation() {
        return this.currentRecommendation;
    }

    /**
     * Return the entity ID associated with this algorithm state.
     *
     * @return the entity ID
     */
    @Override
    public Long getEntityOid() {
        return this.entityOid;
    }

    /**
     * Return the realized savings and investments for this period.
     *
     * @return realized SavingsInvestments for the period.
     */
    @Override
    public SavingsInvestments getRealized() {
        return periodicRealized;
    }

    /**
     * Return the missed savings and investments for this period.
     *
     * @return missed SavingsInvestments for the period.
     */
    @Override
    public SavingsInvestments getMissed() {
        return periodicMissed;
    }

    /**
     * Mark/unmark the associated entity for removal.
     *
     * @param deletePending true to mark the entity for removal after the current stats period,
     * else false.
     */
    @Override
    public void setDeletePending(boolean deletePending) {
        this.deletePending = deletePending;
    }

    /**
     * Return the deletion pending status.
     *
     * @return true if the associated entity state should be deleted after the current stats period.
     */
    @Override
    public boolean getDeletePending() {
        return this.deletePending;
    }

    /**
     * Read entity state that is needed to run the algorithm into local state.
     *
     * @param timestamp the time that the algorithm is invoked
     * @param entityState entity state being tracked
     */
    public void initState(long timestamp, EntityState entityState) {
        // Backward compatibility.  Older entity states do not contain an expiration list.  In this
        // case, all existing entities will immediately expire.
        List<Long> currentExpirationList = entityState.getExpirationList();
        List<Double> currentActionList = entityState.getActionList();
        if (currentExpirationList == null) {
            // Old entity state.  Create a new expiration list that expires all existing actions.
            currentExpirationList = new ArrayList<>();
            currentActionList.clear();
        }
        currentRecommendation = entityState.getCurrentRecommendation();
        powerFactor = entityState.getPowerFactor();

        // Populate the action and expiration lists.  We use two iterators in order to
        // handle any difference in the list sizes (should never happen).
        Iterator<Long> expirations = currentExpirationList.iterator();
        Iterator<Double> deltas = currentActionList.iterator();
        while (expirations.hasNext() && deltas.hasNext()) {
            actionList.offer(deltas.next());
            expirationList.offer(expirations.next());
        }

        // Recalculate savings based on the current action and expiration lists.
        recalculateSavings();
    }

    /**
     * Recalculate the current savings and investment based on the action list.
     */
    private void recalculateSavings() {
        savings = 0d;
        investment = 0d;
        actionList.stream().forEach(this::applyDelta);
    }

    /**
     * Return the action/delta list.
     *
     * @return the action list
     */
    public List<Double> getActionList() {
        return new ArrayList<>(this.actionList);
    }

    /**
     * Return the expiration times list.
     *
     * @return the expiration times list
     */
    public List<Long> getExpirationList() {
        return new ArrayList<>(this.expirationList);
    }

    /**
     * Get the next action expiration time.
     *
     * @return the expiration time of the next action, if present.  If there are no active actions,
     *          the expiration time of 1,000 years from now will be returned instead, which should
     *          be long enough to consider the action permanent.
     */
    public long getNextExpirationTime() {
        // Since the events are sorted in chronological order and all actions have the same action
        // duration, the timestamps in the expiration list are also sorted chronologically, and the
        // first timestamp in the list is the next one to expire.
        return expirationList.isEmpty()
                ? LocalDateTime.now().plusYears(1000L).toInstant(ZoneOffset.UTC).toEpochMilli()
                : expirationList.peek();
    }

    /**
     * Clear the action list and related expiration list.
     */
    public void clearActionList() {
        this.actionList.clear();
        this.expirationList.clear();
    }
}
