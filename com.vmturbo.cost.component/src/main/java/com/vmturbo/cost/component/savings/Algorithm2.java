package com.vmturbo.cost.component.savings;

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

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
    private boolean compress;
    private EntityPriceChange currentRecommendation;


    // Internal state maintained by the algorithm
    private List<Double> actionList;
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
        this.compress = false;
        this.deletePending = false;
        this.segmentStart = segmentStart;
        this.powerFactor = 1;
        this.actionList = new ArrayList<>();

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
     * Add an action delta (price change) to the active action list.
     *
     * @param delta amount of the price change.
     */
    public void addAction(double delta) {
        if (actionList.isEmpty()) {
            actionList.add(delta);
            return;
        }
        int index = actionList.size() - 1;
        double current = actionList.get(index);
        if (signOf(current) == signOf(delta)) {
            // New delta is in the same direction as the lasts, so merge them.
            current += delta;
            actionList.set(index, current);
            // Back up to the last reverse-direction action and continue
            // on to this action out of previous reverse-direction actions.
            index -= 1;
        } else {
            // Direction change. Add this action to the list and continue on to back it out of
            // previous reverse - direction actions.
            actionList.add(delta);
        }

        // Back this delta out of the previous reverse-direction actions
        while (index >= 0) {
            current = actionList.get(index);
            double newCurrent = current + delta;
            if (signOf(newCurrent) != signOf(delta)) {
                // We didn't completely back out the current action, so we're done.
                actionList.set(index, newCurrent);
                break;
            } else {
                // We completely backed out the current action, so remove it
                actionList.set(index, 0d);
                compress = true;  // Remove the inactive actions before updating the entity state
                delta += current;  // add back in the excess back out amount
                index -= 2;  // Go to the next reverse-direction action
            }
        }
    }

    /**
     * Compress a list of deltas by removing all zero values, which represent inactive actions.
     * If removing a zero from the list results in consecutive values with the same sign, collapse
     * those values by replacing them with the sum of the consecutive values.
     *
     * @param deltaList list of deltas to collapse
     * @return new list containing the collapsed values in deltaList
     */
    @VisibleForTesting
     static List<Double> compressList(List<Double> deltaList) {
        List<Double> result = new ArrayList<>();
        Double acc = null;
        int currentSign = 0;
        for (Double delta : deltaList) {
            if (delta == 0d) {
                // Filter out inactive action
                continue;
            }
            if (acc == null) {
                // First active entry
                acc = delta;
                currentSign = signOf(acc);
                continue;
            }
            if (currentSign != signOf(delta)) {
                // Changing sign, so emit previous value
                result.add(acc);
                acc = delta;
                currentSign = signOf(acc);
            } else {
                // Same sign, so merge with previous
                acc += delta;
            }
        }
        // Emit the last value if present
        if (acc != null) {
            result.add(acc);
        }
        return result;
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

        SavingsInvestments result = getCurrentValues();
        periodicRealized.savings += result.getSavings() * segmentLength;
        periodicRealized.investments += result.getInvestments() * segmentLength;
        // If there's an active recommendation, accumulate missed savings/investments.
        if (currentRecommendation != null) {
            double delta = currentRecommendation.getDelta() * segmentLength;
            periodicMissed.savings += deltaToSavings(delta);
            periodicMissed.investments += deltaToInvestment(delta);
        }
    }

    private SavingsInvestments getCurrentValues() {
        double savings = 0d;
        double investment = 0d;
        for (Double delta : actionList) {
            savings += deltaToSavings(delta);
            investment += deltaToInvestment(delta);
        }
        return new SavingsInvestments(savings, investment);
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
        // Collapse the action list if there are any inactive actions in it.
        if (compress) {
            actionList = compressList(actionList);
        }
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
     * @param entityState entity state being tracked
     */
    public void initState(EntityState entityState) {
        this.actionList = entityState.getActionList();
        this.currentRecommendation = entityState.getCurrentRecommendation();
        this.powerFactor = entityState.getPowerFactor();
    }

    /**
     * Return the action/delta list.
     *
     * @return the action list
     */
    public List<Double> getActionList() {
        return this.actionList;
    }
}
