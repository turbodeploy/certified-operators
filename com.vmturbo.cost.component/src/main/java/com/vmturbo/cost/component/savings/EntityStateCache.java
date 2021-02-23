package com.vmturbo.cost.component.savings;

import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Cache interface for keeping entity savings state.
 */
interface EntityStateCache {
    /**
     * Checks if state is present for the given entity.
     *
     * @param entityOid OID of entity to check.
     * @return Whether state exists.
     */
    boolean containsEntityState(long entityOid);

    /**
     * Sets state of entity.
     *
     * @param state EntityState.
     */
    void setEntityState(@Nonnull EntityState state);

    /**
     * Gets state of entity given the oid.
     *
     * @param entityOid Oid of entity to get state for.
     * @return Instance of EntityState, can be null if not found.
     */
    @Nullable
    EntityState getEntityState(long entityOid);

    /**
     * Gets state of entity given the oid.
     *
     * @param entityOid Oid of entity to get state for.
     * @param segmentStart time that the segment was opened.
     * @param createIfNotFound if true, create the entity state if it doesn't already exist.
     * @return Instance of EntityState, can be null if not found.
     */
    @Nullable
    EntityState getEntityState(long entityOid, long segmentStart, boolean createIfNotFound);

        /**
         * Removes state of entity.
         *
         * @param entityOid Oid of entity.
         * @return Old state if successfully removed, or null if not present.
         */
    @Nullable
    EntityState removeEntityState(long entityOid);

    /**
     * Remove all inactive entity states.
     */
    void removeInactiveState();

    /**
     * Current size of state map.
     *
     * @return Size.
     */
    int size();

    /**
     * Gets all entities.
     *
     * @return Stream of all existing entities.
     */
    @Nonnull
    Stream<EntityState> getAll();

    /**
     * Group of savings and investments together.
     */
    class SavingsInvestments {
        double savings;
        double investments;

        SavingsInvestments() {
        }

        SavingsInvestments(SavingsInvestments other) {
            this.investments = other.investments;
            this.savings = other.savings;
        }

        public boolean hasSavings() {
            return savings != 0d;
        }

        public boolean hasInvestments() {
            return investments != 0d;
        }

        public double getSavings() {
            return savings;
        }

        public void setSavings(double savings) {
            this.savings = savings;
        }

        public double getInvestments() {
            return investments;
        }

        public void setInvestments(double investments) {
            this.investments = investments;
        }

        @Override
        public String toString() {
            return String.format("Savings = %f, Investments = %f", savings, investments);
        }
    }

    /**
     * Represents state of entity.
     */
    class EntityState {
        /**
         * Logger.
         */
        private static final Logger logger = LogManager.getLogger();

        /**
         * State of entity. Entities that are removed temporarily enter the inactive state.  They
         * are then removed during the next calculation pass.
         */
        private boolean isActive;

        /**
         * OID of entity.
         */
        private final long entityId;

        /**
         * Timestamp of beginning of segment (when a transition e.g resize or power). There can be
         * multiple segments within a time period (e.g 1 hour). We close a segment after processing,
         * and then start a new segment based on the new event in the time period.
         */
        private long segmentStart;

        /**
         * What we multiple current cost with - usually a 1 or a 0.
         */
        private long powerFactor;

        /**
         * Contains current cost info in the recommendation active for an entity.
         */
        private EntityPriceChange currentRecommendation;

        // Internal state maintained by the algorithm
        private SavingsInvestments currentRealized;
        private SavingsInvestments currentMissed;
        private SavingsInvestments periodicRealized;
        private SavingsInvestments periodicMissed;

        // External snapshot for use by the savings tracker
        private SavingsInvestments externalRealized;
        private SavingsInvestments externalMissed;

        EntityState(long entityId) {
            this.entityId = entityId;
            this.isActive = true;
            this.powerFactor = 1L;

            // The current realized and missed values. Not scaled by period length.
            this.currentRealized = new SavingsInvestments();
            this.currentMissed = new SavingsInvestments();

            // The current realized and missed values accumulated this period.  Not scaled by
            // period length.
            this.periodicRealized = new SavingsInvestments();
            this.periodicMissed = new SavingsInvestments();

            // Snapshot values used for savings record generation
            this.externalRealized = new SavingsInvestments();
            this.externalMissed = new SavingsInvestments();
            this.setSegmentStart(segmentStart);
        }

        public long getEntityId() {
            return entityId;
        }

        public long getSegmentStart() {
            return segmentStart;
        }

        public void setSegmentStart(long segmentStart) {
            this.segmentStart = segmentStart;
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

        @Nullable
        public SavingsInvestments getRealized() {
            return externalRealized;
        }

        public void setRealized(@Nonnull SavingsInvestments realized, long periodLength) {
            this.externalRealized.savings = realized.savings / periodLength;
            this.externalRealized.investments = realized.investments / periodLength;
        }

        public SavingsInvestments getMissed() {
            return externalMissed;
        }

        public void setMissed(SavingsInvestments missed, long periodLength) {
            this.externalMissed.savings = missed.savings / periodLength;
            this.externalMissed.investments = missed.investments / periodLength;
        }

        public boolean isActive() {
            return this.isActive;
        }

        public void setActive(boolean active) {
            this.isActive = active;
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

        // BCTODO all methods below here need to move out of the EntityState and into a new
        //  AlgorithmState object.  This will be done as part of the Algorithm-2 implementation.

        /**
         * Close out the current segment in preparation for a transition to a new one.
         *
         * @param timestamp time that the segment ends.
         */
        public void endSegment(long timestamp) {
            long segmentLength = (timestamp - getSegmentStart())
                    * getPowerFactor();
            setSegmentStart(timestamp);
            periodicRealized.savings += currentRealized.savings * segmentLength;
            periodicRealized.investments += currentRealized.investments * segmentLength;
            periodicMissed.savings += currentMissed.savings * segmentLength;
            periodicMissed.investments += currentMissed.investments * segmentLength;
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
            long periodLength = periodEndTime - periodStartTime;
            if (periodLength <= 0) {
                logger.warn("Period start time {} is the same as or after the end time {}",
                        periodStartTime, periodEndTime);
                return;
            }

            // Snapshot the accumulated values for this period to the exposed savings/investment values.
            setRealized(periodicRealized, periodLength);
            setMissed(periodicMissed, periodLength);

            periodicRealized.savings = 0.00;
            periodicRealized.investments = 0.00;
            periodicMissed.savings = 0.00;
            periodicMissed.investments = 0.00;
        }

        /**
         * Adjust the current savings and investments by the indicated amounts.
         * @param savingsDelta amount to adjust savings
         * @param investmentDelta amount to adjust investments
         */
        public void adjustCurrent(double savingsDelta, double investmentDelta) {
            currentRealized.savings += savingsDelta;
            currentRealized.investments += investmentDelta;
        }

        /**
         * Adjust the current savings or investments by the indicated amount.  If the delta is
         * negative, then adjust the savings amount, else adjust the investment amount.
         *
         * @param delta amount to adjust by
         */
        public void adjustCurrent(double delta) {
            if (delta < 0) {
                currentRealized.savings -= delta;
            } else {
                currentRealized.investments += delta;
            }
        }

        public void adjustCurrentMissed(double savingsDelta, double investmentDelta) {
            currentMissed.savings += savingsDelta;
            currentMissed.investments += investmentDelta;
        }
    }
}
