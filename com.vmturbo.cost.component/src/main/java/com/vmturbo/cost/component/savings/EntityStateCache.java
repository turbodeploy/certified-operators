package com.vmturbo.cost.component.savings;

import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
     * Removes state of entity.
     *
     * @param entityOid Oid of entity.
     * @return Old state if successfully removed, or null if not present.
     */
    @Nullable
    EntityState removeEntityState(long entityOid);

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
        private double savings;
        private double investments;

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
    }

    /**
     * Represents state of entity.
     */
    class EntityState {
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

        private SavingsInvestments previous;
        private SavingsInvestments current;
        private SavingsInvestments realized;
        private SavingsInvestments missed;

        EntityState(long entityId) {
            this.entityId = entityId;
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
        public SavingsInvestments getPrevious() {
            return previous;
        }

        public void setPrevious(@Nonnull SavingsInvestments previous) {
            this.previous = previous;
        }

        @Nullable
        public SavingsInvestments getCurrent() {
            return current;
        }

        public void setCurrent(@Nonnull SavingsInvestments current) {
            this.current = current;
        }

        @Nullable
        public SavingsInvestments getRealized() {
            return realized;
        }

        public void setRealized(@Nonnull SavingsInvestments realized) {
            this.realized = realized;
        }

        public SavingsInvestments getMissed() {
            return missed;
        }

        public void setMissed(SavingsInvestments missed) {
            this.missed = missed;
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
}
