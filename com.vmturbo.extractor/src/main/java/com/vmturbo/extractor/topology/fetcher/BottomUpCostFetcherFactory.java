package com.vmturbo.extractor.topology.fetcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;

/**
 * Factory class for the {@link BottomUpCostFetcher}.
 */
public class BottomUpCostFetcherFactory {

    private static final Cost.CostCategory[] COST_CATEGORY_VALUES = Cost.CostCategory.values();
    private static final Float NEG_ZERO = -0.0f;

    private final CostServiceBlockingStub costService;

    /**
     * Constructor.
     *
     * @param costService To access the account expenses RPC.
     */
    public BottomUpCostFetcherFactory(@Nonnull final CostServiceBlockingStub costService) {
        this.costService = costService;
    }

    /**
     * Create a new fetcher for a particular topology. The fetcher itself just passes through to the
     * factory, in order to utilize the cached {@link BottomUpCostData}, updating it if necessary.
     *
     * @param timer        Timer. See {@link DataFetcher}
     * @param snapshotTime snapshot time of topology whose data is to be fetched
     * @param consumer     Consumer for the resulting {@link BottomUpCostData}. See {@link
     *                     DataFetcher}.
     * @return The {@link BottomUpCostFetcher}.
     */
    @Nonnull
    public BottomUpCostFetcher newFetcher(MultiStageTimer timer, final long snapshotTime, Consumer<BottomUpCostData> consumer) {
        return new BottomUpCostFetcher(timer, snapshotTime, consumer, costService, this);
    }

    /**
     * Utility object to make caching and using the top-down cost data easier.
     */
    public static class BottomUpCostData {
        private final Long2ObjectMap<List<StatRecord>> entityCosts = new Long2ObjectOpenHashMap<>();
        private final long snapshotTime;

        /**
         * Create a new instance.
         * @param snapshotTime snapshot time of topology for which cost data is needed
         */
        public BottomUpCostData(long snapshotTime) {
            this.snapshotTime = snapshotTime;
        }

        void addEntityCost(final StatRecord statRecord) {
            final long oid = statRecord.getAssociatedEntityId();
            entityCosts.computeIfAbsent(oid, _oid -> new ArrayList<>()).add(statRecord);
        }

        public long getSnapshotTime() {
            return snapshotTime;
        }

        public LongStream getEntityOids() {
            return entityCosts.keySet().stream().mapToLong(Long::longValue);
        }

        /**
         * Get the costs associated with a particular entity.
         *
         * @param oid The id of the entity.
         * @return list of {@link StatRecord} or an empty optional if there are no costs for the
         * entity.
         */
        public Optional<List<StatRecord>> getEntityCosts(final long oid) {
            return Optional.ofNullable(entityCosts.get(oid));
        }

        /**
         * Get the number of entities for which we have cost data.
         *
         * @return # of entities
         */
        public int size() {
            return entityCosts.size();
        }

        /**
         * Check if the cost data is empty.
         *
         * @return true if empty
         */
        public boolean isEmpty() {
            return size() == 0;
        }

        /**
         * Collect all the category/source/cost combinations that should be recorded for the given
         * entity. This includes per-category totals across all sources, as well as a per-entity
         * total across all categories and sources.
         *
         * @param oid entity oid
         * @return list of category/source/cost combinations to be recorded
         */
        public List<BottomUpCostDataPoint> getEntityCostDataPoints(final long oid) {
            List<BottomUpCostDataPoint> results = new ArrayList<>();

            // hack alert: all cat totals are initialized to -0.0, which is indistinguishable from 0.0
            // as a `float` but not as a `Float`. In the boxed case, -0.0 is considered < 0.0,
            // but adding 0.0 to -0.0 yields 0.0. So by initializing to -0.0 we are able to see
            // whether each value was actually altered (and therefore should be recorded)
            // while processing this entity. It's an optimization over keeping track of which
            // categories were seen in some other way.
            final float[] categoryTotals = new float[COST_CATEGORY_VALUES.length];
            Arrays.fill(categoryTotals, -0.0f);

            float total = 0.0f;
            for (final StatRecord cost : getEntityCosts(oid).orElse(Collections.emptyList())) {
                final float costValue = cost.getValues().getAvg();
                final CostCategory category = CostCategory.valueOf(cost.getCategory().name());
                final CostSource source = CostSource.valueOf(cost.getCostSource().name());
                results.add(new BottomUpCostDataPoint(category, source, costValue));
                categoryTotals[cost.getCategory().ordinal()] += costValue;
                total += costValue;
            }
            for (int i = 0; i < categoryTotals.length; i++) {
                if (!Float.valueOf(categoryTotals[i]).equals(NEG_ZERO)) {
                    final CostCategory category = CostCategory.valueOf(COST_CATEGORY_VALUES[i].name());
                    results.add(new BottomUpCostDataPoint(category, CostSource.TOTAL, categoryTotals[i]));
                }
            }
            if (!results.isEmpty()) {
                results.add(new BottomUpCostDataPoint(CostCategory.TOTAL, CostSource.TOTAL, total));
            }
            return results;
        }

        /**
         * Class to represent a category/source/cost combination that should be recorded for a given
         * entity oid.
         */
        public static class BottomUpCostDataPoint {

            private final CostCategory category;
            private final CostSource source;
            private final float cost;

            /**
             * Create a new instance.
             *  @param category cost category
             * @param source   cost source
             * @param cost     cost value
             */
            public BottomUpCostDataPoint(CostCategory category, CostSource source, float cost) {
                this.category = category;
                this.source = source;
                this.cost = cost;
            }

            public CostCategory getCategory() {
                return category;
            }

            public CostSource getSource() {
                return source;
            }

            public float getCost() {
                return cost;
            }
        }
    }
}
