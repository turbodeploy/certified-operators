package com.vmturbo.history.stats.projected;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.history.stats.HistoryUtilizationType;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.history.schema.RelationType;

/**
 * Accumulated information about a single type of commodity over a set of entities.
 * For concrete implementations see {@link AccumulatedBoughtCommodity} and
 * {@link AccumulatedSoldCommodity}.
 */
abstract class AccumulatedCommodity {
    private StatsAccumulator used = new StatsAccumulator();

    private StatsAccumulator capacity = new StatsAccumulator();

    private StatsAccumulator percentileUsage = new StatsAccumulator();

    private final String commodityName;

    private boolean empty = true;

    protected AccumulatedCommodity(@Nonnull final String commodityName) {
        this.commodityName = commodityName;
    }

    @Nonnull
    Optional<StatRecord> toStatRecord() {
        if (empty) {
            return Optional.empty();
        }

        // Usage stats for values, used, peak
        final StatValue statValue = used.toStatValue();

        final StatRecord.Builder builder = StatRecord.newBuilder();
        builder.setName(commodityName);
        final StatValue capacityStatValue = capacity.toStatValue();
        builder.setCapacity(capacityStatValue);
        builder.setUsed(statValue);
        builder.setValues(statValue);
        builder.setPeak(statValue);
        builder.setCurrentValue(statValue.getAvg());
        if (percentileUsage.getCount() > 0) {
            builder.addHistUtilizationValue(HistUtilizationValue.newBuilder()
                            .setType(HistoryUtilizationType.Percentile.getApiParameterName())
                            .setUsage(percentileUsage.toStatValue()).setCapacity(capacityStatValue)
                            .build());
        }

        final CommodityTypeUnits commodityType = CommodityTypeUnits.fromString(commodityName);
        if (commodityType != null) {
            builder.setUnits(commodityType.getUnits());
        }

        return Optional.of(finalizeStatRecord(builder));
    }

    /**
     * Subclasses override this method to customize the {@link StatRecord} with
     * subclass-specific information.
     *
     * @param builder The builder for the {@link StatRecord}, with all the generic values
     *                (e.g. name, used, etc.) set.
     * @return The completed {@link StatRecord}.
     */
    protected abstract StatRecord finalizeStatRecord(StatRecord.Builder builder);

    protected void recordUsed(final double used, final double peak) {
        empty = false;
        this.used.record(used, peak);
    }

    protected void recordCapacity(final double capacity) {
        empty = false;
        this.capacity.record(capacity);
    }

    protected void recordPercentile(final double percentileUtilization, double capacity) {
        empty = false;
        this.percentileUsage.record(percentileUtilization * capacity);
    }

    /**
     * Accumulated information about a single bought commodity across some number of
     * entities in the topology.
     */
    static class AccumulatedBoughtCommodity extends AccumulatedCommodity {

        /**
         * The providers that sold this commodity, and the value could be null.
         */
        private Set<Long> providers = new HashSet<>();

        AccumulatedBoughtCommodity(@Nonnull final String commodityName) {
            super(commodityName);
        }

        /**
         * Record a commodity bought by some entity in the topology. The ID of the buyer
         * doesn't matter, and it's up to the caller to ensure there are no repeats in the
         * input. And provider id could be null when commodity bought without any provider id such as
         * unplaced entities.
         *
         * @param commodityBoughtDTO The DTO describing the bought commodity.
         * @param providerId The ID of the provider selling this commodity.
         * @param capacity The provider's capacity of this commodity.
         */
        void recordBoughtCommodity(@Nonnull final CommodityBoughtDTO commodityBoughtDTO,
                                   @Nullable final Long providerId,
                                   final double capacity) {
            recordUsed(commodityBoughtDTO.getUsed(), commodityBoughtDTO.getPeak());
            recordCapacity(capacity);
            if (commodityBoughtDTO.hasHistoricalUsed() && Double.compare(capacity, 0) > 0) {
                final HistoricalValues historicalUsed = commodityBoughtDTO.getHistoricalUsed();
                if (historicalUsed.hasPercentile()) {
                    recordPercentile(historicalUsed.getPercentile(), capacity);
                }
            }
            this.providers.add(providerId);
        }

        @Nonnull
        @Override
        protected StatRecord finalizeStatRecord(@Nonnull final StatRecord.Builder builder) {

            builder.setRelation(RelationType.COMMODITIESBOUGHT.getLiteral());

            // For now, only set the provider UUID if there is exactly one provider and it is not null.
            if (providers.size() == 1 && providers.iterator().next() != null) {
                builder.setProviderUuid(Long.toString(providers.iterator().next()));
            }

            return builder.build();
        }
    }

    /**
     * Information about a single sold commodity across some number of entities in the topology.
     */
    static class AccumulatedSoldCommodity extends AccumulatedCommodity {

        AccumulatedSoldCommodity(@Nonnull final String commodityName) {
            super(commodityName);
        }

        /**
         * Record a commodity sold by some entity in the topology. The ID of the seller doesn't
         * matter for the purposes of accumulation, and it's up to the caller to ensure there are
         * no undesireable repeats in the input.
         *
         * @param commoditySoldDTO The DTO describing the sold commodity.
         */
        void recordSoldCommodity(@Nonnull final CommoditySoldDTO commoditySoldDTO) {
            recordUsed(commoditySoldDTO.getUsed(), commoditySoldDTO.getPeak());
            final double capacity = commoditySoldDTO.getCapacity();
            recordCapacity(capacity);
            if (commoditySoldDTO.hasHistoricalUsed()) {
                final HistoricalValues historicalUsed = commoditySoldDTO.getHistoricalUsed();
                if (historicalUsed.hasPercentile()) {
                    recordPercentile(historicalUsed.getPercentile(), capacity);
                }
            }
        }

        @Nonnull
        @Override
        protected StatRecord finalizeStatRecord(@Nonnull final StatRecord.Builder builder) {
            builder.setRelation(RelationType.COMMODITIES.getLiteral());
            return builder.build();
        }
    }

    /**
     * Information about a single calculated commodity across some number of
     * entities in the topology.
     */
    static class AccumulatedCalculatedCommodity extends AccumulatedCommodity {

        AccumulatedCalculatedCommodity(@Nonnull final String commodityName) {
            super(commodityName);
        }

        /**
         * Record a commodity calculated based on attributes of some entity in the topology.
         *
         * @param value The value for this attribute commodity
         */
        void recordAttributeCommodity(final double value) {
            recordUsed(value, value);
            recordCapacity(value);
        }

        @Nonnull
        @Override
        protected StatRecord finalizeStatRecord(@Nonnull final StatRecord.Builder builder) {
            builder.setRelation(RelationType.METRICS.getLiteral());
            return builder.build();
        }
    }
}
