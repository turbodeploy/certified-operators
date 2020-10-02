package com.vmturbo.history.stats;

import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.history.stats.snapshots.ProducerIdVisitor.ProviderInformation;

/**
 * A helper class to create {@link StatRecord} objects out of nullable fields. The default protobuf
 * builders will throw exceptions if passed null values.
 */
public interface StatRecordBuilder {
    /**
     * Create a {@link StatRecord} protobuf to contain aggregate stats values.
     *
     * @param propertyType name for this stat, e.g. VMem
     * @param propertySubtype refinement for this stat, e.g. "used" vs
     *                 "utilization"
     * @param capacityStat The capacity stat.
     * @param reserved the amount of capacity that is "reserved" and unavailable for
     *                 allocation.
     * @param relatedEntityType the type of the entity related to (buying or
     *                 selling) this commodity
     * @param producerId unique id of the producer for commodity bought
     * @param statValue value statistics
     * @param commodityKey unique key to associate commodities between seller and
     *                 buyer
     * @param relation stat relation to entity, e.g., "CommoditiesBought"
     * @return a {@link StatRecord} protobuf populated with the given values
     */
    @Nonnull
    StatRecord buildStatRecord(@Nonnull String propertyType, @Nullable String propertySubtype,
                    @Nullable StatValue capacityStat, @Nullable Float reserved,
                    @Nullable String relatedEntityType, @Nullable Long producerId,
                    @Nullable StatValue statValue, @Nullable String commodityKey,
                    @Nullable String relation);

    /**
     * Create a {@link StatRecord} protobuf to contain aggregate stats values.
     *
     * @param propertyType name for this stat, e.g. VMem
     * @param propertySubtype refinement for this stat, e.g. "used" vs
     *                 "utilization"
     * @param capacity available amount on the producer
     * @param reserved the (optional) amount of capacity that is unavailable for
     *                 allocation
     * @param relatedEntityType the (optional) entity type this commodity is
     *                 associated with
     * @param producerId unique id of the producer for commodity bought
     * @param statValue value statistics
     * @param commodityKey unique key to associate commodities between seller and
     *                 buyer
     * @param relation stat relation to entity, e.g., "CommoditiesBought"
     * @return a {@link StatRecord} protobuf populated with the given values
     */
    @Nonnull
    default StatRecord buildStatRecord(@Nonnull String propertyType,
                    @Nullable String propertySubtype, @Nullable Float capacity,
                    @Nullable Float reserved, @Nullable String relatedEntityType,
                    @Nullable Long producerId, @Nullable StatValue statValue,
                    @Nullable String commodityKey, @Nullable String relation) {
        return buildStatRecord(propertyType, propertySubtype,
                        capacity == null ? null : StatsAccumulator.singleStatValue(capacity),
                        reserved, relatedEntityType, producerId, statValue, commodityKey, relation);
    }

    /**
     * The default implementation of {@link StatRecordBuilder}, for production use.
     */
    class DefaultStatRecordBuilder implements StatRecordBuilder {
        private final BiConsumer<StatRecord.Builder, String> relatedEntityTypePopulator;
        private final BiConsumer<StatRecord.Builder, String> relationPopulator;
        private final BiConsumer<StatRecord.Builder, Pair<StatValue, String>> usageRecordPopulator;
        private final BiConsumer<StatRecord.Builder, Pair<StatValue, Float>> capacityPopulator;
        private final BiConsumer<StatRecord.Builder, Pair<String, String>> propertyTypePopulator;
        private final BiConsumer<StatRecord.Builder, ProviderInformation> producerIdPopulator;

        public DefaultStatRecordBuilder(
                        @Nonnull BiConsumer<Builder, String> relatedEntityTypePopulator,
                        @Nonnull BiConsumer<Builder, String> relationPopulator,
                        @Nonnull BiConsumer<Builder, Pair<StatValue, String>> usageRecordPopulator,
                        @Nonnull BiConsumer<Builder, Pair<StatValue, Float>> capacityPopulator,
                        @Nonnull BiConsumer<Builder, Pair<String, String>> propertyTypePopulator,
                        @Nonnull BiConsumer<Builder, ProviderInformation> producerIdPopulator) {
            this.relatedEntityTypePopulator = relatedEntityTypePopulator;
            this.relationPopulator = relationPopulator;
            this.usageRecordPopulator = usageRecordPopulator;
            this.capacityPopulator = capacityPopulator;
            this.propertyTypePopulator = propertyTypePopulator;
            this.producerIdPopulator = producerIdPopulator;
        }

        @Nonnull
        @Override
        public StatRecord buildStatRecord(@Nonnull final String propertyType,
                        @Nullable final String propertySubtype,
                        @Nullable final StatValue capacityStat, @Nullable final Float reserved,
                        @Nullable final String relatedEntityType, @Nullable final Long producerId,
                        @Nullable final StatValue statValue, @Nullable final String commodityKey,
                        @Nullable final String relation) {
            final StatRecord.Builder builder = StatRecord.newBuilder();
            propertyTypePopulator.accept(builder, new Pair<>(propertyType, commodityKey));
            usageRecordPopulator.accept(builder, new Pair<>(statValue, propertySubtype));
            relationPopulator.accept(builder, relation);
            relatedEntityTypePopulator.accept(builder, relatedEntityType);
            capacityPopulator.accept(builder, new Pair<>(capacityStat, reserved));
            producerIdPopulator.accept(builder, new ProviderInformation(producerId));
            return builder.build();
        }
    }
}
