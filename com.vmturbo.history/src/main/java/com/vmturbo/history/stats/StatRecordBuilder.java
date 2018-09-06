package com.vmturbo.history.stats;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.history.schema.StringConstants;
import com.vmturbo.history.stats.live.LiveStatsReader;

/**
 * A helper class to create {@link StatRecord} objects out of nullable fields.
 * The default protobuf builders will throw exceptions if passed null values.
 */
public interface StatRecordBuilder {

    /**
     * Create a {@link StatRecord} protobuf to contain aggregate stats values.
     *
     * @param propertyType name for this stat, e.g. VMem
     * @param propertySubtype refinement for this stat, e.g. "used" vs "utilization"
     * @param capacityStat The capacity stat.
     * @param reserved the amount of capacity that is "reserved" and unavailable for allocation.
     * @param producerId unique id of the producer for commodity bought
     * @param avgValue average value reported from discovery
     * @param minValue min value reported from discovery
     * @param maxValue max value reported from discovery
     * @param commodityKey unique key to associate commodities between seller and buyer
     * @param totalValue total of value (avgValue) over all elements of a group
     * @param relation stat relation to entity, e.g., "CommoditiesBought"
     * @return a {@link StatRecord} protobuf populated with the given values
     */
    @Nonnull
    StatRecord buildStatRecord(@Nonnull final String propertyType,
                               @Nullable final String propertySubtype,
                               @Nullable final StatValue capacityStat,
                               @Nullable final Float reserved,
                               @Nullable final Long producerId,
                               @Nullable final Float avgValue,
                               @Nullable final Float minValue,
                               @Nullable final Float maxValue,
                               @Nullable final String commodityKey,
                               @Nullable final Float totalValue,
                               @Nullable final String relation);

    /**
     * Create a {@link StatRecord} protobuf to contain aggregate stats values.
     *
     * @param propertyType name for this stat, e.g. VMem
     * @param propertySubtype refinement for this stat, e.g. "used" vs "utilization"
     * @param capacity available amount on the producer
     * @param reserved the (optional) amount of capacity that is unavailable for allocation
     * @param producerId unique id of the producer for commodity bought
     * @param avgValue average value reported from discovery
     * @param minValue min value reported from discovery
     * @param maxValue max value reported from discovery
     * @param commodityKey unique key to associate commodities between seller and buyer
     * @param totalValue total of value (avgValue) over all elements of a group
     * @param relation stat relation to entity, e.g., "CommoditiesBought"
     * @return a {@link StatRecord} protobuf populated with the given values
     */
    @Nonnull
    default StatRecord buildStatRecord(@Nonnull String propertyType,
                                       @Nullable String propertySubtype,
                                       @Nullable Float capacity,
                                       @Nullable Float reserved,
                                       @Nullable Long producerId,
                                       @Nullable Float avgValue,
                                       @Nullable Float minValue,
                                       @Nullable Float maxValue,
                                       @Nullable String commodityKey,
                                       @Nullable Float totalValue,
                                       @Nullable String relation) {
        return buildStatRecord(propertyType,
                propertySubtype,
                capacity == null ? null : StatsAccumulator.singleStatValue(capacity),
                reserved,
                producerId,
                avgValue,
                minValue,
                maxValue,
                commodityKey,
                totalValue,
                relation);
    }

    /**
     * The default implementation of {@link StatRecordBuilder}, for production use.
     */
    class DefaultStatRecordBuilder implements StatRecordBuilder {

        private final LiveStatsReader liveStatsReader;

        DefaultStatRecordBuilder(@Nonnull final LiveStatsReader liveStatsReader) {
            this.liveStatsReader = Objects.requireNonNull(liveStatsReader);
        }

        @Nonnull
        @Override
        public StatRecord buildStatRecord(@Nonnull final String propertyType,
                                          @Nullable final String propertySubtype,
                                          @Nullable final StatValue capacityStat,
                                          @Nullable final Float reserved,
                                          @Nullable final Long producerId,
                                          @Nullable final Float avgValue,
                                          @Nullable final Float minValue,
                                          @Nullable final Float maxValue,
                                          @Nullable final String commodityKey,
                                          @Nullable final Float totalValue,
                                          @Nullable final String relation) {
            final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder()
                    .setName(propertyType);

            if (capacityStat != null) {
                statRecordBuilder.setCapacity(capacityStat);
            }

            if (relation != null) {
                statRecordBuilder.setRelation(relation);
            }

            if (reserved != null) {
                statRecordBuilder.setReserved(reserved);
            }

            if (commodityKey != null) {
                statRecordBuilder.setStatKey(commodityKey);
            }
            if (producerId != null) {
                // providerUuid
                statRecordBuilder.setProviderUuid(Long.toString(producerId));
                // providerDisplayName
                final String producerDisplayName = liveStatsReader.getEntityDisplayNameForId(producerId);
                if (producerDisplayName != null) {
                    statRecordBuilder.setProviderDisplayName(producerDisplayName);
                }
            }

            // units
            CommodityTypeUnits commodityType = CommodityTypeUnits.fromString(propertyType);
            if (commodityType != null) {
                statRecordBuilder.setUnits(commodityType.getUnits());
            }

            // values, used, peak
            StatValue.Builder statValueBuilder = StatValue.newBuilder();
            if (avgValue != null) {
                statValueBuilder.setAvg(avgValue);
            }
            if (minValue != null) {
                statValueBuilder.setMin(minValue);
            }
            if (maxValue != null) {
                statValueBuilder.setMax(maxValue);
            }
            if (totalValue != null) {
                statValueBuilder.setTotal(totalValue);
            }

            // currentValue
            if (avgValue != null && (propertySubtype == null ||
                    StringConstants.PROPERTY_SUBTYPE_USED.equals(propertySubtype))) {
                statRecordBuilder.setCurrentValue(avgValue);
            } else {
                if (maxValue != null) {
                    statRecordBuilder.setCurrentValue(maxValue);
                }
            }

            StatValue statValue = statValueBuilder.build();

            statRecordBuilder.setValues(statValue);
            statRecordBuilder.setUsed(statValue);
            statRecordBuilder.setPeak(statValue);

            return statRecordBuilder.build();
        }
    }
}
