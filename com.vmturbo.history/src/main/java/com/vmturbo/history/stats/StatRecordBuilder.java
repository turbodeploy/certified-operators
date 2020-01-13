package com.vmturbo.history.stats;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.stats.readers.LiveStatsReader;

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
     * @param relatedEntityType the type of the entity related to (buying or selling) this commodity
     * @param producerId unique id of the producer for commodity bought
     * @param statValue value statistics
     * @param commodityKey unique key to associate commodities between seller and buyer
     * @param relation stat relation to entity, e.g., "CommoditiesBought"
     * @param percentileUtilization commodity percentile utilization
     * @return a {@link StatRecord} protobuf populated with the given values
     */
    @Nonnull
    StatRecord buildStatRecord(@Nonnull String propertyType,
                               @Nullable String propertySubtype,
                               @Nullable StatValue capacityStat,
                               @Nullable Float reserved,
                               @Nullable String relatedEntityType,
                               @Nullable Long producerId,
                               @Nullable StatValue statValue,
                               @Nullable String commodityKey,
                               @Nullable String relation,
                               @Nullable StatValue percentileUtilization);

    /**
     * Create a {@link StatRecord} protobuf to contain aggregate stats values.
     *
     * @param propertyType name for this stat, e.g. VMem
     * @param propertySubtype refinement for this stat, e.g. "used" vs "utilization"
     * @param capacity available amount on the producer
     * @param reserved the (optional) amount of capacity that is unavailable for allocation
     * @param relatedEntityType the (optional) entity type this commodity is associated with
     * @param producerId unique id of the producer for commodity bought
     * @param statValue value statistics
     * @param commodityKey unique key to associate commodities between seller and buyer
     * @param relation stat relation to entity, e.g., "CommoditiesBought"
     * @return a {@link StatRecord} protobuf populated with the given values
     */
    @Nonnull
    default StatRecord buildStatRecord(@Nonnull String propertyType,
                                       @Nullable String propertySubtype,
                                       @Nullable Float capacity,
                                       @Nullable Float reserved,
                                       @Nullable String relatedEntityType,
                                       @Nullable Long producerId,
                                       @Nullable StatValue statValue,
                                       @Nullable String commodityKey,
                                       @Nullable String relation) {
        return buildStatRecord(propertyType,
                propertySubtype,
                capacity == null ? null : StatsAccumulator.singleStatValue(capacity),
                reserved,
                relatedEntityType,
                producerId,
                statValue,
                commodityKey,
                relation,
                null);
    }

    /**
     * The default implementation of {@link StatRecordBuilder}, for production use.
     */
    class DefaultStatRecordBuilder implements StatRecordBuilder {
        /**
         * The Flow names.
         */
        private static final String[] FLOW_NAMES = new String[]{"InProvider", "InDPOD",
                                                                "CrossDPOD", "CrossSite"};

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
                                          @Nullable final String relatedEntityType,
                                          @Nullable final Long producerId,
                                          @Nullable final StatValue statValue,
                                          @Nullable final String commodityKey,
                                          @Nullable final String relation,
                                          @Nullable final StatValue percentileUtilization) {
            final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder();
            if (propertyType.contains("Flow") && commodityKey != null && commodityKey.length() > 5) {
                int index = Integer.parseInt(commodityKey.substring(5));
                statRecordBuilder.setName(FLOW_NAMES[index]);
            } else {
                statRecordBuilder.setName(propertyType);
            }

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
            if (relatedEntityType != null) {
                statRecordBuilder.setRelatedEntityType(relatedEntityType);
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
            } else if (propertyType.startsWith(StringConstants.STAT_PREFIX_CURRENT)) {

                //Plan aggregated source stats have "current" prefix attached
                //We need to remove this prefix and do case insensitive match for CommodityTypeUnits
                //No matches occurs for metrics, {@link StatsMapper.METRIC_NAMES}, i.e numVMs

                final String removedCurrentPrefix =
                        propertyType.substring(StringConstants.STAT_PREFIX_CURRENT.length());
                commodityType = CommodityTypeUnits.fromStringIgnoreCase(removedCurrentPrefix);
                if (commodityType != null) {
                    statRecordBuilder.setUnits(commodityType.getUnits());
                }

            }

            // currentValue
            if (statValue != null && statValue.hasAvg() && (propertySubtype == null ||
                    StringConstants.PROPERTY_SUBTYPE_USED.equals(propertySubtype))) {
                statRecordBuilder.setCurrentValue(statValue.getAvg());
            } else if (statValue != null && statValue.hasMax()) {
                statRecordBuilder.setCurrentValue(statValue.getMax());
            }

            statRecordBuilder.setValues(statValue);
            statRecordBuilder.setUsed(statValue);
            statRecordBuilder.setPeak(statValue);
            if (percentileUtilization != null) {
                statRecordBuilder.setPercentileUtilization(percentileUtilization);
            }
            return statRecordBuilder.build();
        }
    }
}
