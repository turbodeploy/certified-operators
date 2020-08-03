/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.snapshots;

import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.jooq.Record;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.stats.PropertySubType;
import com.vmturbo.history.stats.snapshots.UsageRecordVisitor.UsageState;

/**
 * {@link UsageRecordVisitor} visits property subtype, avg, min and max fields in stats record and
 * populates used, current value, values, used, and peak fields in {@link StatRecord.Builder}.
 */
@ThreadSafe
public class UsageRecordVisitor extends AbstractVisitor<Record, UsageState> {
    private final boolean fullMarket;
    private final BiConsumer<StatRecord.Builder, Pair<StatValue, String>> usagePopulator;

    /**
     * Creates {@link UsageRecordVisitor} instance.
     *
     * @param fullMarket whether we want to get stat record about full market or
     *                 not.
     * @param usagePopulator populates usage value into {@link StatRecord.Builder}
     */
    public UsageRecordVisitor(boolean fullMarket,
                    @Nonnull BiConsumer<StatRecord.Builder, Pair<StatValue, String>> usagePopulator) {
        super(UsageState::clear);
        this.fullMarket = fullMarket;
        this.usagePopulator = usagePopulator;
    }

    @Override
    public void visit(@Nonnull Record record) {
        final Float avgValue = getFloatValue(record, StringConstants.AVG_VALUE);
        final Float minValue = getFloatValue(record, StringConstants.MIN_VALUE);
        final Float maxValue = getFloatValue(record, StringConstants.MAX_VALUE);
        final String propertySubTypeValue =
                        RecordVisitor.getFieldValue(record, StringConstants.PROPERTY_SUBTYPE,
                                        String.class);
        final UsageState state = ensureState(() -> new UsageState(propertySubTypeValue, avgValue != null,
                minValue != null, maxValue != null), record);

        final float avgFloat = avgValue == null ? 0 : avgValue;
        final float minFloat = minValue == null ? avgFloat : minValue;
        final float maxFloat = maxValue == null ? avgFloat : maxValue;

        state.record(minFloat, avgFloat, maxFloat);
    }

    private static Float getFloatValue(@Nonnull Record record, String fieldName) {
        return RecordVisitor.getFieldValue(record, fieldName, Float.class);
    }

    @Override
    protected void buildInternally(@Nonnull Builder builder, @Nonnull Record record,
                    @Nonnull UsageState state) {
        final StatValue statValue = getUsageStat(state.toStatValue());
        usagePopulator.accept(builder, new Pair<>(statValue, state.getPropertySubType()));
    }

    private StatValue getUsageStat(StatValue usageStat) {
        if (fullMarket) {
            // For the full market, we don't divide the values by the number of
            // records. This is because we know all records for this commodity at
            // this time refer to the same "entity" (i.e. the market). The total
            // value for this entity got "split" according to certain properties
            // when we saved it, and we add up all the matching rows to "re-unite"
            // it at query-time.
            //
            // For example, if a certain commodity "amtConsumed" has value 2 for
            // environment_type ON_PREM and value 3 for environment_type CLOUD,
            // then when we search for commodity "amtConsumed" with no environment
            // type filter (i.e. we want the amount consumed across environments),
            // the returned value should be 5 (total), not 2.5 (average)
            //
            // Note (roman, Feb 6 2019): It's not clear what the different expected
            // records are here. It's possible that there are stats for which we
            // DO want the amount to be averaged across records.
            return usageStat.toBuilder().setAvg(usageStat.getTotal())
                            .setMax(usageStat.getTotalMax()).setMin(usageStat.getTotalMin())
                            .build();
        }
        return usageStat;
    }

    /**
     * Populates {@link StatRecord.Builder} instance with usage related values, calculated from
     * stats record.
     */
    public static class UsagePopulator
                    implements BiConsumer<StatRecord.Builder, Pair<StatValue, String>> {
        @Override
        public void accept(StatRecord.Builder builder,
                        Pair<StatValue, String> statValuePropertySubTypePair) {
            final StatValue statValue = statValuePropertySubTypePair.first;
            final PropertySubType propertySubType =
                            PropertySubType.fromApiParameter(statValuePropertySubTypePair.second);
            builder.setUsed(statValue);
            // currentValue
            if (statValue != null) {
                if (statValue.hasAvg() && PropertySubType.Used == propertySubType) {
                    builder.setCurrentValue(statValue.getAvg());
                } else if (statValue.hasMax()) {
                    builder.setCurrentValue(statValue.getMax());
                }
            }
            builder.setValues(statValue);
            builder.setUsed(statValue);
            builder.setPeak(statValue);
        }
    }


    /**
     * Contains state accumulated by {@link UsageRecordVisitor} instance.
     */
    public static class UsageState {
        private final StatsAccumulator usageAccumulator = new StatsAccumulator();
        private String propertySubType;
        private boolean hasAvg = true;
        private boolean hasMin = true;
        private boolean hasMax = true;

        private UsageState(String propertySubType, boolean hasAvg, boolean hasMin, boolean hasMax) {
            this.propertySubType = propertySubType;
            this.hasAvg = hasAvg;
            this.hasMin = hasMin;
            this.hasMax = hasMax;
        }

        private String getPropertySubType() {
            return propertySubType;
        }

        @Nonnull
        private StatsAccumulator record(double minValue, double avgValue, double peakValue) {
            return usageAccumulator.record(minValue, avgValue, peakValue);
        }

        @Nonnull
        private StatValue toStatValue() {
            StatValue.Builder valueBuilder = StatValue.newBuilder();
            if (hasAvg) {
                valueBuilder.setAvg((float)usageAccumulator.getAvg());
                valueBuilder.setTotal((float)usageAccumulator.getTotal());
            }
            if (hasMin) {
                valueBuilder.setMin((float)usageAccumulator.getMin());
                valueBuilder.setTotalMin((float)usageAccumulator.getTotalMin());
            }
            if (hasMax) {
                valueBuilder.setMax((float)usageAccumulator.getMax());
                valueBuilder.setTotalMax((float)usageAccumulator.getTotalMax());
            }
            return valueBuilder.build();
        }

        private void clear() {
            usageAccumulator.clear();
            propertySubType = null;
            hasAvg = true;
            hasMin = true;
            hasMax = true;
        }

    }
}
