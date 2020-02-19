package com.vmturbo.topology.processor.history.timeslot;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.commons.forecasting.TimeInMillisConstants;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;

/**
 * Per-commodity cache for storing utilizations for time-slot calculations.
 */
public class TimeSlotCommodityData
                implements IHistoryCommodityData<TimeslotHistoricalEditorConfig, List<Pair<Long, StatRecord>>, Void> {
    private static final Logger logger = LogManager.getLogger();
    private static final String LOG_PREFIX = "Timeslot calculation: ";
    // hourly slots' statistics for entire observation period
    private SlotStatistics[] previousSlots;
    // statistics since the beginning of current hour (not accumulated into previousSlots yet)
    private SlotStatistics currentSlot = new SlotStatistics();
    // oldest moment of time recorded in current slot
    private long timestamp;

    @Override
    public synchronized void init(@Nonnull EntityCommodityFieldReference field,
                     @Nullable List<Pair<Long, StatRecord>> dbValue, @Nonnull TimeslotHistoricalEditorConfig config,
                     @Nonnull HistoryAggregationContext context) {
        int slotCount = config.getSlots(context, field.getEntityOid());
        if (logger.isTraceEnabled()) {
            logger.trace("{}initializing timeslot cache for {} and {} slots", LOG_PREFIX, field, slotCount);
        }
        if (slotCount > 1) {
            previousSlots = new SlotStatistics[slotCount];
            for (int i = 0; i < slotCount; ++i) {
                previousSlots[i] = new SlotStatistics();
            }
            if (CollectionUtils.isNotEmpty(dbValue)) {
                // expect to be ordered and rounded to hourly points as sent from persistence
                recordsToSlots(dbValue, previousSlots);
            }
        } else {
            previousSlots = null;
            currentSlot.clear();
        }
    }

    @Override
    public synchronized void aggregate(@Nonnull EntityCommodityFieldReference field,
                          @Nonnull TimeslotHistoricalEditorConfig config,
                          @Nonnull HistoryAggregationContext context) {
        if (previousSlots == null) {
            // no slots configured
            return;
        }
        final ICommodityFieldAccessor commodityFieldsAccessor = context.getAccessor();
        Double capacity = commodityFieldsAccessor.getCapacity(field);
        if (capacity == null || capacity <= 0d) {
            logger.error(LOG_PREFIX + "cannot be done for " + field
                         + ": cannot find capacity for commodity");
            return;
        }

        if (!context.isPlan()) {
            long now = config.getClock().millis();
            if (currentSlot.count == 0) {
                timestamp = now;
            } else if (timestamp > 0
                       && (now - timestamp) / TimeInMillisConstants.HOUR_LENGTH_IN_MILLIS > 0) {
                // crossed the hour boundary - dump currentSlot into previousSlots
                int slot = getSlot(timestamp, previousSlots.length);
                float hourAverage = currentSlot.total / currentSlot.count;
                if (logger.isTraceEnabled()) {
                    logger.trace(LOG_PREFIX + "adding hourly point {} to slot {} of {}", hourAverage, slot, field);
                }
                previousSlots[slot].accumulate(currentSlot);
                timestamp = now;
            }
            // add discovered point
            Double used = commodityFieldsAccessor.getRealTimeValue(field);
            if (used != null) {
                currentSlot.count++;
                currentSlot.total += used / capacity;
            }
        }

        // set historical value in the broadcast
        List<Double> averagedSlotUsages = Arrays.stream(previousSlots)
                        .map(stat -> stat.count == 0 ? 0D : stat.total / stat.count)
                        .collect(Collectors.toList());
        if (logger.isTraceEnabled()) {
            logger.trace(LOG_PREFIX + "values for {}: {}", field, averagedSlotUsages);
        }
        commodityFieldsAccessor.updateHistoryValue(field,
                                                   hv -> hv.addAllTimeSlot(averagedSlotUsages),
                                                   TimeSlotEditor.class.getSimpleName());
    }

    @Override
    public synchronized Void checkpoint(@Nonnull List<List<Pair<Long, StatRecord>>> outdated)
                    throws HistoryCalculationException {
        if (previousSlots == null) {
            // no slots configured
            return null;
        }
        // subtract outdated from previous slots
        SlotStatistics[] outdatedSlots = new SlotStatistics[previousSlots.length];
        for (List<Pair<Long, StatRecord>> outdatedPage : outdated) {
            recordsToSlots(outdatedPage, outdatedSlots);
        }
        for (int i = 0; i < previousSlots.length; ++i) {
            previousSlots[i].subtract(outdatedSlots[i]);
        }
        // do not save anything
        return null;
    }

    private static int getSlot(long timestamp, int slots) {
        long startOfDay = timestamp / TimeInMillisConstants.DAY_LENGTH_IN_MILLIS
                          * TimeInMillisConstants.DAY_LENGTH_IN_MILLIS;
        long slotLength = TimeInMillisConstants.DAY_LENGTH_IN_MILLIS / slots;
        return (int)((timestamp - startOfDay) / slotLength);
    }

    @VisibleForTesting
    static void recordsToSlots(@Nonnull List<Pair<Long, StatRecord>> dbValue,
                               @Nonnull SlotStatistics[] slots) {
        for (Pair<Long, StatRecord> point : dbValue) {
            int slot = getSlot(point.getFirst(), slots.length);
            slots[slot].count++;
            StatRecord stat = point.getSecond();
            float cap = stat.getCapacity().getAvg();
            if (!stat.hasCapacity() || !stat.getCapacity().hasAvg() || cap <= 0) {
                logger.debug(LOG_PREFIX + "db capacity is missing for {} at {}",
                             stat::getStatKey, () -> point.getFirst());
                continue;
            }
            float used = stat.getUsed().getAvg();
            slots[slot].total += used / cap;
        }
    }

    /**
     * Holder for the data of a single time slot.
     */
    static class SlotStatistics {
        // number of recorded points in that slot
        private int count;
        // sum of utilizations of points over the slot (regardless of capacities)
        private float total;

        /**
         * Subtract the data from another slot instance.
         *
         * @param other slots to accumulate in and clear
         */
        public void subtract(@Nonnull SlotStatistics other) {
            count = Math.max(0, count - other.count);
            total = Math.max(0, total - other.total);
        }

        /**
         * Accumulate the data from another slot instance by compressing.
         * Clear that other instance.
         *
         * @param other slots to accumulate in and clear
         */
        public void accumulate(@Nonnull SlotStatistics other) {
            if (other.count > 0) {
                count++;
                total += other.total / other.count;
            }
            other.clear();
        }

        /**
         * Clear the data.
         */
        public void clear() {
            count = 0;
            total = 0;
        }

        public int getCount() {
            return count;
        }

        public float getTotal() {
            return total;
        }
    }
}
