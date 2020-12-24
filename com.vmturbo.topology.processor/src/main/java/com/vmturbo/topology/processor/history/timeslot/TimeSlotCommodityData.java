package com.vmturbo.topology.processor.history.timeslot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.HistoryAggregationContext;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.ICommodityFieldAccessor;
import com.vmturbo.topology.processor.history.IHistoryCommodityData;

/**
 * Per-commodity cache for storing utilizations for time-slot calculations.
 */
public class TimeSlotCommodityData
                implements IHistoryCommodityData<TimeslotHistoricalEditorConfig, List<Pair<Long, StatRecord>>, Void>,
        Serializable {
    private static final Logger logger = LogManager.getLogger();
    private static final String LOG_PREFIX = "Timeslot calculation: ";
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    // hourly slots' statistics for entire observation period
    private SlotStatistics[] previousSlots;
    // statistics since the beginning of current hour (not accumulated into previousSlots yet)
    private SlotStatistics currentSlot = new SlotStatistics();
    // oldest moment of time recorded in current slot
    private long timestamp;
    private long lastMaintenanceTimestamp;
    private int currentObservationPeriod;
    private EntityCommodityFieldReference reference;

    /**
     * Construct a copy of <code>other</code>.
     *
     * @param other object to copy from
     */
    public TimeSlotCommodityData(@Nonnull TimeSlotCommodityData other) {
        this.previousSlots = Arrays.copyOf(other.previousSlots, other.previousSlots.length);
        this.currentSlot = new SlotStatistics(other.currentSlot);
        this.timestamp = other.timestamp;
        this.lastMaintenanceTimestamp = other.lastMaintenanceTimestamp;
        this.currentObservationPeriod = other.currentObservationPeriod;
        this.reference = other.reference;
    }

    /**
     * Construct an empty instance of class.
     *
     * @apiNote We need an explicit empty constructor because this class has a throwing copy
     *         one.
     */
    public TimeSlotCommodityData() {}

    /**
     * Returns last successful maintenance timestamp or 0L in case maintenance did not happen yet.
     *
     * @return last successful maintenance timestamp.
     */
    public long getLastMaintenanceTimestamp() {
        return lastMaintenanceTimestamp;
    }

    @Override
    public synchronized void init(@Nonnull EntityCommodityFieldReference field,
                     @Nullable List<Pair<Long, StatRecord>> dbValue, @Nonnull TimeslotHistoricalEditorConfig config,
                     @Nonnull HistoryAggregationContext context) {
        reference = field;
        currentObservationPeriod = config.getObservationPeriod(context, field);
        final int slotCount = config.getSlots(context, field);
        if (logger.isTraceEnabled()) {
            logger.trace("{}initializing timeslot cache for {} and {} slots", LOG_PREFIX, field, slotCount);
        }
        if (slotCount > 1) {
            previousSlots = createSlots(slotCount);
            if (CollectionUtils.isNotEmpty(dbValue)) {
                // expect to be ordered and rounded to hourly points as sent from persistence
                recordsToSlots(dbValue, previousSlots);
                lastMaintenanceTimestamp = dbValue.stream().mapToLong(Pair::getFirst).min()
                                .orElseGet(() -> config.getClock().millis());
            }
        } else {
            lastMaintenanceTimestamp = config.getClock().millis();
            previousSlots = null;
            currentSlot.clear();
        }
    }

    private static SlotStatistics[] createSlots(int slotCount) {
        final SlotStatistics[] result = new SlotStatistics[slotCount];
        for (int i = 0; i < slotCount; ++i) {
            result[i] = new SlotStatistics();
        }
        return result;
    }

    /**
     * Checks whether currently existing settings have changed.
     *
     * @param ref reference to entity commodity for which we want to check whether
     *                 settings have been changed or not.
     * @param context history context which contains information about entities from
     *                 the current broadcast.
     * @param config config which knows how to extract setting values from the
     *                 current broadcast snapshot.
     * @return {@code true} in case settings changed, otherwise {@code false}.
     */
    @Override
    public boolean needsReinitialization(@Nonnull EntityCommodityReference ref,
                    @Nonnull HistoryAggregationContext context,
                    @Nonnull TimeslotHistoricalEditorConfig config) {
        final int slotCount = config.getSlots(context, ref);
        final int observationPeriod = config.getObservationPeriod(context, ref);
        return (previousSlots != null && slotCount != previousSlots.length)
                        || currentObservationPeriod != observationPeriod;
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
        long now = config.getClock().millis();
        final int length = previousSlots.length;
        if (!context.isPlan()) {
            if (lastMaintenanceTimestamp <= 0) {
                lastMaintenanceTimestamp = now;
            }
            if (currentSlot.count == 0) {
                timestamp = now;
            } else if (timestamp > 0
                       && (now - timestamp) / TimeUnit.HOURS.toMillis(1) > 0) {
                // crossed the hour boundary - dump currentSlot into previousSlots
                int slot = getSlot(timestamp, length);
                if (logger.isTraceEnabled()) {
                    float hourAverage = currentSlot.total / currentSlot.count;
                    logger.trace(LOG_PREFIX + "adding hourly point {} to slot {} of {}", hourAverage, slot, field);
                }
                previousSlots[slot].accumulate(currentSlot, true);
                timestamp = now;
            }
            // add discovered point
            Double used = commodityFieldsAccessor.getRealTimeValue(field);
            if (used != null) {
                currentSlot.count++;
                currentSlot.total += used;
            }
        }


        // set historical value in the broadcast
        final List<Double> averagedSlotUsages = getAveragedSlotUsages(now, length);
        if (logger.isTraceEnabled()) {
            logger.trace(LOG_PREFIX + "values for {}: {}", field, averagedSlotUsages);
        }
        commodityFieldsAccessor.updateHistoryValue(field,
                                                   hv -> hv.addAllTimeSlot(averagedSlotUsages),
                                                   TimeSlotEditor.class.getSimpleName());
    }

    @Nonnull
    private List<Double> getAveragedSlotUsages(long now, int length) {
        final List<Double> averagedSlotUsages = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final SlotStatistics previousSlot = previousSlots[i];
            final boolean isCurrentSlot = getSlot(now, length) == i;
            final SlotStatistics other = isCurrentSlot ? currentSlot : null;
            final double averageValue = previousSlot.accumulate(other, !isCurrentSlot);
            averagedSlotUsages.add(averageValue);
        }
        return averagedSlotUsages;
    }

    @Override
    public synchronized Void checkpoint(@Nonnull List<List<Pair<Long, StatRecord>>> outdated)
                    throws HistoryCalculationException {
        if (previousSlots == null) {
            // no slots configured
            return null;
        }
        // subtract outdated from previous slots
        final SlotStatistics[] outdatedSlots = createSlots(previousSlots.length);
        for (List<Pair<Long, StatRecord>> outdatedPage : outdated) {
            recordsToSlots(outdatedPage, outdatedSlots);
        }
        for (int i = 0; i < previousSlots.length; ++i) {
            previousSlots[i].subtract(outdatedSlots[i]);
        }
        lastMaintenanceTimestamp = 0;
        // do not save anything
        return null;
    }

    private static int getSlot(long timestamp, int slots) {
        long startOfDay = timestamp / MILLIS_IN_DAY * MILLIS_IN_DAY;
        long slotLength = MILLIS_IN_DAY / slots;
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
            slots[slot].total += used;
        }
    }

    /**
     * Getting an array of previous slots.
     *
     * @return array of previous slots.
     */
    @VisibleForTesting
    protected SlotStatistics[] getPreviousSlots() {
        return previousSlots;
    }

    /**
     * Creates a string which is describing current instance in details. Useful for debugging
     * purposes.
     *
     * @return string to represent current data instance for debugging purposes.
     */
    @Nonnull
    public String toDebugString() {
        return String.format(
                        "%s [previousSlots=%s, currentSlot=%s, timestamp=%s, lastMaintenanceTimestamp=%s, currentObservationPeriod=%s, reference=%s]",
                        getClass().getSimpleName(), Arrays.toString(this.previousSlots),
                        this.currentSlot, this.timestamp, this.lastMaintenanceTimestamp,
                        this.currentObservationPeriod, this.reference);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimeSlotCommodityData that = (TimeSlotCommodityData)o;
        return Objects.equals(this.reference, that.reference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reference);
    }

    /**
     * Holder for the data of a single time slot.
     */
    static class SlotStatistics implements Serializable {
        // number of recorded points in that slot
        private int count;
        // sum of utilizations of points over the slot (regardless of capacities)
        private float total;

        /**
         * Construct the slot statistics.
         */
        SlotStatistics() {
        }

        private SlotStatistics(@Nonnull SlotStatistics other) {
            this.total = other.total;
            this.count = other.count;
        }

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
         * @param update should we update the current instance or create a copy from the
         *         current instance.
         * @return average timeslot value by dividing the total / count.
         */
        public double accumulate(@Nullable SlotStatistics other, boolean update) {
            final SlotStatistics result = update ? this : new SlotStatistics(this);
            if (other != null) {
                if (other.count > 0) {
                    result.count++;
                    result.total += other.total / other.count;
                }
                if (update) {
                    other.clear();
                }
            }
            return result.count == 0 ? 0D : result.total / result.count;
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

        @Override
        public String toString() {
            return String.format("%s [count=%s, total=%s]", getClass().getSimpleName(), this.count,
                            this.total);
        }
    }
}
