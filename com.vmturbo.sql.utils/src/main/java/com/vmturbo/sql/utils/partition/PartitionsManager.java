package com.vmturbo.sql.utils.partition;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Table;
import org.threeten.extra.Interval;
import org.threeten.extra.PeriodDuration;

import com.vmturbo.components.common.utils.RollupTimeFrame;

/**
 * This class models partitions present in various tables, typically all the partitioned tables in a
 * given schema. It also supports a handful of partitioning-related operations, which are executed
 * on the database itself and also update the in-memory models accordingly.
 *
 * <p>Currently only time-based partitions are supported.</p>
 */
public class PartitionsManager {
    private static final Logger logger = LogManager.getLogger();

    private final Map<String, Map<String, List<Partition<Instant>>>> partMap = new HashMap<>();

    private final IPartitionAdapter adapter;

    /**
     * Create a new instance.
     *
     * @param adapter dialect-specific adapter for partitioning operations in the database
     */
    public PartitionsManager(IPartitionAdapter adapter) {
        this.adapter = adapter;
    }

    /**
     * Load partition information for all partitioned tables in the given schema.
     *
     * <p>This method discards and replaces any partition info currently held for the given
     * schema.</p>
     *
     * @param schemaName schema whose tables should be loaded
     * @return number of partitioned tables loaded
     * @throws PartitionProcessingException If the operation fails
     */
    public synchronized int load(String schemaName) throws PartitionProcessingException {
        Map<String, List<Partition<Instant>>> tableParts = adapter.getSchemaPartitions(schemaName);
        partMap.put(schemaName, tableParts);
        return tableParts.size();
    }

    /**
     * Reload all the schemas we have loaded in the past.
     */
    public void reload() {
        for (String schemaName : partMap.keySet()) {
            try {
                load(schemaName);
            } catch (PartitionProcessingException e) {
                logger.error("Failed to reload partition info for schema {}", schemaName, e);
            }
        }
    }

    /**
     * Make sure there is a partition to receive records with the given time value, creating a
     * suitable partition if none exists.
     *
     * @param schemaName        the name of the table schema
     * @param table         the name of the table
     * @param t             the time for which a partition is needed
     * @param partitionSize currently configured partition size for this table
     * @return true if a suitable partition already existed, false if a new partition had to be
     *         created
     * @throws PartitionProcessingException if there's a problem creating a new partition
     */
    public synchronized boolean ensurePartition(String schemaName, Table<?> table, Instant t,
            PeriodDuration partitionSize) throws PartitionProcessingException {
        Map<String, List<Partition<Instant>>> tableMap = partMap.get(schemaName);
        List<Partition<Instant>> tableParts =
                tableMap != null ? tableMap.get(table.getName()) : null;
        Partition<Instant> existingPart = null;
        if (!CollectionUtils.emptyIfNull(tableParts).isEmpty()) {
            existingPart = tableParts.stream()
                    .filter(p -> p.contains(t))
                    .findFirst()
                    .orElse(null);
        }
        if (existingPart == null) {
            Interval alignedBounds = getAlignedBounds(t, partitionSize);
            // the nearest ascending or descending partitions might actually have contained timestamp t,
            // but we've already determined that no partition contains timestamp t
            Pair<Partition<Instant>, Partition<Instant>> nearestAdjacentParts =
                    findNearestAdjacentPartitions(t, tableParts);
            Interval adjustedBounds =
                    adjustBounds(alignedBounds, t, tableParts, nearestAdjacentParts);
            Partition<Instant> newPartition = adapter.createPartition(
                    schemaName, table.getName(),
                    adjustedBounds.getStart(), adjustedBounds.getEnd(),
                    nearestAdjacentParts.getRight());
            addPartitionToInfo(newPartition);
        }
        return existingPart != null;
    }

    /**
     * Drop any partitions that are expired according to current retention settings.
     *
     * @param schemaName        name of schema to operate on
     * @param tableTimeframeFn  function to determine timeframe (and hence applicable retention
     *                          setting) for any given table
     * @param retentionSettings current retention settings
     * @param t                 reference time to use in computing expiry times
     */
    public void dropExpiredPartitions(String schemaName,
            Function<String, Optional<RollupTimeFrame>> tableTimeframeFn,
            RetentionSettings retentionSettings, Instant t) {
        OffsetDateTime refTime = OffsetDateTime.ofInstant(t, ZoneOffset.UTC);
        Map<String, List<Partition<Instant>>> tableMap =
                partMap.getOrDefault(schemaName, Collections.emptyMap());
        for (String tableName : tableMap.keySet()) {
            Instant threshold = tableTimeframeFn.apply(tableName)
                    .map(retentionSettings::getRetentionPeriod)
                    .map(period -> period.subtractFrom(refTime))
                    .map(Instant::from)
                    .orElse(null);
            if (threshold != null) {
                List<Partition<Instant>> toBeDropped = new ArrayList<>();
                for (Partition<Instant> part : tableMap.get(tableName)) {
                    if (!part.getExclusiveUpper().isAfter(threshold)) {
                        try {
                            adapter.dropPartition(part);
                            toBeDropped.add(part);
                        } catch (PartitionProcessingException e) {
                            logger.error("Failed to drop partition {} of table {}",
                                    part.getPartitionName(), part.getTableName(), e);
                        }
                    } else {
                        // partition lists are sorted, so we don't need to keep looking
                        break;
                    }
                }
                tableMap.get(tableName).removeAll(toBeDropped);
            }
        }
    }

    private Interval getAlignedBounds(Instant t, PeriodDuration partitionSize) {
        // convert target time to OffsetDateTime since we can't add/remove periods involving
        // months or years to Instant values.
        OffsetDateTime tODT = OffsetDateTime.ofInstant(t, ZoneOffset.UTC);
        // get a duration to use as if our partition size were uniform (which it may or may not be
        OffsetDateTime sampleStart =
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(0L), ZoneOffset.UTC);
        OffsetDateTime sampleEnd = sampleStart.plus(partitionSize);
        Duration sampleDuration = Duration.between(sampleStart, sampleEnd);
        // now figure out how many of those precede our target time, starting with epoch
        int n = (int)(t.toEpochMilli() / sampleDuration.toMillis());
        // figure out where exactly that many partitions would land us
        OffsetDateTime start = sampleStart.plus(partitionSize.multipliedBy(n));
        // now move backward or forward to find the correct partition start.
        // at most one of the while loops below will execute any actual iterations.
        // if we landed in an interval that's beyond our target time, move back until we're good
        while (start.isAfter(tODT)) {
            start = start.minus(partitionSize);
        }
        OffsetDateTime end = start.plus(partitionSize);
        // if we (initially) landed in an interval that precedes our target time, move forward
        // until we're good (we won't ever have to do this if we had to move back in the above
        // loop)
        while (!tODT.isBefore(end)) {
            end = end.plus(partitionSize);
        }
        start = end.minus(partitionSize);
        return Interval.of(Instant.from(start), Instant.from(end));
    }

    @Nonnull
    private Pair<Partition<Instant>, Partition<Instant>> findNearestAdjacentPartitions(Instant t, @Nullable List<Partition<Instant>> tableParts) {
        // find the earliest partition that at least partially follows our target time - and
        // if found, note its immediate predecessor as well
        // (i.e. if t is in a partition, it will be considered the next "following" partition)
        Partition<Instant> followingPart = null;
        Partition<Instant> priorPart = null;
        if (!CollectionUtils.emptyIfNull(tableParts).isEmpty()) {
            for (int i = 0; i < tableParts.size(); i++) {
                if (tableParts.get(i).getExclusiveUpper().isAfter(t)) {
                    followingPart = tableParts.get(i);
                    priorPart = i > 0 ? tableParts.get(i - 1) : null;
                    break;
                }
            }
        }
        return Pair.of(priorPart, followingPart);
    }

    private Interval adjustBounds(
            Interval alignedBounds, Instant t, List<Partition<Instant>> tableParts,
            Pair<Partition<Instant>, Partition<Instant>> nearestAdjacentParts)
            throws PartitionProcessingException {
        Instant start = alignedBounds.getStart();
        Instant end = alignedBounds.getEnd();
        if (!CollectionUtils.emptyIfNull(tableParts).isEmpty()) {
            // the earliest partition that at least partially follows our target time,
            // and that partition's immediate predecessor
            Partition<Instant> followingPart = nearestAdjacentParts.getRight();
            Partition<Instant> priorPart = nearestAdjacentParts.getLeft();

            if (followingPart == null) {
                // no partition follows our target point, so we need to go at the end.
                Partition<Instant> lastPart = tableParts.get(tableParts.size() - 1);
                if (start.isBefore(lastPart.getExclusiveUpper())) {
                    start = lastPart.getExclusiveUpper();
                }
            } else {
                // we know the earliest existing partition that at least partially follows our
                // target time. We need to match our bounds with that partition and the prior
                // partition. Note that with MariaDB this situation can't arise, because it
                // requires a gap in the partitioning immediately prior to the followingPart.
                // Since lower bounds are implicit in MariaDB (coinciding with the prior part's
                // upper bound), gaps are impossible.
                if (end.isAfter(followingPart.getInclusiveLower())) {
                    end = followingPart.getInclusiveLower();
                }
                if (priorPart != null && start.isBefore(priorPart.getExclusiveUpper())) {
                    start = priorPart.getExclusiveUpper();
                }
            }
        }
        // here we've either adjusted our aligned bounds to fit among existing partitions, or
        // we've determined that our aligned bounds fit with no need for adjustment.
        // check to make sure we didn't adjust our bounds into nonsense
        if (!start.isBefore(end)) {
            logger.error(
                    "Nonsense adjusted partition bounds [{}, {}) from aligned bounds [{}, {})",
                    start, end, alignedBounds.getStart(), alignedBounds.getEnd());
            throw new PartitionProcessingException(
                    "Failed to fit new partition among existing partitions");
        }
        return Interval.of(start, end);
    }

    private void addPartitionToInfo(Partition<Instant> newPartition) {
        Map<String, List<Partition<Instant>>> tableMap = partMap.computeIfAbsent(
                newPartition.getSchemaName(), _s -> new HashMap<>());
        List<Partition<Instant>> tableParts = tableMap.computeIfAbsent(
                newPartition.getTableName(), _t -> new ArrayList<>());
        tableParts.add(newPartition);
        // keep the partitions list properly sorted
        tableParts.sort(Comparator.comparing(Partition::getInclusiveLower));
    }
}
