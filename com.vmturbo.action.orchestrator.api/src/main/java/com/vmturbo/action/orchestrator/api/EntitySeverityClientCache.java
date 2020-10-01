package com.vmturbo.action.orchestrator.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.EntitiesWithSeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification.SeverityBreakdown.SingleSeverityCount;

/**
 * A reusable cache to capture entity severities sent out by the action orchestrator.
 *
 * <p/>This is expected to be small, because there is at most one entry per entity with an action,
 * plus one entry per business application. The former is just a long + {@link Severity} enum, and
 * the latter is a long + a 4-element array.
 */
public class EntitySeverityClientCache implements EntitySeverityListener {
    private static final Logger logger = LogManager.getLogger();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @GuardedBy("lock")
    private final Long2ObjectOpenHashMap<Severity> nonDefaultSeveritiesByEntity = new Long2ObjectOpenHashMap<>();

    @GuardedBy("lock")
    private final Long2ObjectOpenHashMap<SeverityBreakdown> severityBreakdowns = new Long2ObjectOpenHashMap<>();

    /**
     * Get the total number of severities in the cache.
     *
     * @return The number of severities.
     */
    public int numSeverities() {
        lock.readLock().lock();
        try {
            return nonDefaultSeveritiesByEntity.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the total number of severity breakdowns in the cache.
     *
     * @return The number of severity breakdowns.
     */
    public int numBreakdowns() {
        lock.readLock().lock();
        try {
            return severityBreakdowns.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the severity counts for a list of input entities.
     *
     * @param entityOids The list of input OIDs.
     * @return A map of ({@link Severity}) to the number of entities in the input that have
     *         those severities. For entities that have severity breakdowns (e.g. business apps)
     *         the returned map will contain all severities from the breakdown. Entities that do
     *         not exist in the cache will be assumed to have a {@link Severity#NORMAL} severity.
     *         The cache has no way of knowing if those entities are invalid, or simply have
     *         no actions.
     */
    @Nonnull
    public Map<Severity, Long> getSeverityCounts(@Nonnull final List<Long> entityOids) {
        final Object2LongMap<Severity> retMap = new Object2LongOpenHashMap<>(Severity.values().length);
        retMap.defaultReturnValue(0);
        lock.readLock().lock();
        try {
            // Unbox once.
            for (long oid : entityOids) {
                final SeverityBreakdown breakdown = severityBreakdowns.get(oid);
                if (breakdown != null) {
                    breakdown.visitSeverityCounts((severity, cnt) -> {
                        long curCnt = retMap.getLong(severity);
                        retMap.put(severity, curCnt + cnt);
                    });
                } else {
                    final Severity s = getEntitySeverity(oid);
                    retMap.put(s, retMap.getLong(s) + 1);
                }
            }
            return retMap;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Captures the severity counts of an "aggregated" entity like a business application.
     */
    private static class SeverityBreakdown {
        /**
         * Used for double comparisons.
         */
        private static final double EPSILON = 0.00001;

        /**
         * Cached {@link Severity} values to avoid constructing the array multiple times.
         */
        private static final Severity[] SEVERITIES = Severity.values();

        /**
         * The index in the array is the {@link Severity#ordinal()} value.
         * The value at the index is the number of entities with that severity represented in this
         * breakdown.
         */
        private final IntList cntList;

        SeverityBreakdown() {
            cntList = new IntArrayList(SEVERITIES.length);
            for (Severity s : SEVERITIES) {
                cntList.add(s.ordinal(), 0);
            }

            reset();
        }

        private void reset() {
            for (Severity s : Severity.values()) {
                cntList.set(s.ordinal(), 0);
            }
        }

        void updateSeverityCnt(@Nonnull final List<SingleSeverityCount> severityCount) {
            // Reset, so that any severities not present in the input list go back to 0.
            reset();
            severityCount.forEach(cnt -> {
                cntList.set(cnt.getSeverity().ordinal(), cnt.getCount());
            });
        }

        int totalSeverities() {
            int total = 0;
            for (int i = 0; i < cntList.size(); ++i) {
                total += cntList.getInt(i);
            }
            return total;
        }

        /**
         * Return the highest severity present in this breakdown, or NORMAL if the breakdown is
         * empty.
         *
         * @return A {@link Severity}.
         */
        @Nonnull
        Severity highestSeverity() {
            // Start from the highest severity, and proceed to the lowest.
            for (int i = cntList.size() - 1; i >= 0; --i) {
                if (cntList.getInt(i) > 0) {
                    return SEVERITIES[i];
                }
            }
            return Severity.NORMAL;
        }

        public int compareTo(@Nonnull final SeverityBreakdown other) {
            final double thisTotal = totalSeverities();
            final double otherTotal = other.totalSeverities();

            if (thisTotal == 0) {
                return otherTotal == 0 ? 0 : -1;
            } else if (otherTotal == 0) {
                // thisTotal is not 0.
                return 1;
            } else {
                // Start from the highest severity, and proceed to the lowest.
                for (int i = cntList.size() - 1; i >= 0; --i) {
                    // Compare the ratios of entities with the severity to total number of entities.
                    double thisRatio = cntList.getInt(i) / thisTotal;
                    double otherRatio = other.cntList.getInt(i) / otherTotal;
                    double diff = thisRatio - otherRatio;
                    if (diff > EPSILON) {
                        return 1;
                    } else if (diff < -EPSILON) {
                        return -1;
                    }
                }

                // If all the ratios are the same, compare the total amounts.
                return Double.compare(thisTotal, otherTotal);
            }
        }

        public void visitSeverityCounts(BiConsumer<Severity, Integer> countVisitor) {
            for (Severity severity : SEVERITIES) {
                int cnt = cntList.getInt(severity.ordinal());
                if (cnt > 0) {
                    countVisitor.accept(severity, cntList.getInt(severity.ordinal()));
                }
            }
        }
    }

    @Override
    public void entitySeveritiesRefresh(@Nonnull Collection<EntitiesWithSeverity> severityRefresh,
            @Nonnull final Map<Long, EntitySeverityNotification.SeverityBreakdown> severityBreakdowns) {
        final long cnt = severityRefresh.stream()
            .mapToLong(EntitiesWithSeverity::getOidsCount)
            .sum();

        // Not in danger of logspam, because this should only happen once per broadcast.
        logger.info("Received entity severity refresh with {} entities.", cnt);
        lock.writeLock().lock();
        try {
            nonDefaultSeveritiesByEntity.clear();
            this.severityBreakdowns.clear();
            insertSeverities(severityRefresh);
            insertSeverityBreakdowns(severityBreakdowns);
            nonDefaultSeveritiesByEntity.trim();
        } finally {
            lock.writeLock().unlock();
        }
        logger.info("Processed entity severity refresh with {} entities.", cnt);
    }

    private void insertSeverityBreakdowns(Map<Long, EntitySeverityNotification.SeverityBreakdown> severityBreakdowns) {
        severityBreakdowns.forEach((id, breakdown) -> {
            final SeverityBreakdown cacheBreakdown =
                    this.severityBreakdowns.computeIfAbsent(id, k -> new SeverityBreakdown());
            cacheBreakdown.updateSeverityCnt(breakdown.getCountsList());
        });
    }

    private void insertSeverities(@Nonnull final Collection<EntitiesWithSeverity> newSeverities) {
        newSeverities.forEach(entitiesWithSeverity -> {
            final Severity severity = entitiesWithSeverity.getSeverity();
            entitiesWithSeverity.getOidsList().forEach(oid -> {
                nonDefaultSeveritiesByEntity.put(oid, severity);
            });
        });
    }

    @Override
    public void entitySeveritiesUpdate(@Nonnull Collection<EntitiesWithSeverity> severityUpdate,
                                       @Nonnull Map<Long, EntitySeverityNotification.SeverityBreakdown> breakdownUpdate) {
        logger.debug("Received entity severity refresh with {} entities.",
            () -> severityUpdate.stream()
                .mapToLong(EntitiesWithSeverity::getOidsCount)
                .count());
        lock.writeLock().lock();
        try {
            insertSeverities(severityUpdate);
            insertSeverityBreakdowns(breakdownUpdate);
        } finally {
            lock.writeLock().unlock();
        }
        logger.debug("Processed entity severity refresh with {} entities.",
                () -> severityUpdate.stream()
                        .mapToLong(EntitiesWithSeverity::getOidsCount)
                        .count());
    }

    /**
     * Get the severity of an entity.
     *
     * @param entityId The OID of the entity.
     * @return The {@link Severity} associated with the entity. Note - for aggregate entities this
     *         will return "NORMAL"
     */
    @Nonnull
    public Severity getEntitySeverity(long entityId) {
        lock.readLock().lock();
        try {
            return nonDefaultSeveritiesByEntity.getOrDefault(entityId, Severity.NORMAL);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Method to sort a list of entities by severity.
     * This is different than manually sorting by the results of
     * {@link EntitySeverityClientCache#getEntitySeverity(long)} because it also takes severity
     * breakdowns into accounts, and does the sort transactionally (while holding the internal
     * cache lock).
     *
     * @param entityIds The collection of input OIDs. This collection is not modified.
     * @param ascending Whether to sort in ascending order (i.e. lowest severity first).
     * @return A list containing the input OIDs, sorted.
     */
    @Nonnull
    public LongList sortBySeverity(@Nonnull final Collection<Long> entityIds, final boolean ascending) {
        final LongList retList = new LongArrayList(entityIds);
        lock.readLock().lock();
        try {
            retList.sort(severityComparator(ascending));
            return retList;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get a comparator that orders oids currently in the cache by considering their severity
     * breakdown and entity level severity according to the following rules.
     * 1. An entity without severity and without severity breakdown
     * 2. An entity with severity but without severity breakdown
     * 3. An entity with severity but with empty severity breakdown map
     * 4. An entity with lowest severity break down
     * 5. An entity with same proportion, but a higher count of that severity
     * 6. An entity with the same highest severity, but the proportion is higher
     * 7. An entity with a higher severity in the breakdown.
     * 8. An entity with an even higher severity in the breakdown but the entity does not have a
     *    a severity (edge case).
     *
     * <p/>Note - the comparator must be used while holding at least a read lock!
     *
     * @param ascending Whether to return a comparator that sorts in ascending or descending order.
     * @return The {@link LongComparator} which can be used to sort OIDs.
     */
    @Nonnull
    private LongComparator severityComparator(final boolean ascending) {
        final LongComparator ret = (id1, id2) -> {
            SeverityBreakdown bk1 = severityBreakdowns.get(id1);
            SeverityBreakdown bk2 = severityBreakdowns.get(id2);
            final int result;
            if (bk1 == null && bk2 == null) {
                // Compare the actual severities
                result = nonDefaultSeveritiesByEntity.getOrDefault(id1, Severity.NORMAL)
                        .compareTo(nonDefaultSeveritiesByEntity.getOrDefault(id2, Severity.NORMAL));
            } else if (bk1 == null) {
                // Compare the severity of oid 1 to the highest severity in the breakdown.
                result = nonDefaultSeveritiesByEntity.getOrDefault(id1, Severity.NORMAL)
                        .compareTo(bk2.highestSeverity());
            } else if (bk2 == null) {
                result = bk1.highestSeverity().compareTo(
                        nonDefaultSeveritiesByEntity.getOrDefault(id2, Severity.NORMAL));
            } else {
                // Compare breakdowns.
                result = bk1.compareTo(bk2);
            }
            // For stable sort.
            return result == 0 ? Long.compare(id1, id2) : result;
        };
        return ascending ? ret : ret.reversed();
    }
}
