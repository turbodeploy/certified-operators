package com.vmturbo.group.service;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;

import io.opentracing.Tracer;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongConsumer;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer.MemoryMeasurement;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.identity.RoaringBitmapOidSet;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.GroupUpdateListener;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * A {@link GroupMemberCalculator} that delegates to an internal {@link GroupMemberCalculator},
 * but caches results - either eagerly or lazily.
 */
public class CachingMemberCalculator implements GroupMemberCalculator, RepositoryListener, GroupUpdateListener {

    private static final CachedGroupMembers EMPTY_GROUP = new CachedGroupMembers() {
        @Override
        public boolean get(LongConsumer consumer) {
            return false;
        }

        @Override
        public void set(Collection<Long> members) {
        }

        @Override
        public int size() {
            return 0;
        }
    };

    private final Logger logger = LogManager.getLogger();

    private final GroupMemberCalculator internalCalculator;

    private final GroupDAO groupDAO;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @GuardedBy("lock")
    private final Long2ObjectMap<CachedGroupMembers> cachedMembers = new Long2ObjectOpenHashMap<>();

    /**
     * A set of all currently known group IDs.
     */
    @GuardedBy("lock")
    private final LongOpenHashSet allGroupIds = new LongOpenHashSet();

    private final Supplier<CachedGroupMembers> cachedMemberFactory;

    /**
     * If true, we eagerly resolve all groups every time a new topology is available in the
     * repository.
     *
     * <p/>If false, we cache groups only when they are queried.
     */
    private final boolean doRegrouping;

    CachingMemberCalculator(@Nonnull final GroupDAO groupDAO,
            @Nonnull final GroupMemberCalculator internalCalculator,
            @Nonnull final CachedGroupMembers.Type memberCacheType,
            final boolean doRegrouping) {
        this(groupDAO, internalCalculator, memberCacheType, doRegrouping, Thread::new);
    }

    @VisibleForTesting
    CachingMemberCalculator(@Nonnull final GroupDAO groupDAO,
                @Nonnull final GroupMemberCalculator internalCalculator,
                @Nonnull final CachedGroupMembers.Type memberCacheType,
                final boolean doRegrouping,
                BiFunction<Runnable, String, Thread> threadFactory) {
        this.groupDAO = groupDAO;
        this.internalCalculator = internalCalculator;
        this.doRegrouping = doRegrouping;
        this.cachedMemberFactory = memberCacheType.getFactory();

        // When the group component comes up, we try to do regrouping to initialize the cache.
        threadFactory.apply(() -> {
            try {
                RetriableOperation.newOperation(this::regroup)
                        .retryOnOutput(success -> !success)
                        .run(10, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Regrouping thread interrupted.", e);
            } catch (RetriableOperationFailedException e) {
                logger.error("Regrouping on initialization failed.", e);
            } catch (TimeoutException e) {
                logger.error("Timed out trying to successfully regroup on initialization.", e);
            }
        }, "regrouping-initialization").start();
    }

    private boolean regroup() {
        // Invert the conditions because the "not" case is very short.
        if (!doRegrouping) {
            // We need to know all the group ids even if we're not doing regrouping, because we use
            // them when expanding nested groups.
            final Set<Long> allGroups;
            try {
                allGroups = groupDAO.getGroupIds(GroupFilters.getDefaultInstance());
            } catch (DataAccessException e) {
                logger.error("Failed to fetch all group ids.", e);
                return false;
            } finally {
                // Clear any cached members every time we regroup topology.
                lock.writeLock().lock();
                try {
                    cachedMembers.clear();
                } finally {
                    lock.writeLock().unlock();
                }
            }

            // Replace the group ids.
            lock.writeLock().lock();
            try {
                allGroupIds.clear();
                allGroupIds.addAll(allGroups);
                allGroupIds.trim();
                return true;
            } finally {
                lock.writeLock().unlock();
            }
        } else {
            final Tracer tracer = Tracing.tracer();
            try (DataMetricTimer timer = Metrics.REGROUPING_SUMMARY.startTimer();
                 TracingScope scope = Tracing.trace("regrouping", tracer)) {
                final LongSet distinctMembers = new LongOpenHashSet();
                long totalMemberCnt = 0;
                final Collection<Grouping> groups;
                try {
                    groups = groupDAO.getGroups(GroupDTO.GroupFilter.getDefaultInstance());
                } catch (DataAccessException e) {
                    logger.error("Abandoning regrouping because the query for all groups failed.", e);
                    return false;
                } finally {
                    // Clear all cached members even if there was an error.
                    // We don't need to clear the "allGroupIds" in the error case because we will
                    // not reassign the ids of previously-existing groups to new non-group objects.
                    lock.writeLock().lock();
                    try {
                        cachedMembers.clear();
                    } finally {
                        lock.writeLock().unlock();
                    }
                }

                // Record all the group IDs.
                lock.writeLock().lock();
                try {
                    allGroupIds.clear();
                    groups.forEach(g -> allGroupIds.add(g.getId()));
                    allGroupIds.trim();
                } finally {
                    lock.writeLock().unlock();
                }

                for (final Grouping group : groups) {
                    // Before resolving the group, check to make sure it's not already cached.
                    // A concurrent request may have triggered the computation + caching of the
                    // group's members.
                    lock.readLock().lock();
                    try {
                        if (cachedMembers.containsKey(group.getId())) {
                            continue;
                        }
                    } finally {
                        lock.readLock().unlock();
                    }

                    // Do not expand nested groups - we only put the direct members into the
                    // cache, and resolve nested groups recursively at query-time.
                    try {
                        final CachedGroupMembers m = cachedMemberFactory.get();
                        m.set(getGroupMembers(groupDAO, group.getDefinition(), false));
                        lock.writeLock().lock();
                        try {
                            // There are two ways the group may find itself in the cache:
                            // 1) There was a concurrent external query for the group's members, and the
                            //    results were cached. In this case it's safe to keep the cached results.
                            // 2) There was a concurrent update to an existing group, and the
                            //    new members were eagerly cached. In this case it would be wrong to
                            //    overwrite.
                            this.cachedMembers.putIfAbsent(group.getId(), m);
                        } finally {
                            lock.writeLock().unlock();
                        }

                        m.get(distinctMembers::add);
                        totalMemberCnt += m.size();
                    } catch (StoreOperationException e) {
                        logger.error("Failed to do regrouping. Error: ", e);
                    }
                }

                final MemoryMeasurement memory;
                lock.readLock().lock();
                try {
                    memory = MemoryMeasurer.measure(cachedMembers);
                } finally {
                    lock.readLock().unlock();
                }
                Metrics.REGROUPING_SIZE_SUMMARY.observe((double)memory.getTotalSizeBytes());
                Metrics.REGROUPING_CNT_SUMMARY.observe((double)totalMemberCnt);
                Metrics.REGROUPING_DISTINCT_CNT_SUMMARY.observe((double)distinctMembers.size());
                logger.info("Completed regrouping in {} seconds. {} members ({} distinct entities)"
                                + " in {} groups. Cached members memory: {}", timer.getTimeElapsedSecs(),
                        totalMemberCnt, distinctMembers.size(), groups.size(), memory);
                return true;
            }
        }
    }

    @Override
    public void onSourceTopologyAvailable(long topologyId, long topologyContextId) {
        // TODO (roman, Oct 14 2020: - if we get a flurry of "topology available" messages (e.g.
        // reading from the start of the topic) we should skip to the last one. This will also
        // let us avoid infinite staleness if regrouping somehow becomes slower than topology
        // ingestion.
        regroup();
    }

    @Nonnull
    @Override
    public Set<Long> getGroupMembers(@Nonnull IGroupStore groupStore,
            @Nonnull Collection<Long> groupIds, boolean expandNestedGroups)
            throws StoreOperationException {
        final Set<Long> retMembers = new HashSet<>();

        LongSet groupsToExpand = new LongOpenHashSet();

        groupsToExpand.addAll(groupIds);

        // We only cache groups with expandNestedGroups == false, and do the expansion here
        // recursively-iteratively.
        while (!groupsToExpand.isEmpty()) {
            // This set will only be used when expanding a group-of-groups (e.g. group of clusters).
            final LongSet nextGroupsToExpand = new LongOpenHashSet(1);
            // Use this function whenever processing a direct member.
            // If a direct member is a group AND we want to expand nested groups,
            // record that member id separately for the next round of expansion.
            final LongConsumer memberIdConsumer = (directMemberId) -> {
                if (expandNestedGroups && allGroupIds.contains(directMemberId)) {
                    nextGroupsToExpand.add(directMemberId);
                } else {
                    retMembers.add(directMemberId);
                }
            };

            // Iterate through the "groupsToExpand", resolving any cache hits and removing those
            // group ids from the set. At the end of the iteration the set will only contain
            // group ids not in the cache.
            lock.readLock().lock();
            try {
                final LongIterator groupIt = groupsToExpand.iterator();
                while (groupIt.hasNext()) {
                    final long groupId = groupIt.nextLong();
                    final CachedGroupMembers cachedUnexpandedGroups = this.cachedMembers.getOrDefault(groupId, EMPTY_GROUP);
                    final boolean present = cachedUnexpandedGroups.get(memberIdConsumer);

                    // If there was a cache hit, we remove this group ID.
                    if (present) {
                        groupIt.remove();
                    }
                }
            } finally {
                lock.readLock().unlock();
            }

            // We removed all cached groups from the set. Now we query for the non-cached groups.
            if (!groupsToExpand.isEmpty()) {
                // Do not expand nested groups via the internal calculator to utilize any cached
                // sub-groups.
                final Set<Long> restOfDirectMembers = internalCalculator.getGroupMembers(groupStore, groupsToExpand, false);
                // We can only populate the cache if we are getting members for a single group. Otherwise we don't
                // know which of the returned members belong to which group.
                if (groupsToExpand.size() == 1) {
                    CachedGroupMembers cachedMembers = cachedMemberFactory.get();
                    cachedMembers.set(restOfDirectMembers);
                    lock.writeLock().lock();
                    try {
                        this.cachedMembers.put(groupsToExpand.iterator().nextLong(), cachedMembers);
                    } finally {
                        lock.writeLock().unlock();
                    }
                }
                restOfDirectMembers.forEach(memberIdConsumer);
            }

            // Swap in the set of nested groups to expand.
            groupsToExpand = nextGroupsToExpand;
        }
        return retMembers;
    }

    @Nonnull
    @Override
    public Set<Long> getGroupMembers(@Nonnull IGroupStore groupStore,
            @Nonnull GroupDefinition groupDefinition, boolean expandNestedGroups)
            throws StoreOperationException {
        return internalCalculator.getGroupMembers(groupStore, groupDefinition, expandNestedGroups);
    }

    @Override
    public void onUserGroupCreated(final long createdGroup, @Nonnull final GroupDefinition groupDefinition) {
        if (doRegrouping) {
            cacheGroupMembers(createdGroup, groupDefinition);
        }
    }

    private void cacheGroupMembers(final long groupId, @Nonnull final GroupDefinition groupDefinition) {
        lock.writeLock().lock();
        try {
            // Ensure the group ID is known.
            allGroupIds.add(groupId);
            CachedGroupMembers m = cachedMembers.computeIfAbsent(groupId, k -> cachedMemberFactory.get());
            m.set(internalCalculator.getGroupMembers(groupDAO, groupDefinition, false));
        } catch (StoreOperationException e) {
            logger.error("Failed to eagerly populate members of group {} (id: {})",
                groupDefinition.getDisplayName(), groupId, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void remove(final long groupId) {
        lock.writeLock().lock();
        try {
            allGroupIds.remove(groupId);
            cachedMembers.remove(groupId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void onUserGroupDeleted(final long groupId) {
        remove(groupId);
    }

    @Override
    public void onUserGroupUpdated(final long updatedGroup, @Nonnull final GroupDefinition groupDefinition) {
        if (doRegrouping) {
            cacheGroupMembers(updatedGroup, groupDefinition);
        } else {
            remove(updatedGroup);
        }
    }

    /**
     * Cached members of a group. Interface exists to support swapping in different implementations.
     */
    interface CachedGroupMembers {
        boolean get(LongConsumer consumer);

        void set(Collection<Long> members);

        int size();

        /**
         * The type of the implementation, used for a more user-friendly configuration interface
         * for the {@link CachingMemberCalculator}.
         */
        enum Type {
            /**
             * Backed by a fastutil LongSet.
             */
            SET(FastUtilCachedGroupMembers::new),

            /**
             * Backed by a RoaringNavigableBitmap.
             */
            BITMAP(BitmapCachedGroupMembers::new);

            private final Supplier<CachedGroupMembers> factory;

            Type(Supplier<CachedGroupMembers> factory) {
                this.factory = factory;
            }

            @Nonnull
            public Supplier<CachedGroupMembers> getFactory() {
                return factory;
            }

            @Nonnull
            public static Type fromString(String str) {
                for (Type t : values()) {
                    if (str.equalsIgnoreCase(t.name())) {
                        return t;
                    }
                }
                // SET is the default.
                return Type.SET;
            }
        }
    }

    /**
     * {@link CachedGroupMembers} backed by a {@link RoaringBitmapOidSet}, which may compress
     * better.
     */
    private static class BitmapCachedGroupMembers implements CachedGroupMembers {
        private RoaringBitmapOidSet members = null;

        @Override
        public boolean get(LongConsumer consumer) {
            if (members != null) {
                members.iterator().forEachRemaining((java.util.function.LongConsumer)consumer);
                return true;
            }
            return false;
        }

        @Override
        public void set(Collection<Long> members) {
            this.members = new RoaringBitmapOidSet(members);
        }

        @Override
        public int size() {
            return members == null ? 0 : members.size();
        }

    }

    /**
     * {@link CachedGroupMembers} implementation that uses the fastutil library to store
     * members in a primitive set.
     */
    private static class FastUtilCachedGroupMembers implements CachedGroupMembers {

        private LongSet members = null;

        @Override
        public boolean get(LongConsumer consumer) {
            if (members != null) {
                members.forEach((java.util.function.LongConsumer)consumer);
                return true;
            }
            return false;
        }

        @Override
        public void set(Collection<Long> members) {
            // No need to "trim" because we initialize directly from the collection.
            this.members = new LongOpenHashSet(members);
        }

        @Override
        public int size() {
            return members == null ? 0 : members.size();
        }
    }

    /**
     * Metrics for the class.
     */
    private static class Metrics {
        /**
         * This metric tracks the total duration of a topology broadcast (i.e. all the stages
         * in the pipeline).
         */
        private static final DataMetricSummary REGROUPING_SUMMARY =
            DataMetricSummary.builder().withName("group_regrouping_duration_seconds").withHelp(
                "Duration to repopulate the group member cache.").build().register();

        private static final DataMetricSummary REGROUPING_SIZE_SUMMARY =
            DataMetricSummary.builder().withName("group_regrouping_cache_size_bytes").withHelp(
                "Size of the cache after regrouping, in bytes.").build().register();

        private static final DataMetricSummary REGROUPING_CNT_SUMMARY =
            DataMetricSummary.builder().withName("group_regrouping_total_members_count")
                .withHelp("Number of cached group members")
                .build().register();

        private static final DataMetricSummary REGROUPING_DISTINCT_CNT_SUMMARY =
            DataMetricSummary.builder().withName("group_regrouping_distinct_members_count")
                    .withHelp("Total number of entities that appear in some groups.")
                    .build().register();
    }
}
