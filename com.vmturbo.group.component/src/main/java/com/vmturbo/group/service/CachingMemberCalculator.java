package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ShortMap;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongConsumer;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer.MemoryMeasurement;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.GroupUpdateListener;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.service.GroupMemberCachePopulator.CachedGroupMembers;
import com.vmturbo.group.service.GroupMemberCachePopulator.GroupMembershipRelationships;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * A {@link GroupMemberCalculator} that delegates to an internal {@link GroupMemberCalculator},
 * but caches results - either eagerly or lazily.
 */
public class CachingMemberCalculator implements GroupMemberCalculator, GroupUpdateListener {

    /**
     * An instance of CachedGroupMembers that represents empty members.
     */
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

    /**
     * The lock object used to make sure one re-group operation happen at a time.
     */
    private final Object regroupLock = new Object();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * The map from group ids to their cached immediate members list.
     */
    @GuardedBy("lock")
    private Long2ObjectMap<CachedGroupMembers> groupIdToCachedMembers =
        new Long2ObjectOpenHashMap<>();

    /**
     * The map from entities to their immediate parent groups.
     */
    @GuardedBy("lock")
    private Long2ObjectMap<LongSet> memberIdToParentGroupIds =
        new Long2ObjectOpenHashMap<>();

    /**
     * A map from group ids to their types.
     */
    @GuardedBy("lock")
    private Long2ShortMap groupToType = new Long2ShortOpenHashMap();

    /**
     * This queue is used for saving group changes during a regroup operation. The list
     * of changes are then applied on the membership relationships calculated during regroup.
     */
    @GuardedBy("lock")
    private final Queue<GroupChange> groupChanges = new LinkedList<>();

    /**
     * This is true when regroup operation is in progress.
     */
    private boolean isRegroupRunning = false;

    private final boolean cacheParentGroups;

    private final Supplier<CachedGroupMembers> cachedMemberFactory;


    private final Supplier<GroupMembershipRelationships> membershipRelationshipsSupplier;

    CachingMemberCalculator(@Nonnull final GroupDAO groupDAO,
            @Nonnull final GroupMemberCalculator internalCalculator,
            @Nonnull final CachedGroupMembers.Type memberCacheType,
            boolean cacheParentGroups) {
        this(groupDAO, internalCalculator, memberCacheType, cacheParentGroups, Thread::new,
                () -> GroupMemberCachePopulator.calculate(internalCalculator,
                        groupDAO, memberCacheType.getFactory(), cacheParentGroups));
    }

    @VisibleForTesting
    CachingMemberCalculator(@Nonnull final GroupDAO groupDAO,
            @Nonnull final GroupMemberCalculator internalCalculator,
            @Nonnull final CachedGroupMembers.Type memberCacheType,
            final boolean cacheParentGroups,
            BiFunction<Runnable, String, Thread> threadFactory,
            Supplier<GroupMembershipRelationships> membershipRelationshipsSupplier) {
        this.groupDAO = groupDAO;
        this.internalCalculator = internalCalculator;
        this.cacheParentGroups = cacheParentGroups;
        this.cachedMemberFactory = memberCacheType.getFactory();
        this.membershipRelationshipsSupplier = membershipRelationshipsSupplier;

        // When the group component comes up, we try to do regrouping to initialize the cache.
        threadFactory.apply(() -> {
            try {
                RetriableOperation.newOperation(this::regroup)
                        .retryOnOutput(r -> !r.isSuccessfull())
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

    /**
     * Returns the uuids of the groups that currently exist in cache.
     *
     * @return the uuids of the cache's current groups.
     */
    public LongSet getCachedGroupIds() {
        lock.readLock().lock();
        try {
            return new LongOpenHashSet(groupToType.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Perform regrouping, clearing the cache and recalculating group members for all groups.
     *
     * @return a summary of the result. Includes a flag on whether regrouping was successful, and in
     *         case it was, the ids of the groups whose members were resolved as well as some
     *         statistics on the cache (group/entities counts & memory size). If regrouping failed,
     *         then only the successful flag carries meaningful information.
     */
    public RegroupingResult regroup() {
        // At this time, we only support one regroup operation running at a time.
        // The reason for that is that the regroup operation is unblocking, which means
        // group create/update/delete can update the cache. We need to make sure that
        // after getting the new group relationships, we apply the changes that happened during
        // regroup to the result cache. Managing these changes for multiple regroup is hard and
        // introduces additional complexity that we don't need.
        synchronized (regroupLock) {
            lock.writeLock().lock();
            try {
                isRegroupRunning = true;
                groupChanges.clear();
            } finally {
                lock.writeLock().unlock();
            }

            final GroupMembershipRelationships updatedMembershipsResult =
                    membershipRelationshipsSupplier.get();

            final RegroupingResult result;

            // replace the old cache with the new one. There co
            lock.writeLock().lock();
            try {
                if (updatedMembershipsResult.getSuccess()) {
                    groupIdToCachedMembers = updatedMembershipsResult.getGroupIdToGroupMemberIdsMap();
                    memberIdToParentGroupIds = updatedMembershipsResult.getEntityIdToGroupIdsMap();
                    groupToType = updatedMembershipsResult.getGroupIdToType();
                    // Re-apply the changes that happens during regroup to the cache. This makes
                    // sure that the changes that are made to the user groups during regroup
                    // operation are  also applied to the replacement caches.
                    // Let's give an example. Assume, we have a user group A with members [1, 2].
                    // This means we have the following state when the regroup operation starts:
                    // A [1,2]  CACHE [1, 2] GROUPS MEMBERS READ FROM DB FOR REGROUP [1, 2]
                    // DATABASE [1, 2]
                    // Let's assume a user updates group A during this period so A now has [3, 4].
                    // Since the lock is released the CACHE and DATABASE will get update. However,
                    // groups read from DB for regrouping will not this change.
                    // We add this change to group changes queue.
                    // A [3, 4] CACHE [3, 4] GROUPS MEMBERS READ FROM DB FOR REGROUP [1, 2]
                    // DATABASE [3, 4] GROUP_CHANGE [ A -> [3, 4]]
                    // After, the groups members for all groups are retrieved, we will have a wrong
                    // value for group A if we simply use the values come from regroup operation.
                    // A [3, 4] CACHE [1, 2] GROUPS MEMBERS READ FROM DB FOR REGROUP N/A
                    // DATABASE [3, 4] GROUP_CHANGE [A -> [3, 4]]
                    // Therefore we apply the changes stored in groups changes queue on the cache
                    // A [3, 4] CACHE [3, 4] GROUPS MEMBERS READ FROM DB FOR REGROUP N/A
                    // DATABASE [3, 4] GROUP_CHANGE []
                    // the readers will not see the transient state where cache has outdated value
                    // since the writing lock is acquired during the last two operations.
                    // There is this possibility that of the effect of the change we saved to
                    // group changes queue has been already reflected in the members read from db.
                    // That should be fine as the application of group changes are idempotent
                    // so applying one change twice does not break anything.
                    logger.debug("There are {} changes to apply.", () -> groupChanges.size());
                    while (!groupChanges.isEmpty()) {
                        final GroupChange change = groupChanges.remove();
                        logger.trace("Applying change {} to cache.", change);
                        switch (change.getType()) {
                            case CREATE:
                            case UPDATE:
                                cacheGroupMembers(change.getId(), change.getGroupType(),
                                        change.getMembers());
                                break;
                            case DELETE:
                                remove(change.getId());
                                break;
                            default:
                                throw new IllegalStateException("Unexpected group change type "
                                        + change.getType());
                        }
                    }

                    result = new RegroupingResult(true,
                            updatedMembershipsResult.getResolvedGroupsIds(),
                            updatedMembershipsResult.getTotalMemberCount(),
                            updatedMembershipsResult.getDistinctEntitiesCount(),
                            updatedMembershipsResult.getMemoryMeasurement());
                } else {
                    groupChanges.clear();
                    result = new RegroupingResult(false, null, 0, 0, null);
                }
            } finally {
                isRegroupRunning = false;
                lock.writeLock().unlock();
            }
            return result;
        }
    }

    @Nonnull
    @Override
    public Set<Long> getGroupMembers(@Nonnull IGroupStore groupStore,
            @Nonnull Collection<Long> groupIds, boolean expandNestedGroups)
            throws StoreOperationException {
        final Set<Long> retMembers = new HashSet<>();

        LongSet groupsToExpand = new LongOpenHashSet();
        LongSet visitedGroups = new LongOpenHashSet();


        groupsToExpand.addAll(groupIds);
        visitedGroups.addAll(groupIds);

        // We only cache groups with expandNestedGroups == false, and do the expansion here
        // recursively-iteratively.
        while (!groupsToExpand.isEmpty()) {
            // This set will only be used when expanding a group-of-groups (e.g. group of clusters).
            final LongSet nextGroupsToExpand = new LongOpenHashSet(1);
            // Use this function whenever processing a direct member. The visited groups set
            // prevent us from iterating over the same group twice.
            // If a direct member is a group AND we want to expand nested groups,
            // record that member id separately for the next round of expansion.
            final LongConsumer memberIdConsumer = (directMemberId) -> {
                if (expandNestedGroups && groupToType.containsKey(directMemberId) && !visitedGroups.contains(directMemberId)) {
                    nextGroupsToExpand.add(directMemberId);
                    visitedGroups.add(directMemberId);
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
                    final CachedGroupMembers cachedUnexpandedGroups = this.groupIdToCachedMembers.getOrDefault(groupId, EMPTY_GROUP);
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
                        this.groupIdToCachedMembers.put(groupsToExpand.iterator().nextLong(), cachedMembers);
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
    @Nonnull
    public Map<Long, Set<Long>> getEntityGroups(@Nonnull IGroupStore groupStore,
                    @Nonnull Set<Long> entityIds,
                    @Nonnull Set<GroupType> groupTypes) throws StoreOperationException {
        if (cacheParentGroups) {
            final Set<Short> types = groupTypes
                .stream()
                .map(GroupType::getNumber)
                .map(Integer::shortValue)
                .collect(Collectors.toSet());

            final Map<Long, Set<Long>> result = new HashMap<>();

            lock.readLock().lock();
            try {
                for (long entityId : entityIds) {
                    final LongSet groups = memberIdToParentGroupIds.get(entityId);
                    if (groups != null) {
                        final Set<Long> filteredGroup = groups.stream()
                                .filter(e -> types.isEmpty() || types.contains(groupToType.get((long)e)))
                                .collect(Collectors.toSet());
                        result.put(entityId, filteredGroup);
                    } else {
                        result.put(entityId, Collections.emptySet());
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
            return result;
        } else {
            return internalCalculator.getEntityGroups(groupStore, entityIds, groupTypes);
        }
    }

    @Nullable
    private Set<Long> cacheGroupMembers(final long groupId, @Nonnull final GroupDefinition groupDefinition) {
        try {
            final Set<Long> groupMembers = internalCalculator
                .getGroupMembers(groupDAO, groupDefinition, false);
            cacheGroupMembers(groupId, groupDefinition.getType(), groupMembers);
            return groupMembers;
        } catch (StoreOperationException e) {
            logger.error("Failed to eagerly populate members of group {} (id: {})",
                groupDefinition.getDisplayName(), groupId, e);
            return null;
        }
    }

    private void cacheGroupMembers(final long groupId, GroupType groupType,
                                   @Nonnull final Set<Long> groupMembers) {
        // Ensure the group ID is known.
        groupToType.put(groupId, (short)groupType.getNumber());
        CachedGroupMembers cachedGroupMembers = groupIdToCachedMembers.computeIfAbsent(groupId,
                k -> cachedMemberFactory.get());

        // updating entity parents logic
        if (cacheParentGroups) {
            final Set<Long> newMembers = new HashSet<>(groupMembers);
            final Set<Long> removedMembers = new HashSet<>();
            cachedGroupMembers.get(removedMembers::add);
            // new members are those members are in not in current set of members
            newMembers.removeAll(removedMembers);
            // removed members are those that are in old groups members and not in the new
            // members
            removedMembers.removeAll(groupMembers);
            // add new members
            for (long memberId : newMembers) {
                memberIdToParentGroupIds.computeIfAbsent(memberId, v -> new LongOpenHashSet()).add(groupId);
            }
            // remove members
            removedMembers.forEach(memberId -> removeParentRelationship(groupId, memberId));
        }
        cachedGroupMembers.set(groupMembers);
    }

    private void remove(final long groupId) {
        if (cacheParentGroups) {
            final CachedGroupMembers cachedGroupMembers = groupIdToCachedMembers.get(groupId);
            if (cachedGroupMembers != null) {
                cachedGroupMembers.get(memberId -> removeParentRelationship(groupId, memberId));
            }
        }
        groupToType.remove(groupId);
        groupIdToCachedMembers.remove(groupId);
    }

    private void removeParentRelationship(long groupId, long removedMemberId) {
        LongSet groups = memberIdToParentGroupIds.get(removedMemberId);
        if (groups != null) {
            groups.remove(groupId);
            // if there is no more parent just remove the entry
            if (groups.isEmpty()) {
                memberIdToParentGroupIds.remove(removedMemberId);
            }
        }
    }

    @Nonnull
    @Override
    public Collection<Long> getEmptyGroupIds(@Nonnull IGroupStore groupStore) {
        lock.readLock().lock();
        try {
            return groupIdToCachedMembers.long2ObjectEntrySet().stream()
                    .filter(e -> e.getValue().size() == 0)
                    .map(Entry::getLongKey)
                    .collect(Collectors.toSet());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void onUserGroupCreated(final long createdGroup, @Nonnull final GroupDefinition groupDefinition) {
        lock.writeLock().lock();
        try {
            Set<Long> members = cacheGroupMembers(createdGroup, groupDefinition);
            if (isRegroupRunning) {
                groupChanges.add(GroupChange.create(createdGroup, groupDefinition.getType(), members));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void onUserGroupUpdated(final long updatedGroup, @Nonnull final GroupDefinition groupDefinition) {
        lock.writeLock().lock();
        try {
            Set<Long> members = cacheGroupMembers(updatedGroup, groupDefinition);
            if (isRegroupRunning) {
                groupChanges.add(GroupChange.update(updatedGroup, groupDefinition.getType(), members));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void onUserGroupDeleted(final long groupId) {
        lock.writeLock().lock();
        try {
            if (isRegroupRunning) {
                groupChanges.add(GroupChange.delete(groupId));
            }
            remove(groupId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Wrapper for the result of regrouping.
     */
    public static class RegroupingResult {
        private final boolean success;
        private final LongSet resolvedGroupsIds;
        private final long totalMemberCount;
        private final int distinctEntitiesCount;
        private final MemoryMeasurement memory;

        /**
         * Constructor.
         *
         * @param success flag to indicate whether regrouping finished successfully or not.
         * @param resolvedGroupsIds the ids of the groups whose members were resolved.
         * @param totalMemberCount the total number of members, aggregating all groups.
         * @param distinctEntitiesCount total number of distinct entities that are members of a
         *                              group.
         * @param memory measures the size of the cache.
         */
        public RegroupingResult(final boolean success, final LongSet resolvedGroupsIds,
                final long totalMemberCount, final int distinctEntitiesCount,
                final MemoryMeasurement memory) {
            this.success = success;
            this.resolvedGroupsIds = resolvedGroupsIds;
            this.totalMemberCount = totalMemberCount;
            this.distinctEntitiesCount = distinctEntitiesCount;
            this.memory = memory;
        }

        public boolean isSuccessfull() {
            return success;
        }

        public LongSet getResolvedGroupsIds() {
            return resolvedGroupsIds;
        }

        public long getTotalMemberCount() {
            return totalMemberCount;
        }

        public int getDistinctEntitiesCount() {
            return distinctEntitiesCount;
        }

        public MemoryMeasurement getMemory() {
            return memory;
        }
    }

    /**
     * This keeps information about changes to a group.
     */
    private static class GroupChange {
        final GroupChangeType type;
        final long id;
        final Set<Long> members;
        final GroupType groupType;

        private GroupChange(@Nonnull final GroupChangeType type,
                            final long id,
                            @Nullable final GroupType groupType,
                            @Nullable final Set<Long> members) {
            this.type = Objects.requireNonNull(type);
            this.id = id;
            this.members = members;
            this.groupType = groupType;
        }

        public static GroupChange create(final long id, final GroupType groupType, final Set<Long> members) {
            return new GroupChange(GroupChangeType.CREATE, id, Objects.requireNonNull(groupType),
                    Objects.requireNonNull(members));
        }

        public static GroupChange update(final long id, final GroupType groupType, final Set<Long> members) {
            return new GroupChange(GroupChangeType.UPDATE, id, Objects.requireNonNull(groupType),
                    Objects.requireNonNull(members));
        }

        public static GroupChange delete(final long id) {
            return new GroupChange(GroupChangeType.DELETE, id, null, null);
        }

        @Nonnull
        GroupChangeType getType() {
            return type;
        }

        long getId() {
            return id;
        }

        @Nullable
        Set<Long> getMembers() {
            return members;
        }

        @Nullable
        public GroupType getGroupType() {
            return groupType;
        }

        @Override
        public String toString() {
            return "GroupChange{"
                    + "type=" + type
                    + ", id=" + id
                    + ", members=" + members
                    + "}";
        }

    }

    /**
     * The type of group change.
     */
    private enum GroupChangeType {
        CREATE, UPDATE, DELETE
    }
}
