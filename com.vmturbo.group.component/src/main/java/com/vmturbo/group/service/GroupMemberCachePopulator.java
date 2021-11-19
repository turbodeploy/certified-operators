package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.opentracing.Tracer;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ShortMap;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongConsumer;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.oid.identity.RoaringBitmapOidSet;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * This calculates following three pieces of information and returns it:
 *
 * <p>1. All groups in the system and their immediate child members.</p>
 *
 * <p>2. All groups that each entity/group is immediate member of.</p>
 *
 * <p>3. The type of all groups in the system.</p>
 *
 * <p>Note: This operation can take longer in the bigger environments and group membership
 * may change from the time that operation started.</p>
 */
public class GroupMemberCachePopulator {
    private final Logger logger = LogManager.getLogger();

    private final GroupMemberCalculator internalCalculator;

    private final GroupDAO groupDAO;

    private final Supplier<CachedGroupMembers> cachedMemberFactory;

    private final boolean cacheEntityParentGroups;

    private GroupMemberCachePopulator(final GroupMemberCalculator groupMemberCalculator,
                                      final GroupDAO groupDAO,
                                      final Supplier<CachedGroupMembers> cachedMemberFactory,
                                      final boolean cacheEntityParentGroups) {
        this.internalCalculator = groupMemberCalculator;
        this.groupDAO = groupDAO;
        this.cachedMemberFactory = cachedMemberFactory;
        this.cacheEntityParentGroups = cacheEntityParentGroups;
    }

    /**
     * Calculates the group membership relationship.
     *
     * @param groupMemberCalculator   the group member calculator.
     * @param groupDAO                the data access object for groups.
     * @param cachedMemberFactory     the factory for creating member sets.
     * @param cacheParentGroups if true we also create a map from entities/groups to their parents.
     * @return an object containing group relationships.
     */
    public static GroupMembershipRelationships calculate(
            @Nonnull final GroupMemberCalculator groupMemberCalculator,
            @Nonnull final GroupDAO groupDAO,
            @Nonnull final Supplier<CachedGroupMembers> cachedMemberFactory,
            final boolean cacheParentGroups) {
        GroupMemberCachePopulator calculator =
                new GroupMemberCachePopulator(
                        Objects.requireNonNull(groupMemberCalculator),
                        Objects.requireNonNull(groupDAO), cachedMemberFactory,
                        cacheParentGroups);
        return calculator.calculate();
    }

    private GroupMembershipRelationships calculate() {
        final Tracer tracer = Tracing.tracer();
        try (DataMetricTimer timer = Metrics.REGROUPING_SUMMARY.startTimer();
             Tracing.TracingScope scope = Tracing.trace("regrouping", tracer)) {
            final Collection<GroupDTO.Grouping> groups;
            try {
                groups = groupDAO.getGroups(GroupDTO.GroupFilter.getDefaultInstance());
            } catch (DataAccessException e) {
                logger.error("Abandoning regrouping because the query for all groups failed.", e);
                return ImmutableGroupMembershipRelationships.builder()
                        .success(false)
                        .build();
            }

            final Long2ShortOpenHashMap groupToType = new Long2ShortOpenHashMap(groups.size());
            groups.forEach(g -> groupToType.put(g.getId(),
                    (short)g.getDefinition().getType().getNumber()));
            groupToType.trim();

            final LongSet distinctMembers = new LongOpenHashSet();
            long totalMemberCnt = 0;
            final Long2ObjectOpenHashMap<CachedGroupMembers> groupIdToMemberIds =
                    new Long2ObjectOpenHashMap<>(groups.size());
            final Long2ObjectOpenHashMap<LongSet> entityIdToParentGroupIds =
                    new Long2ObjectOpenHashMap<>();
            for (final GroupDTO.Grouping group : groups) {
                try {
                    // Get the group members
                    // Do not expand nested groups - we only put the direct members into the
                    // cache, and resolve nested groups recursively at query-time.
                    logger.trace("Getting members for group with id {}", () -> group.getId());
                    final Set<Long> groupMembers = internalCalculator
                            .getGroupMembers(groupDAO, group.getDefinition(), false);

                    // add group members to group members cache
                    final CachedGroupMembers cachedGroupMembers = cachedMemberFactory.get();
                    cachedGroupMembers.set(groupMembers);
                    groupIdToMemberIds.put(group.getId(), cachedGroupMembers);

                    // add entity to group parent relationship
                    if (cacheEntityParentGroups) {
                        for (long memberId : groupMembers) {
                            entityIdToParentGroupIds.computeIfAbsent(memberId, v -> new LongOpenHashSet())
                                    .add(group.getId());
                        }
                    }

                    cachedGroupMembers.get(distinctMembers::add);
                    totalMemberCnt += cachedGroupMembers.size();
                } catch (RuntimeException | StoreOperationException e) {
                    logger.error("An error occurred during regrouping for group with uuid: "
                            + group.getId() + ". Error: ", e);
                }
            }

            if (cacheEntityParentGroups) {
                // try to trim the members to reduce memory footprint
                entityIdToParentGroupIds.trim();
                entityIdToParentGroupIds.values().forEach(x -> ((LongOpenHashSet)x).trim());
            }


            final MemoryMeasurer.MemoryMeasurement memory = MemoryMeasurer.measure(groupIdToMemberIds);
            final MemoryMeasurer.MemoryMeasurement entityParentMemory = MemoryMeasurer.measure(entityIdToParentGroupIds);
            final MemoryMeasurer.MemoryMeasurement totalMemory = MemoryMeasurer.MemoryMeasurement.add(memory,
                    entityParentMemory);
            Metrics.REGROUPING_SIZE_SUMMARY.observe((double)totalMemory.getTotalSizeBytes());
            Metrics.REGROUPING_CNT_SUMMARY.observe((double)totalMemberCnt);
            Metrics.REGROUPING_DISTINCT_CNT_SUMMARY.observe((double)distinctMembers.size());
            logger.info("Completed regrouping in {} seconds. {} members ({} distinct entities)"
                            + " in {} groups. Cached members memory: {} Cached parents memory: {}",
                    timer.getTimeElapsedSecs(), totalMemberCnt, distinctMembers.size(),
                    groups.size(), memory, entityParentMemory);
            return ImmutableGroupMembershipRelationships.builder()
                    .success(true)
                    .resolvedGroupsIds(new LongOpenHashSet(groupToType.keySet()))
                    .totalMemberCount(totalMemberCnt)
                    .distinctEntitiesCount(distinctMembers.size())
                    .memoryMeasurement(memory)
                    .groupIdToGroupMemberIdsMap(groupIdToMemberIds)
                    .entityIdToGroupIdsMap(entityIdToParentGroupIds)
                    .groupIdToType(groupToType)
                    .build();
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
            public static Type fromString(String str) {
                for (Type t : values()) {
                    if (str.equalsIgnoreCase(t.name())) {
                        return t;
                    }
                }
                // SET is the default.
                return Type.SET;
            }

            @Nonnull
            public Supplier<CachedGroupMembers> getFactory() {
                return factory;
            }
        }
    }

    /**
     * The wrapper object for result of regroup calculation.
     */
    @Value.Immutable
    public interface GroupMembershipRelationships {
        /**
         * If the operation was successful.
         *
         * @return if the operation was successful.
         */
        boolean getSuccess();

        /**
         * The of all group ids that were processed.
         *
         * @return the group ids.
         */
        @Nullable
        LongSet getResolvedGroupsIds();

        /**
         * The total member count of groups members.
         *
         * @return the total count of group members.
         */
        @Value.Default
        default long getTotalMemberCount() {
            return 0;
        }

        /**
         * The total number of distinct entities in the groups.
         *
         * @return total number of distinct entities in the groups.
         */
        @Value.Default
        default int getDistinctEntitiesCount() {
            return 0;
        }

        /**
         * The amount of memory required for keeping the result of operation.
         *
         * @return the required memory.
         */
        @Nullable
        MemoryMeasurer.MemoryMeasurement getMemoryMeasurement();

        /**
         * The map from group ids to the ids of its members.
         *
         * @return the group members map.
         */
        @Nullable
        Long2ObjectMap<CachedGroupMembers> getGroupIdToGroupMemberIdsMap();

        /**
         * The map from entities ids to their parent groups ids.
         *
         * @return the map from entities ids to their parent groups ids.
         */
        @Nullable
        Long2ObjectMap<LongSet> getEntityIdToGroupIdsMap();

        /**
         * The map from group ids to the number representing their types.
         *
         * @return the map from group to their types.
         */
        @Nullable
        Long2ShortMap getGroupIdToType();
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
