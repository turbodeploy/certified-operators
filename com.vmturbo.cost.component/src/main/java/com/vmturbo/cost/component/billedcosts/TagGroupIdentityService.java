package com.vmturbo.cost.component.billedcosts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collector;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;
import com.vmturbo.cost.component.billed.cost.tag.Tag;
import com.vmturbo.cost.component.billed.cost.tag.TagGroupIdentity;
import com.vmturbo.platform.sdk.common.CostBilling;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;
import com.vmturbo.sql.utils.DbException;

/**
 * Responsible for resolving identities for discovered tag groups by retrieving previously stored tag group ids from the
 * tag grouping table or generating new tag group ids and storing them into the tag grouping table.
 */
public class TagGroupIdentityService {

    private static final Logger logger = LogManager.getLogger();

    private final TagGroupStore tagGroupStore;

    private final IdentityProvider identityProvider;

    private final TagIdentityService tagIdentityService;

    private final Map<LongSet, TagGroupIdentity> tagGroupIdentityCache = new HashMap<>();

    private final AtomicBoolean cacheInitialized = new AtomicBoolean(false);

    private final int batchSize;

    /**
     * Creates an instance of CostTagGroupIdentityResolver.
     *
     * @param tagGroupStore for retrieving / writing into cost tag grouping table.
     * @param tagIdentityService for retrieving tag ids.
     * @param identityProvider for generating ids for new tag groups.
     * @param batchSize num records inserted per batch.
     */
    public TagGroupIdentityService(@Nonnull TagGroupStore tagGroupStore,
                                   @Nonnull TagIdentityService tagIdentityService,
                                   @Nonnull IdentityProvider identityProvider,
                                   int batchSize) {
        this.tagGroupStore = Objects.requireNonNull(tagGroupStore);
        this.tagIdentityService = Objects.requireNonNull(tagIdentityService);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.batchSize = batchSize;
    }

    /**
     * Returns a map from discovered tag group id to tag group id that is stored in tag grouping table. For tag groups
     * seen before, the tag group id is retrieved from the tagGroupIdentityCache by looking by tag members. For new tag
     * groups, a new tag group id is generated via IdentityProvider and then stored in the cost tag grouping table with
     * the members.
     *
     * @param discoveredTagGroups tag groups for which tag group ids are to be resolved.
     * @return map from discovered tag group id to persisted tag group id.
     * @throws com.vmturbo.sql.utils.DbException on encountering an error during inserts.
     */
    @Nonnull
    public Map<Long, Long> resolveIdForDiscoveredTagGroups(
        @Nonnull final Map<Long, CostBilling.CostTagGroup> discoveredTagGroups) throws DbException {

        initializeCache();

        final Stopwatch stopwatch = Stopwatch.createStarted();

        // Collect to List (instead of Set) - 2 Tag groups may resolve to the same durable Tag group oid if their
        // constituent Tags are identical when trailing / leading spaces are ignored.
        final Set<Tag> allTags = new HashSet<>();
        final List<DiscoveredTagGroup> tagGroups = new ArrayList<>(discoveredTagGroups.size());
        discoveredTagGroups.forEach((discoveredGroupId, costTagGroup) -> {

            final DiscoveredTagGroup discoveredTagGroup = DiscoveredTagGroup.create(discoveredGroupId, costTagGroup);

            tagGroups.add(discoveredTagGroup);
            allTags.addAll(discoveredTagGroup.tags());

        });

        final Map<Tag, Long> resolvedTagIds = tagIdentityService.resolveIdForDiscoveredTags(allTags);

        final Map<Long, TagGroupIdentity> tagGroupIdentities = tagGroups.stream()
                .collect(ImmutableMap.toImmutableMap(
                        DiscoveredTagGroup::discoveredTagGroupId,
                        tagGroup -> getOrCreateTagGroupIdentity(tagGroup, resolvedTagIds)));


        for (List<TagGroupIdentity> identityBatch : Iterables.partition(tagGroupIdentities.values(), batchSize)) {
            // Make an attempt ot persist the IDs. The store may decide to skip persisting the IDs, if they have
            // recently been seen, but it must guarantee to return only if the IDs were successfully persisted.
            tagGroupStore.insertCostTagGroups(identityBatch);
        }

        logger.debug("Resolved {} discovered cost tag identities in {}", discoveredTagGroups.size(), stopwatch);

        return tagGroupIdentities.entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(
                        Map.Entry::getKey,
                        tagGroupIdentityEntry -> tagGroupIdentityEntry.getValue().tagGroupId()));
    }

    /**
     * Gets the {@link CostTagGroup}s associated with the provided tag group IDs. Any tag group Ids or referenced tag IDs
     * that cannot be resolved will be ignored.
     * @param tagGroupIds The persisted (not discovered) tag group IDs.
     * @return A map of each tag group ID to the {@link CostTagGroup} representation.
     */
    @Nonnull
    public Map<Long, CostBilling.CostTagGroup> getTagGroupsById(@Nonnull Set<Long> tagGroupIds) {


        final Map<Long, TagGroupIdentity> tagGroupIdentities = tagGroupStore.retrieveTagGroupIdentities(tagGroupIds);
        final Set<Long> allTagIds = tagGroupIdentities.values().stream()
                .map(TagGroupIdentity::tagIds)
                .flatMap(Set::stream)
                .collect(ImmutableSet.toImmutableSet());

        final Map<Long, Tag> tagMap = tagIdentityService.getTagsById(allTagIds);

        return tagGroupIdentities.entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        Map.Entry::getKey,
                        tagGroupEntry -> tagGroupEntry.getValue().tagIds()
                                .stream()
                                .filter(tagMap::containsKey)
                                .map(tagMap::get)
                                .collect(Collector.of(
                                        CostTagGroup::newBuilder,
                                        (tagGroup, tag) -> tagGroup.putTags(tag.key(), tag.value()),
                                        (groupA, groupB) -> groupA.mergeFrom(groupB.build()),
                                        CostTagGroup.Builder::build))));
    }

    private LongSet createTagIdSet(final Set<Tag> tags, final Map<Tag, Long> resolvedTagIds) {

        final LongSet tagIdSet = new LongArraySet(tags.size());
        tags.forEach(tag -> tagIdSet.add(resolvedTagIds.get(tag)));
        return tagIdSet;
    }

    private TagGroupIdentity getOrCreateTagGroupIdentity(@Nonnull DiscoveredTagGroup tagGroup,
                                                         @Nonnull Map<Tag, Long> tagIdMap) {

        final LongSet tagIdSet = createTagIdSet(tagGroup.tags(), tagIdMap);

        return tagGroupIdentityCache.computeIfAbsent(tagIdSet, tagIds ->
                TagGroupIdentity.of(identityProvider.next(), tagIds));
    }

    private void initializeCache() throws DbException {

        synchronized (cacheInitialized) {
            if (!cacheInitialized.get()) {

                tagGroupIdentityCache.clear();

                tagGroupStore.retrieveAllTagGroups().forEach(tagGroupIdentity -> {

                    final LongSet tagIdSet = new LongArraySet(tagGroupIdentity.tagIds());
                    tagGroupIdentityCache.compute(tagIdSet, (tagSet, originalTagGroupIdentity) -> {
                        if (originalTagGroupIdentity != null) {

                            logger.warn("");

                            return tagGroupIdentity.tagGroupId() < originalTagGroupIdentity.tagGroupId()
                                    ? tagGroupIdentity
                                    : originalTagGroupIdentity;
                        } else {
                            return tagGroupIdentity;
                        }
                    });
                });

                cacheInitialized.set(true);
            }
        }
    }


    /**
     * Tag group internal representation.
     */

    @HiddenImmutableTupleImplementation
    @Immutable
    interface DiscoveredTagGroup {

        long discoveredTagGroupId();

        @Nonnull
        Set<Tag> tags();

        @Nonnull
        static DiscoveredTagGroup create(long discoveredTagGroupId,
                                         @Nonnull CostTagGroup discoveredTagGroup) {

            final Set<Tag> tags = discoveredTagGroup.getTagsMap().entrySet()
                    .stream()
                    .map(tagEntry -> Tag.of(tagEntry.getKey(), tagEntry.getValue()))
                    .collect(ImmutableSet.toImmutableSet());

            return DiscoveredTagGroupTuple.of(discoveredTagGroupId, tags);
        }
    }
}