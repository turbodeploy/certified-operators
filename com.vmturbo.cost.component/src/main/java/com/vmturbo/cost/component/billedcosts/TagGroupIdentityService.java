package com.vmturbo.cost.component.billedcosts;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cost.component.db.tables.records.CostTagGroupingRecord;
import com.vmturbo.platform.sdk.common.CostBilling;
import com.vmturbo.platform.sdk.common.util.SetOnce;
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
    private final Map<LongSet, Long> tagGroupIdentityCache = new HashMap<>();
    private final SetOnce<Boolean> cacheInitialized = SetOnce.create();

    /**
     * Creates an instance of CostTagGroupIdentityResolver.
     *
     * @param tagGroupStore for retrieving / writing into cost tag grouping table.
     * @param tagIdentityService for retrieving tag ids.
     * @param identityProvider for generating ids for new tag groups.
     */
    public TagGroupIdentityService(@Nonnull TagGroupStore tagGroupStore,
                                   @Nonnull TagIdentityService tagIdentityService,
                                   @Nonnull IdentityProvider identityProvider) {
        this.tagGroupStore = Objects.requireNonNull(tagGroupStore);
        this.tagIdentityService = Objects.requireNonNull(tagIdentityService);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    /**
     * Returns a map from discovered tag group id to tag group id that is stored in tag grouping table. For tag groups
     * seen before, the tag group id is retrieved from the tagGroupIdentityCache by looking by tag members. For new tag
     * groups, a new tag group id is generated via IdentityProvider and then stored in the cost tag grouping table with
     * the members.
     *
     * @param tagGroupsByTagGroupId tag groups for which tag group ids are to be resolved.
     * @return map from discovered tag group id to persisted tag group id.
     * @throws com.vmturbo.sql.utils.DbException on encountering an error during inserts.
     */
    @Nonnull
    synchronized Map<Long, Long> resolveIdForDiscoveredTagGroups(
        @Nonnull final Map<Long, CostBilling.CostTagGroup> tagGroupsByTagGroupId) throws DbException {
        if (!cacheInitialized.getValue().isPresent()) {
            tagGroupIdentityCache.putAll(tagGroupStore.retrieveAllTagGroups().stream()
                .collect(Collectors.groupingBy(CostTagGroupingRecord::getTagGroupId, Collectors.toSet()))
                .entrySet().stream()
                .collect(Collectors.toMap(e -> new LongArraySet(
                    e.getValue().stream().map(CostTagGroupingRecord::getTagId).collect(Collectors.toSet())),
                    Map.Entry::getKey)));
            cacheInitialized.ensureSet(() -> true);
        }

        final Set<TagGroup> tagGroups = tagGroupsByTagGroupId.entrySet().stream()
            .map(entry -> new TagGroup(entry.getKey(), tagMapToSet(entry.getValue().getTagsMap())))
            .collect(Collectors.toSet());

        final Map<Tag, Long> resolvedTagIds = tagIdentityService.resolveIdForDiscoveredTags(
            tagGroups.stream().map(TagGroup::getTags).flatMap(Set::stream).collect(Collectors.toSet())
        );
        final Set<TagGroup> unseenTagGroups = tagGroups.stream()
            .filter(tagGroup -> !tagGroupIdentityCache.containsKey(tagsToTagIds(tagGroup.getTags(), resolvedTagIds)))
            .collect(Collectors.toSet());
        final Map<Long, TagGroup> unseenTagGroupsByOid = generateOidPerTagGroup(unseenTagGroups);
        if (!unseenTagGroups.isEmpty()) {
            logger.debug("Inserting the following newly discovered tag groups: {}", unseenTagGroupsByOid::values);
            final Set<CostTagGroupingRecord> newTagGroupRecords =
                convertTagGroupsToRecords(unseenTagGroupsByOid, resolvedTagIds);
            tagGroupStore.insertCostTagGroups(newTagGroupRecords);
            unseenTagGroupsByOid.forEach((oid, tagGroup) -> {
                tagGroupIdentityCache.put(tagsToTagIds(tagGroup.getTags(), resolvedTagIds), oid);
            });
        }
        return tagGroups.stream().collect(Collectors.toMap(TagGroup::getDiscoveredTagGroupId,
            tagGroup -> tagGroupIdentityCache.get(tagsToTagIds(tagGroup.getTags(), resolvedTagIds))));
    }

    private Map<Long, TagGroup> generateOidPerTagGroup(final Set<TagGroup> tagGroups) {
        return tagGroups.stream()
            .collect(Collectors.toMap(tagGroup -> identityProvider.next(), Function.identity()));
    }

    private Set<CostTagGroupingRecord> convertTagGroupsToRecords(final Map<Long, TagGroup> tagGroups,
                                                                  final Map<Tag, Long> resolvedTagIds) {
        return tagGroups.entrySet().stream()
            .map(entry -> {
                final long oid = entry.getKey();
                final TagGroup tagGroup = entry.getValue();
                final Set<Tag> tags = entry.getValue().getTags();
                final LongSet tagIds = tagsToTagIds(tags, resolvedTagIds);
                if (tags.size() != tagIds.size()) {
                    logger.warn("Tag ids were not successfully resolved for tag group {}. Tags: {}, Tag Ids: {}",
                        tagGroup.getDiscoveredTagGroupId(), tags, tagIds);
                    return null;
                }
                return tagsToTagIds(entry.getValue().getTags(), resolvedTagIds).stream()
                    .map(tagId -> new CostTagGroupingRecord(oid, tagId))
                    .collect(Collectors.toList());
            }).filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }

    private LongSet tagsToTagIds(final Set<Tag> tags, final Map<Tag, Long> resolvedTagIds) {
        return new LongArraySet(tags.stream().map(resolvedTagIds::get)
            .collect(Collectors.toSet()));
    }

    private Set<Tag> tagMapToSet(final Map<String, String> tags) {
        return tags.entrySet().stream()
            .map(entry -> new Tag(entry.getKey(), entry.getValue()))
            .collect(Collectors.toSet());
    }

    /**
     * Tag group internal representation.
     */
    private static class TagGroup {
        private final long discoveredTagGroupId;
        private final Set<Tag> tags;

        private TagGroup(long discoveredTagGroupId, Set<Tag> tags) {
            this.discoveredTagGroupId = discoveredTagGroupId;
            this.tags = tags;
        }

        public long getDiscoveredTagGroupId() {
            return discoveredTagGroupId;
        }

        public Set<Tag> getTags() {
            return tags;
        }

        @Override
        public String toString() {
            return tags.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TagGroup)) {
                return false;
            }
            TagGroup tagGroup = (TagGroup)o;
            return getTags().equals(tagGroup.getTags());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTags());
        }
    }
}