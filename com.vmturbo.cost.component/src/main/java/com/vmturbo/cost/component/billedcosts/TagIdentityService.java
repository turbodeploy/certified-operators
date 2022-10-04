package com.vmturbo.cost.component.billedcosts;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cost.component.cloud.cost.tag.Tag;
import com.vmturbo.cost.component.cloud.cost.tag.TagIdentity;
import com.vmturbo.cost.component.db.tables.records.CostTagRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * Responsible for resolving identity of discovered tags by retrieving previously stored tags ids or generating new tag
 * ids, storing them in the cost_tag table and on successful insertion, returning the generated tag ids back.
 */
public class TagIdentityService {

    private static final Logger logger = LogManager.getLogger();
    private final TagStore tagStore;
    private final IdentityProvider identityProvider;
    private final Map<Tag, Long> tagIdentityCache = new ConcurrentHashMap<>();
    private final AtomicBoolean cacheInitialized = new AtomicBoolean(false);
    private final int batchSize;

    /**
     * Creates an instance of CostTagIdentityResolver.
     *
     * @param tagStore instance to retrieve / store data into the Cost Tag table.
     * @param identityProvider to generate new tag ids.
     * @param batchSize num records inserted per batch.
     */
    public TagIdentityService(@Nonnull TagStore tagStore, @Nonnull IdentityProvider identityProvider, int batchSize) {
        this.tagStore = Objects.requireNonNull(tagStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.batchSize = batchSize;
    }

    /**
     * Returns a map from tag to the tag id. The tag id is retrieved from the tagIdentityCache for tags seen before.
     * For new tags, the tag id is generated using the IdentityProvider instance and written into the cost tag table
     * before returning as a part of this map.
     *
     * @param tags for which tag ids are to be retrieved.
     * @return map from tag to tag id.
     * @throws com.vmturbo.sql.utils.DbException on encountering an error during inserts.
     */
    public Map<Tag, Long> resolveIdForDiscoveredTags(@Nonnull final Set<Tag> tags) throws DbException {

        initializeCache();

        final Set<TagIdentity> tagIdentities = tags.stream()
                .map(this::getOrCreateTagIdentity)
                .collect(ImmutableSet.toImmutableSet());

        tagStore.insertCostTagIdentities(tagIdentities);

        return tagIdentities.stream()
                .collect(ImmutableMap.toImmutableMap(
                        TagIdentity::tag,
                        TagIdentity::tagId));
    }

    /**
     * Gets the {@link Tag}s associated with the requested tag IDs. Those tag IDs not found will
     * be skipped.
     * @param tagIds The tag IDs.
     * @return A map of the tag ID to its associated {@link Tag}.
     */
    @Nonnull
    public Map<Long, Tag> getTagsById(@Nonnull Set<Long> tagIds) {

        return tagStore.retrieveCostTags(tagIds).entrySet()
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        Map.Entry::getKey,
                        tagIdentityEntry -> tagIdentityEntry.getValue().tag()));
    }

    @Nonnull
    private TagIdentity getOrCreateTagIdentity(@Nonnull Tag tag) {
        return TagIdentity.of(
                tagIdentityCache.computeIfAbsent(tag, t -> identityProvider.next()),
                tag);
    }

    private void initializeCache() throws DbException {

        synchronized (cacheInitialized) {
            if (!cacheInitialized.get()) {

                tagIdentityCache.clear();

                tagIdentityCache.putAll(tagStore.retrieveAllCostTags().stream()
                        .collect(Collectors.toMap(rec -> Tag.of(rec.getTagKey(), rec.getTagValue()),
                                CostTagRecord::getTagId)));

                cacheInitialized.set(true);
            }
        }
    }
}