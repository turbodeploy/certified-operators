package com.vmturbo.cost.component.billedcosts;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cost.component.db.tables.records.CostTagRecord;
import com.vmturbo.platform.sdk.common.util.SetOnce;
import com.vmturbo.sql.utils.DbException;

/**
 * Responsible for resolving identity of discovered tags by retrieving previously stored tags ids or generating new tag
 * ids, storing them in the cost_tag table and on successful insertion, returning the generated tag ids back.
 */
public class TagIdentityService {

    private static final Logger logger = LogManager.getLogger();
    private final TagStore tagStore;
    private final IdentityProvider identityProvider;
    private final Map<Tag, Long> tagIdentityCache = new HashMap<>();
    private final SetOnce<Boolean> cacheInitialized = SetOnce.create();

    /**
     * Creates an instance of CostTagIdentityResolver.
     *
     * @param tagStore instance to retrieve / store data into the Cost Tag table.
     * @param identityProvider to generate new tag ids.
     */
    public TagIdentityService(@Nonnull TagStore tagStore, @Nonnull IdentityProvider identityProvider) {
        this.tagStore = Objects.requireNonNull(tagStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
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
    synchronized Map<Tag, Long> resolveIdForDiscoveredTags(@Nonnull final Set<Tag> tags) throws DbException {
        if (!cacheInitialized.getValue().isPresent()) {
            tagIdentityCache.putAll(tagStore.retrieveAllCostTags().stream()
                .collect(Collectors.toMap(rec -> new Tag(rec.getTagKey(), rec.getTagValue()),
                    CostTagRecord::getTagId)));
            cacheInitialized.ensureSet(() -> true);
        }
        final Set<Tag> unseenTags = tags.stream()
            .filter(tag -> !tagIdentityCache.containsKey(tag))
            .collect(Collectors.toSet());
        if (!unseenTags.isEmpty()) {
            logger.debug("Inserting the following newly discovered tags: {}", () -> unseenTags);
            final Set<CostTagRecord> recordsToInsert = unseenTags.stream()
                .map(tag -> {
                    final CostTagRecord record = new CostTagRecord();
                    record.setTagKey(tag.getKey());
                    record.setTagValue(tag.getValue());
                    record.setTagId(identityProvider.next());
                    return record;
                }).collect(Collectors.toSet());
            tagStore.insertCostTagRecords(recordsToInsert);
            tagIdentityCache.putAll(recordsToInsert.stream()
                .collect(Collectors.toMap(rec -> new Tag(rec.getTagKey(), rec.getTagValue()), CostTagRecord::getTagId)));
        }
        return tags.stream().collect(Collectors.toMap(Function.identity(), tagIdentityCache::get));
    }
}