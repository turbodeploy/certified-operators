package com.vmturbo.cost.component.billedcosts;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

import com.vmturbo.cost.component.billed.cost.tag.Tag;
import com.vmturbo.cost.component.billed.cost.tag.TagIdentity;
import com.vmturbo.cost.component.db.tables.CostTag;
import com.vmturbo.cost.component.db.tables.records.CostTagRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * An object of this type contains data operations for the Cost Tag table.
 */
public class TagStore {

    private final DSLContext dslContext;

    private final Map<Long, TagIdentity> tagIdentityCache = new ConcurrentHashMap<>();

    /**
     * Creates an instance of TagStore.
     *
     * @param dslContext instance for executing queries.
     */
    public TagStore(@Nonnull final DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    /**
     * Retrieves ALL CostTagRecords from the Cost Tag table.
     *
     * @return records for tags that are persisted in the Cost tag table.
     * @throws com.vmturbo.sql.utils.DbException on encountering error while executing select query.
     */
    public Result<CostTagRecord> retrieveAllCostTags() throws DbException {
        try {
            return dslContext.selectFrom(CostTag.COST_TAG).fetch();
        } catch (DataAccessException ex) {
            throw new DbException("Exception while retrieving tags.", ex);
        }
    }

    /**
     * Retrieves the cost tags associated with the provided IDs. If a cost tag cannot be found
     * for a given ID, it will be skipped.
     * @param tagIds The tag IDs.
     * @return The {@link TagIdentity} associated with each requested tag ID.
     */
    @Nonnull
    public Map<Long, TagIdentity> retrieveCostTags(@Nonnull Set<Long> tagIds) {

        final ImmutableMap.Builder<Long, TagIdentity> tagIdentityMap = ImmutableMap.builder();
        final Set<Long> unseenTagIds = new HashSet<>();

        tagIds.forEach(tagId -> {
            if (tagIdentityCache.containsKey(tagId)) {
                tagIdentityMap.put(tagId, tagIdentityCache.get(tagId));
            } else {
                unseenTagIds.add(tagId);
            }
        });

        if (!unseenTagIds.isEmpty()) {
            dslContext.selectFrom(CostTag.COST_TAG)
                    .where(CostTag.COST_TAG.TAG_ID.in(unseenTagIds))
                    .fetch()
                    .forEach(costTagRecord -> {
                        final TagIdentity tagIdentity = TagIdentity.of(
                                costTagRecord.getTagId(), Tag.of(costTagRecord.getTagKey(), costTagRecord.getTagValue()));

                        tagIdentityMap.put(tagIdentity.tagId(), tagIdentity);
                        tagIdentityCache.put(tagIdentity.tagId(), tagIdentity);
                    });
        }

        return tagIdentityMap.build();
    }

    /**
     * Inserts CostTagRecords to the Cost Tag table.
     *
     * @param tagIdentities to be inserted into the Cost Tag table.
     * @throws com.vmturbo.sql.utils.DbException on encountering DataAccessException during query execution.
     */
    public void insertCostTagIdentities(@Nonnull final Collection<TagIdentity> tagIdentities) throws DbException {

        // filter out those tag identities already in the cache. It's possible two+ concurrent processes both determine
        // the same record should be inserted. This should be handled in the DB through on-update do nothing
        try {
            final List<TagIdentity> unseenIdentities = tagIdentities.stream()
                    .filter(tagIdentity -> !tagIdentityCache.containsKey(tagIdentity.tagId()))
                    .collect(ImmutableList.toImmutableList());

            if (!unseenIdentities.isEmpty()) {

                final List<CostTagRecord> unseenRecords =
                        unseenIdentities.stream().map(this::createRecordFromIdentity).collect(ImmutableList.toImmutableList());

                dslContext.batch(unseenRecords.stream()
                        .map(unseenRecord -> dslContext.insertInto(CostTag.COST_TAG).set(unseenRecord).onDuplicateKeyIgnore())
                        .toArray(Query[]::new)).execute();
            }

            // update the cache to avoid attempting persistence
            unseenIdentities.forEach(tagIdentity -> tagIdentityCache.put(tagIdentity.tagId(), tagIdentity));
        } catch (Exception e) {
            throw new DbException("Failed to get tags from DB", e);
        }
    }

    private CostTagRecord createRecordFromIdentity(@Nonnull TagIdentity tagIdentity) {

        final CostTagRecord record = new CostTagRecord();
        record.setTagKey(tagIdentity.tag().key());
        record.setTagValue(tagIdentity.tag().value());
        record.setTagId(tagIdentity.tagId());
        return record;
    }
}