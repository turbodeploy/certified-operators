package com.vmturbo.group.entitytags;

import static com.vmturbo.group.db.tables.EntityCustomTags.ENTITY_CUSTOM_TAGS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;

import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.common.Truncator;
import com.vmturbo.group.db.tables.pojos.EntityCustomTags;
import com.vmturbo.group.db.tables.records.EntityCustomTagsRecord;
import com.vmturbo.group.service.StoreOperationException;

/**
 * The {@link EntityCustomTagsStore} class is used for CRUD operations on user defined entity tags, to abstract away the
 * persistence details from the rest of the component.
 */
public class EntityCustomTagsStore implements IEntityCustomTagsStore {

    // The transactional DB context.
    private final DSLContext dslContext;

    /**
     * Constructs placement policy store.
     *
     * @param dslContext DB connection context to use
     */
    public EntityCustomTagsStore(@Nonnull final DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    @Override
    public int deleteTag(long entityId, @Nonnull String tagKey) throws StoreOperationException {
        int affectedRows;
        try {
            affectedRows = dslContext.deleteFrom(ENTITY_CUSTOM_TAGS).where(
                    ENTITY_CUSTOM_TAGS.ENTITY_ID.eq(entityId),
                    ENTITY_CUSTOM_TAGS.TAG_KEY.eq(tagKey)
            ).execute();
        } catch (DataAccessException e) {
            throw new StoreOperationException(Status.INTERNAL,
                    "Could not delete tags for Entity: '" + entityId + "' and key: '" + tagKey + "'");
        }

        return affectedRows;
    }

    @Override
    public void deleteTags(long entityId) throws StoreOperationException {
        try {
            dslContext.deleteFrom(ENTITY_CUSTOM_TAGS)
                    .where(ENTITY_CUSTOM_TAGS.ENTITY_ID.eq(entityId))
                    .execute();
        } catch (DataAccessException e) {
            throw new StoreOperationException(Status.INTERNAL,
                    "Could not delete tags for Entity: '" + entityId + "'");
        }
    }

    /**
     * Delete a list of tags for an entity. Note that it will first check if requested tag for
     * delete exists, and if it doesn't it will fail. If not it will proceed to the deletion, but a
     * double delete can still happen, if in the meantime of the "check" another delete happens.
     *
     * @param entityId is the entity oid.
     * @param tagKeys is the list of tag keys to delete.
     * @return the affected rows after the database operations.
     *
     * @throws StoreOperationException if the tags for the entity could not to be deleted.
     */
    @Override
    public int deleteTagList(long entityId, Collection<String> tagKeys) throws StoreOperationException {
        Set<String> tagSet = new HashSet<>(tagKeys);
        // Fetch tag records to see if exist
        Collection<? extends EntityCustomTagsRecord> tagRecords =
                dslContext.selectFrom(ENTITY_CUSTOM_TAGS)
                .where(ENTITY_CUSTOM_TAGS.ENTITY_ID.eq(entityId),
                        ENTITY_CUSTOM_TAGS.TAG_KEY.in(tagSet)
                ).fetch();

        // Filter out the fetched tag keys, for proper error message if needed
        Set<String> tagKeysNotFetched = new HashSet<>(tagKeys);
        tagKeysNotFetched.removeAll(
                tagRecords.stream().map(EntityCustomTagsRecord::getTagKey)
                .collect(Collectors.toSet())
        );
        if (!tagKeysNotFetched.isEmpty()) {
            throw new StoreOperationException(Status.NOT_FOUND,
                    "No such tag(s) with tag key(s): '" + tagKeysNotFetched + " for entity '"
                            + entityId + "'");
        }

        int affectedRows;
        try {
            // This executes delete in multiple rows. If this is expected to be used for delete on
            // many tags, then we need to do a batch execute instead.
            affectedRows = dslContext.deleteFrom(ENTITY_CUSTOM_TAGS).where(
                    ENTITY_CUSTOM_TAGS.ENTITY_ID.eq(entityId),
                    ENTITY_CUSTOM_TAGS.TAG_KEY.in(tagSet)
            ).execute();
        } catch (DataAccessException e) {
            throw new StoreOperationException(Status.INTERNAL,
                    "Could not delete tags for Entity: '" + entityId + "'");
        }

        return affectedRows;
    }

    @Override
    public int insertTags(long entityId, @Nonnull Tags tags) throws StoreOperationException {
        Collection<Query> queries = insertTags(dslContext, tags, entityId);
        return Arrays.stream(dslContext.batch(queries).execute()).sum();
    }

    /**
     * Creates new user defined tags in DAO.
     *
     * @param context transactional DB context
     * @param tags is the tags to create
     * @param entityId the entity uuid
     * @return the set of records to insert
     *
     * @throws StoreOperationException if a tag to be inserted, already exists.
     */
    private Collection<Query> insertTags(@Nonnull DSLContext context, @Nonnull Tags tags,
        long entityId) throws StoreOperationException {
        final Collection<Query> result = new ArrayList<>();

        for (Entry<String, TagValuesDTO> entry : tags.getTagsMap().entrySet()) {
            final String tagKey = Truncator.truncateTagKey(entry.getKey(), true);
            if (context.fetchExists(context.selectFrom(ENTITY_CUSTOM_TAGS).where(
                    ENTITY_CUSTOM_TAGS.ENTITY_ID.eq(entityId),
                    ENTITY_CUSTOM_TAGS.TAG_KEY.eq(tagKey)))) {
                throw new StoreOperationException(Status.INVALID_ARGUMENT,
                        "Trying to insert a tag with a key that already exists: " + tagKey);
            }

            for (String tagValue : entry.getValue().getValuesList()) {
                final String truncateTagValue = Truncator.truncateTagValue(tagValue, true);
                final EntityCustomTags tag = new EntityCustomTags(entityId, tagKey, truncateTagValue);
                EntityCustomTagsRecord tagRecord = context.newRecord(ENTITY_CUSTOM_TAGS, tag);
                result.add(context.insertInto(ENTITY_CUSTOM_TAGS)
                        .set(tagRecord).onDuplicateKeyIgnore());
            }
        }
        return result;
    }

    @Override
    public Tags getTags(long entityId) {
        List<? extends EntityCustomTagsRecord> tagRecords = dslContext.selectFrom(ENTITY_CUSTOM_TAGS).where(
                ENTITY_CUSTOM_TAGS.ENTITY_ID.eq(entityId)).fetch();

        Map<String, TagValuesDTO.Builder> items = new HashMap<>();
        for (EntityCustomTagsRecord tagRecord : tagRecords) {
            items.computeIfAbsent(tagRecord.getTagKey(),
                    k -> TagValuesDTO.newBuilder()).addValues(tagRecord.getTagValue());
        }

        return Tags.newBuilder().putAllTags(
                items.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().build()
                ))
        ).build();
    }

    @Override
    public List<EntityCustomTagsOuterClass.EntityCustomTags> getAllTags() {
        List<? extends EntityCustomTagsRecord> tagRecords =
                dslContext.selectFrom(ENTITY_CUSTOM_TAGS).fetch();

        // Prepare the map of tags from the record. This is needed so that same tags keys for the
        // same entity to unify its values into a new set of values.
        Map<Long, Map<String, TagValuesDTO.Builder>> tagsMap = new HashMap<>();
        for (EntityCustomTagsRecord tagRecord : tagRecords) {

            tagsMap.computeIfAbsent(tagRecord.getEntityId(), id -> new HashMap<>())
                    .computeIfAbsent(tagRecord.getTagKey(),
                            k -> TagValuesDTO.newBuilder()).addValues(tagRecord.getTagValue());
        }

        // Create the final list of EntityCustomTags, which is basically the mapping from entity oid
        // to the Tags attached to it.
        List<EntityCustomTagsOuterClass.EntityCustomTags> tags = new ArrayList<>();
        for (Map.Entry<Long, Map<String, TagValuesDTO.Builder>> entry : tagsMap.entrySet()) {
            tags.add(
                EntityCustomTagsOuterClass.EntityCustomTags.newBuilder()
                        .setTags(
                            Tags.newBuilder().putAllTags(
                                    entry.getValue().entrySet().stream().collect(Collectors.toMap(
                                            Map.Entry::getKey,
                                            e -> e.getValue().build()
                                    ))).build()
                        )
                        .setEntityId(entry.getKey())
                        .build()
            );
        }

        return tags;
    }
}