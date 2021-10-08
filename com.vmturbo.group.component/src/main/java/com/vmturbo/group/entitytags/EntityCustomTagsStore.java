package com.vmturbo.group.entitytags;

import static com.vmturbo.group.db.tables.EntityCustomTags.ENTITY_CUSTOM_TAGS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;

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

    private final Logger logger = LogManager.getLogger();

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
        // same entity to unify it's values into a new set fo values.
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