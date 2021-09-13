package com.vmturbo.group.entitytags;

import static com.vmturbo.group.db.tables.EntityCustomTags.ENTITY_CUSTOM_TAGS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;

import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.common.Truncator;
import com.vmturbo.group.db.tables.pojos.EntityCustomTags;

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
    public int[] insertTags(long entityId, @Nonnull Tags tags) {
        return dslContext.batch(insertTags(dslContext, tags, entityId)).execute();
    }

    /**
     * Creates new user defined tags in DAO.
     *
     * @param context transactional DB context
     * @param tags is the ga for
     * @param entityId the entity uuid
     * @return the array of tags inserted
     */
    private Collection<Query> insertTags(@Nonnull DSLContext context, @Nonnull Tags tags,
        long entityId) {
        final Collection<Query> result = new ArrayList<>();
        for (Entry<String, TagValuesDTO> entry : tags.getTagsMap().entrySet()) {
            final String tagKey = Truncator.truncateTagKey(entry.getKey(), true);
            for (String tagValue : entry.getValue().getValuesList()) {
                final String truncateTagValue = Truncator.truncateTagValue(tagValue, true);
                final EntityCustomTags tag = new EntityCustomTags(entityId, tagKey, truncateTagValue);
                result.add(
                    context.insertInto(ENTITY_CUSTOM_TAGS)
                        .set(context.newRecord(ENTITY_CUSTOM_TAGS, tag))
                        .onDuplicateKeyIgnore()
                );
            }
        }
        return result;
    }
}