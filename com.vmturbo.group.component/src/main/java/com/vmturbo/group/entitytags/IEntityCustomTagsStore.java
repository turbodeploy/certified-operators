package com.vmturbo.group.entitytags;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.EntityCustomTags;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.service.StoreOperationException;

/**
 * Store to operate with user defined entity tags. It's responsible to create tags.
 */
public interface IEntityCustomTagsStore {

    /**
     * Create a list of user defined entity tags. Discovered tags are not supported by this
     * call.
     *
     * @param entityId is the entity oid to attach the tags.
     * @param tags is the list of tag entries to store.
     * @return the number of records attempted to insert.
     *
     * @throws StoreOperationException if there are duplicate tags in the tag list.
     */
    int insertTags(long entityId, @Nonnull Tags tags) throws StoreOperationException;

    /**
     * Get the list of user defined tags for an entity.
     *
     * @param entityId is the oid of the entity whose tags will be fetched.
     * @return the tags of the specified entity.
     */
    Tags getTags(long entityId);

    /**
     * Get the list of user defined entity tags.
     *
     * @return the tags of all entities.
     */
    List<EntityCustomTags> getAllTags();
}
