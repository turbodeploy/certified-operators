package com.vmturbo.group.entitytags;

import java.util.Collection;
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
     * Delete a user defined tag for entity.
     *
     * @param entityId is the entity oid to attach the tag
     * @param tagKey tag key
     * @return the affected rows after the database operation
     *
     * @throws StoreOperationException if the tag with key could not be deleted.
     */
    int deleteTag(long entityId, @Nonnull String tagKey) throws StoreOperationException;

    /**
     * Delete all user defined tags for an entity.
     *
     * @param entityId is the entity oid upon which all the tags will be deleted.
     *
     * @throws StoreOperationException if the tags for the entity could not to be deleted.
     */
    void deleteTags(long entityId) throws StoreOperationException;

    /**
     * Delete a list of tags for an entity. Note that it will first check if requested tag for
     * delete exists, and if it doesn't it will fail. If not it will proceed to the deletion, but a
     * double delete can still happen, if in the meantime of the "check" another delete happens.
     *
     * @param entityId is the entity oid.
     * @param tagKeys is the list of tag keys to delete.
     * @return the affected rows after the database operations. This should match the length of the
     * tag key list.
     *
     * @throws StoreOperationException if the tags for the entity could not to be deleted.
     */
    int deleteTagList(long entityId, Collection<String> tagKeys) throws StoreOperationException;

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
