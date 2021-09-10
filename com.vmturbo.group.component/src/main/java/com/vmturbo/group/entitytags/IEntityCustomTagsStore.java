package com.vmturbo.group.entitytags;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.tag.Tag.Tags;

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
     * @return the query results.
     */
    int[] insertTags(long entityId, @Nonnull Tags tags);
}
