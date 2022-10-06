package com.vmturbo.cost.component.billed.cost.tag;

import java.util.Collection;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;

/**
 * A tag group with its associated persistence ID.
 */
@HiddenImmutableTupleImplementation
@Immutable
public interface TagGroupIdentity {

    /**
     * The tag group ID.
     * @return The tag group ID.
     */
    long tagGroupId();

    /**
     * The set of tag Ids comprising this tag group.
     * @return The set of tag Ids comprising this tag group.
     */
    @Nonnull
    Set<Long> tagIds();

    /**
     * Constructs a new {@link TagGroupIdentity} instance.
     * @param tagGroupId The tag group ID.
     * @param tagIds The set of tag IDs.
     * @return The {@link TagGroupIdentity} instance.
     */
    static TagGroupIdentity of(long tagGroupId,
                               @Nonnull Collection<Long> tagIds) {
        return TagGroupIdentityTuple.of(tagGroupId, tagIds);
    }
}
