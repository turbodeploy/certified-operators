package com.vmturbo.cost.component.billed.cost.tag;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;

/**
 * A {@link Tag} with its persistence ID.
 */
@HiddenImmutableTupleImplementation
@Immutable
public interface TagIdentity {

    /**
     * The tag ID.
     * @return The tag ID.
     */
    long tagId();

    /**
     * The tag.
     * @return The tag.
     */
    @Nonnull
    Tag tag();

    /**
     * Constructs a new {@link TagIdentity} instance.
     * @param tagId The tag ID.
     * @param tag The tag.
     * @return The new {@link TagIdentity} instance.
     */
    @Nonnull
    static TagIdentity of(long tagId,
                          @Nonnull Tag tag) {
        return TagIdentityTuple.of(tagId, tag);
    }
}
