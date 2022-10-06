package com.vmturbo.cost.component.cloud.cost.tag;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;

/**
 * Representation of a Tag.
 */
@HiddenImmutableTupleImplementation
@Immutable(prehash = true)
public interface Tag {

    /**
     * The tag key.
     * @return The tag key.
     */
    @Nonnull
    String key();

    /**
     * The tag value.
     * @return The tag balue.
     */
    @Nonnull
    String value();

    /**
     * Constructs a new {@link Tag}.
     * @param key The tag key.
     * @param value The tag value.
     * @return The new {@link Tag} instance.
     */
    @Nonnull
    static Tag of(@Nonnull String key,
                  @Nonnull String value) {

        // remove any leading or trailing spaces from key and value
        return TagTuple.of(key.trim(), value.trim());
    }
}