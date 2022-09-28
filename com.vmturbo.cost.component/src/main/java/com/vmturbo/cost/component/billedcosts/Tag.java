package com.vmturbo.cost.component.billedcosts;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Representation of a Tag.
 */
public class Tag {
    private final String key;
    private final String value;

    /**
     * Creates a tag instance.
     *
     * @param key of the tag.
     * @param value of the tag.
     */
    public Tag(@Nonnull final String key, @Nonnull final String value) {
        this.key = Objects.requireNonNull(key).trim();
        this.value = Objects.requireNonNull(value).trim();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Tag)) {
            return false;
        }
        final Tag tag = (Tag)o;
        return getKey().equals(tag.getKey()) && getValue().equals(tag.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKey(), getValue());
    }

    @Override
    public String toString() {
        return "[" + key + ":" + value + "]";
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}