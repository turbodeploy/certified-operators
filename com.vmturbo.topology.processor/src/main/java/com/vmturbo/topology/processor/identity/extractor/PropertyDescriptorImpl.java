package com.vmturbo.topology.processor.identity.extractor;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.MoreObjects;

import com.vmturbo.topology.processor.identity.PropertyDescriptor;

/**
 * Implementation of the PropertyDescriptor interface.
 */
public class PropertyDescriptorImpl implements PropertyDescriptor {

    private final String value;

    private final int rank;

    public PropertyDescriptorImpl(@Nonnull String value, int rank) {
        Objects.requireNonNull(value);
        this.value = value;
        this.rank = rank;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Integer getPropertyTypeRank() {
        return rank;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("value", value)
                          .add("rank", rank)
                          .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PropertyDescriptorImpl)) {
            return false;
        }

        @Nonnull PropertyDescriptorImpl that = (PropertyDescriptorImpl)o;
        return rank == that.rank && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return 37 * rank + value.hashCode();
    }
}
