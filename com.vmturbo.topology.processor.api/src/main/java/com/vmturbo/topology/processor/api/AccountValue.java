package com.vmturbo.topology.processor.api;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a single record in account values. All the implementations must be treaded as equal,
 * if all the methods declared in this interface return same appropriate values. The same - for
 * {@link #hashCode()}.
 */
public interface AccountValue {
    /**
     * Returns name of the field, which value is represented by the object.
     *
     * @return name of the field
     */
    @Nonnull
    String getName();

    /**
     * Returns string value (if specified). Must return {@code null} if the value this object holds
     * is not a string value.
     *
     * @return string value of the field.
     */
    @Nullable
    String getStringValue();

    /**
     * Returns list of lists (matrix) of group scope property, if this field holds group scope
     * information. Every upper list (holding {@code List<String>} represents a single entity. Every
     * underlying list represents the entity's field value. This method must return {@code null} if
     * this field does not hold group scope information.
     *
     * @return grop scope data
     */
    @Nullable
    List<List<String>> getGroupScopeProperties();
}
