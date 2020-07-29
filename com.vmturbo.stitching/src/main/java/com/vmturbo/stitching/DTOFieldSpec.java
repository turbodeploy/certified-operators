package com.vmturbo.stitching;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * A class representing the field in a DTO that should be pushed from the internal entity to the
 * external entity during stitching.  Each DTOFieldSpec represents a (potential) sequence of calls
 * to the MessageOrBuilder needed to get to a field in the DTO and the name of the actual field.
 * For example, for stitching the controllable value of the consumerPolicy, we would pass the list
 * {"consumerPolicy"} and the field name would be "controllable"}.
 */
public interface DTOFieldSpec {

    /**
     * Returns name of the wrapped field.
     *
     * @return name of the wrapped field.
     */
    @Nonnull
    String getFieldName();

    /**
     * Returns message paths which are pointing to the wrapped field.
     *
     * @return message paths which are pointing to the wrapped field.
     */
    @Nonnull
    List<String> getMessagePath();
}
