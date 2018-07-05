package com.vmturbo.stitching;

import java.util.List;

/**
 * A class representing the field in a DTO that should be pushed from the internal entity to the
 * external entity during stitching.  Each DTOFieldSpec represents a (potential) sequence of calls
 * to the MessageOrBuilder needed to get to a field in the DTO and the name of the actual field.
 * For example, for stitching the controllable value of the consumerPolicy, we would pass the list
 * {"consumerPolicy"} and the field name would be "controllable"}.
 */
public interface DTOFieldSpec {
    String getFieldName();
    List<String> getMessagePath();
}
