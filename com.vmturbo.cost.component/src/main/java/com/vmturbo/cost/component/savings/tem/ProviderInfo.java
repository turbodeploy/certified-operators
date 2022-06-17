package com.vmturbo.cost.component.savings.tem;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;

/**
 * Interface for implementations of ProviderInfo objects.
 */
public interface ProviderInfo {
    /**
     * Get entity type.
     *
     * @return entity type
     */
    int getEntityType();

    /**
     * Match a discovered entity's provider information with information in an executed action.
     *
     * @param actionSpec The ActionSpec to compare.
     * @param isDestinationCheck true if we're checking the action destination, false if we're
     *                           checking the action source for match.
     * @return true if there's a match, false otherwise.
     */
    boolean matchesAction(ActionSpec actionSpec, boolean isDestinationCheck);
}
