package com.vmturbo.api.component.external.api.mapper;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;

/**
 * Severity map holds severity data for some part of the environment.
 */
public interface SeverityMap {
    /**
     * Return the severity for an entity by ID. If the entity is unknown, return NORMAL.
     *
     * @param id The id of the entity whose severity should be retrieved..
     * @return the severity for an entity by ID. If the entity is unknown, return NORMAL.
     */
    @Nonnull
    Severity getSeverity(@Nonnull Long id);

    /**
     * Calculate the highest severity for the passed in entity OIDs.
     *
     * @param entityOids The set of entity OIDs.
     * @return calculated highest severity
     */
    @Nonnull
    Severity calculateSeverity(@Nonnull Collection<Long> entityOids);

}
