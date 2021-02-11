package com.vmturbo.group.group;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.api.EntitySeverityClientCache;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;

/**
 * Utility class responsible for calculating severity.
 */
public class GroupSeverityCalculator {

    private final EntitySeverityClientCache entitySeverityCache;

    /**
     * Constructor.
     *
     * @param entitySeverityClientCache a cache that holds the severities of all entities.
     */
    public GroupSeverityCalculator(@Nonnull EntitySeverityClientCache entitySeverityClientCache) {
        this.entitySeverityCache = entitySeverityClientCache;
    }

    /**
     * Calculates severity for a group, given its entities.
     *
     * @param groupEntities the group's entities.
     * @return group's severity.
     */
    public Severity calculateSeverity(Set<Long> groupEntities) {
        if (groupEntities.isEmpty()) {
            return Severity.NORMAL;
        }
        return groupEntities.stream()
                .map(entitySeverityCache::getEntitySeverity)
                .filter(Objects::nonNull)
                .max(Enum::compareTo)
                .orElse(Severity.NORMAL);
    }
}
