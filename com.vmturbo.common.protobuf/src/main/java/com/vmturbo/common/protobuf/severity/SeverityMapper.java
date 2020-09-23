package com.vmturbo.common.protobuf.severity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;

/**
 * Wraps a map of EntityId -> Severity.
 * Rather than returning null for an unknown entity, it returns NORMAL.
 */
public class SeverityMapper implements SeverityMap {

    private final Map<Long, EntitySeverity> severities;

    public SeverityMapper(@Nonnull final Map<Long, EntitySeverity> severities) {
        this.severities = Objects.requireNonNull(severities);
    }

    /**
     * Create an empty {@link SeverityMap}.
     *
     * @return The {@link SeverityMap}.
     */
    public static SeverityMap empty() {
        return new SeverityMapper(Collections.emptyMap());
    }

    /**
     * Return the severity for an entity by ID. If the entity is unknown, return NORMAL.
     *
     * @param id The id of the entity whose severity should be retrieved..
     * @return the severity for an entity by ID. If the entity is unknown, return NORMAL.
     */
    @Nonnull
    public Severity getSeverity(@Nonnull final Long id) {
        return getAllEntitySeverities(severities.get(id)).stream()
                .max(Enum::compareTo)
                .orElse(Severity.NORMAL);
    }

    /**
     * Calculate the highest severity for the passed in entity OIDs using each entity level
     * severity, and the severity breakdown.
     *
     * @param entityOids The set of entity OIDs.
     * @return calculated highest severity
     */
    @Nonnull
    public Severity calculateSeverity(@Nonnull final Collection<Long> entityOids) {
        return entityOids.stream()
                .map(severities::get)
                .filter(Objects::nonNull)
                .map(this::getAllEntitySeverities)
                .flatMap(Collection::stream)
                .max(Enum::compareTo)
                .orElse(Severity.NORMAL);
    }

    /**
     * Expands all severities from entity level severity and severity breaks with a count
     * greater than 0.
     *
     * @param entitySeverity the entity severity to expand.
     *
     * @return all severities from entity level severity and severity breaks with a count
     *            greater than 0. Returns an empty list if there is no entity severity and
     *            no severity breakdown with a count greater than 0.
     */
    @Nonnull
    private List<Severity> getAllEntitySeverities(@Nullable EntitySeverity entitySeverity) {
        if (entitySeverity == null) {
            return Collections.emptyList();
        }
        final List<Severity> result = new ArrayList<>();
        if (entitySeverity.getSeverityBreakdownMap() != null) {
            for (Map.Entry<Integer, Long> entry : entitySeverity.getSeverityBreakdownMap().entrySet()) {
                if (entry.getKey() != null
                        && entry.getValue() != null
                        && entry.getValue() > 0
                        && entry.getKey() >= 0
                        && Severity.forNumber(entry.getKey()) != null) {
                    result.add(Severity.forNumber(entry.getKey()));
                }
            }
        }
        if (entitySeverity.getSeverity() != null) {
            result.add(entitySeverity.getSeverity());
        }
        return result;
    }

    /**
     * Return the severity breakdown for the given id. Returns null if not found.
     *
     * @param id the id to search for.
     * @return the severity breakdown for the given id. Returns null if not found.
     */
    @Nullable
    public Map<String, Long> getSeverityBreakdown(long id) {
        EntitySeverity entitySeverity = severities.get(id);
        if (entitySeverity == null) {
            return null;
        }
        return entitySeverity.getSeverityBreakdownMap().entrySet().stream()
                .filter(entry -> entry.getKey() != null)
                .collect(Collectors.toMap(
                        entry -> Severity.forNumber(entry.getKey()).name(),
                        Entry::getValue));
    }

    @Override
    public int size() {
        return severities.size();
    }
}
