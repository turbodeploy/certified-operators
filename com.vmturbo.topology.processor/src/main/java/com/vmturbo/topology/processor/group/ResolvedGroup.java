package com.vmturbo.topology.processor.group;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility class for a group, the dynamic filters of which have been resolved against the
 * topology currently under construction. The entities in the group are accessible by type.
 */
public class ResolvedGroup {
    private final Grouping group;
    private final Map<ApiEntityType, Set<Long>> entitiesByType;

    /**
     * Create a new {@link ResolvedGroup}.
     *
     * @param group The {@link Grouping} that was resolved.
     * @param entitiesByType The member entities, arranged by type.
     */
    public ResolvedGroup(@Nonnull final Grouping group,
                         @Nonnull final Map<ApiEntityType, Set<Long>> entitiesByType) {
        this.group = group;
        this.entitiesByType = entitiesByType;
    }

    /**
     * Return the {@link Grouping} for this group.
     *
     * @return The {@link Grouping}.
     */
    @Nonnull
    public Grouping getGroup() {
        return group;
    }

    /**
     * Get all entities in the group, broken down by type.
     *
     * @return Map of type to the entities of that type.
     */
    @Nonnull
    public Map<ApiEntityType, Set<Long>> getEntitiesByType() {
        return Collections.unmodifiableMap(entitiesByType);
    }

    /**
     * Get entities of a particular type.
     *
     * @param type The type.
     * @return Entities in this group of that type.
     */
    @Nonnull
    public Set<Long> getEntitiesOfType(@Nonnull final ApiEntityType type) {
        return Collections.unmodifiableSet(entitiesByType.getOrDefault(type, Collections.emptySet()));
    }

    /**
     * Get entities of a particular set of types.
     *
     * @param types The types.
     * @return Entities in this group matching the input set of types.
     */
    @Nonnull
    public Set<Long> getEntitiesOfTypes(@Nonnull final Set<ApiEntityType> types) {
        Set<Long> retSet = new HashSet<>();
        types.forEach(type -> retSet.addAll(getEntitiesOfType(type)));
        return retSet;
    }

    /**
     * Get the entities of a particular set of SDK types.
     *
     * @param types The types.
     * @return Entities in this group matching the input set of types.
     */
    @Nonnull
    public Set<Long> getEntitiesOfSdkTypes(@Nonnull final Set<EntityType> types) {
        Set<Long> retSet = new HashSet<>();
        types.forEach(type -> retSet.addAll(getEntitiesOfType(ApiEntityType.fromType(type.getNumber()))));
        return retSet;
    }

    /**
     * Get all entities that are members of the group.
     *
     * @return The set of entities, regardless of type.
     */
    @Nonnull
    public Set<Long> getAllEntities() {
        return entitiesByType.values().stream()
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    }
}
