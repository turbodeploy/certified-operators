package com.vmturbo.stitching.utilities;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.StitchingEntity;

/**
 * A filter utility that knows how to filter streams of entities in various ways.
 */
public class EntityScopeFilters {
    /**
     * Obtain a stream of all entities with multiple instances as determined by two or more instances having
     * the same OID. That is, if the OIDs on the input stream look similar to: <1,2,3,3,1,4>, then the returned
     * OIDs of the entities in the returned stream will look similar to <(3,3),(1,1)> (though no
     * guarantees are made about the order).
     *
     * @param entities The entities to be grouped and filtered by OID.
     * @return A stream of the collections of all shared entities as determined by their having the same OID.
     */
    public static Stream<List<StitchingEntity>> sharedEntitiesByOid(@Nonnull final Stream<StitchingEntity> entities) {
        final Map<Long, List<StitchingEntity>> entitiesByOid =
            entities.collect(Collectors.groupingBy(StitchingEntity::getOid));

        return entitiesByOid.values().stream()
            .filter(entityGroup -> entityGroup.size() > 1);
    }
}
