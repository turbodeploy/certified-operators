package com.vmturbo.topology.graph.search.filter;

import java.util.function.LongPredicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.topology.graph.SearchableProps;
import com.vmturbo.topology.graph.TagIndex;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphSearchableEntity;

/**
 * An optimized filter for applying a {@link MapFilter} to a {@link TagIndex}.
 * When checking which entities in a list have tags matching a particular filter, we can get all
 * matching entities with one call to {@link TagIndex#getMatchingEntities(MapFilter, LongPredicate)}.
 *
 * @param <E> The topology entity type.
 */
public class TagPropertyFilter<E extends TopologyGraphSearchableEntity<E>> extends PropertyFilter<E> {

    private static final Logger logger = LogManager.getLogger();

    private final MapFilter mapFilter;

    TagPropertyFilter(MapFilter mapCriteria) {
        super(e -> false);
        this.mapFilter = mapCriteria;
    }

    @Nonnull
    @Override
    public Stream<E> apply(@Nonnull Stream<E> entities, @Nonnull TopologyGraph<E> graph) {
        final Long2ObjectMap<E> entitiesById = new Long2ObjectOpenHashMap<>();
        entities.forEach(e -> entitiesById.put(e.getOid(), e));
        if (entitiesById.isEmpty()) {
            return Stream.empty();
        } else {
            E firstEntity = entitiesById.values().iterator().next();
            SearchableProps searchableProps = firstEntity.getSearchableProps(SearchableProps.class);
            if (searchableProps == null) {
                // This shouldn't happen, because all SearchableProps implementations implement
                // the base interface.
                logger.warn("Unable to find searchable props for entity {}", firstEntity);
                return Stream.empty();
            }
            final TagIndex tagIndex = searchableProps.getTagIndex();
            final LongSet matchedEntities = tagIndex.getMatchingEntities(mapFilter, entitiesById::containsKey);
            return entitiesById.values().stream()
                    .filter(e -> matchedEntities.contains(e.getOid()));
        }
    }
}
