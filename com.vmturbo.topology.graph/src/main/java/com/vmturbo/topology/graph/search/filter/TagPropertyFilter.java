package com.vmturbo.topology.graph.search.filter;

import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
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
        // Each entity may have its own tag index, or there may be a global optimized tag index.
        // Store the distinct tag indices using identity comparisons, and then check all of them
        // for matching entities.
        final IdentityHashMap<TagIndex, Boolean> tagIndices = new IdentityHashMap<>(1);
        entities.forEach(e -> {
            entitiesById.put(e.getOid(), e);
            SearchableProps searchableProps = e.getSearchableProps(SearchableProps.class);
            if (searchableProps != null) {
                tagIndices.put(searchableProps.getTagIndex(), true);
            }
        });
        if (entitiesById.isEmpty() || tagIndices.keySet().isEmpty()) {
            return Stream.empty();
        } else {
            final LongSet matchedEntities = getMatchedEntities(tagIndices.keySet(), entitiesById::containsKey);
            return entitiesById.values().stream()
                .filter(e -> mapFilter.getPositiveMatch() == matchedEntities.contains(e.getOid()));
        }
    }

    @Nonnull
    private LongSet getMatchedEntities(Set<TagIndex> indices, LongPredicate idPredicate) {
        // Take the returned set of the first index, and add on to it. This optimization is
        // mostly for when we have a shared tag index (e.g. in the repository) and want to avoid
        // allocating an extra set and copying all results into it.
        LongSet allMatched = null;
        for (final TagIndex index : indices) {
            final LongSet indexMatchedEntities = index.getMatchingEntities(mapFilter, idPredicate);
            if (allMatched == null) {
                allMatched = indexMatchedEntities;
            } else {
                allMatched.addAll(indexMatchedEntities);
            }
        }
        // Should not be null, but just in case.
        return allMatched == null ? new LongOpenHashSet() : allMatched;
    }
}
