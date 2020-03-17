package com.vmturbo.topology.graph.search;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;

/**
 * Resolves search queries on a particular {@link TopologyGraph}.
 *
 * @param <E> The type of entities in the {@link TopologyGraph}.
 */
@ThreadSafe
public class SearchResolver<E extends TopologyGraphEntity<E>> {
    private static final Logger logger = LogManager.getLogger();

    private final TopologyFilterFactory<E> filterFactory;

    /**
     * Create a GroupResolver.
     *
     * @param filterFactory The topology filter factory to use when resolving dynamic groups.
     */
    public SearchResolver(@Nonnull final TopologyFilterFactory<E> filterFactory) {
        this.filterFactory = Objects.requireNonNull(filterFactory);
    }

    /**
     * Perform a search using {@link SearchParameters}.
     *
     * @param search The input {@link SearchParameters} describing the search.
     * @param graph The {@link TopologyGraph} to use for the search.
     * @return A {@link Stream} of {@link TopologyGraphEntity}s that match the parameters.
     */
    @Nonnull
    public Stream<E> search(@Nonnull final SearchParameters search,
                            @Nonnull final TopologyGraph<E> graph) {
        Stream<E> matchingEntities = startingEntities(search.getStartingFilter(), graph);

        for (SearchFilter filter : search.getSearchFilterList()) {
            matchingEntities = filterFactory.filterFor(filter).apply(matchingEntities, graph);
        }

        return matchingEntities;
    }

    /**
     * Perform a search using a collection of {@link SearchParameters}.
     *
     * The equivalent of applying {@link SearchResolver#search(SearchParameters, TopologyGraph)}
     * multiple times and intersecting the results.
     *
     * @param search The list of input {@link SearchParameters}.
     * @param graph The {@link TopologyGraph} to use for the search.
     * @return A {@link Stream} of {@link TopologyGraphEntity}s that match all the parameters.
     */
    @Nonnull
    public Stream<E> search(@Nonnull final List<SearchParameters> search,
                            @Nonnull final TopologyGraph<E> graph) {
        if (search.isEmpty()) {
            return Stream.empty();
        } else if (search.size() == 1) {
            return search(search.get(0), graph);
        } else {
            final Map<Long, E> matchingEntitiesById =
                search(search.get(0), graph)
                    .collect(Collectors.toMap(E::getOid, Function.identity()));
            for (int i = 1; i < search.size(); ++i) {
                // TODO (roman, May 20 2019): Can we pass the current matching entities
                // as an additional input to reduce the number of entities considered in
                // the subsequent searches?
                matchingEntitiesById.keySet().retainAll(search(search.get(i), graph)
                    .map(E::getOid)
                    .collect(Collectors.toList()));
            }
            return matchingEntitiesById.values().stream();
        }
    }

    @Nonnull
    private Stream<E> startingEntities(@Nonnull final Search.PropertyFilter startingFilter,
                                       @Nonnull final TopologyGraph<E> graph) {
        // Optimized handling for two common starting filters - entity type and oid.
        if (startingFilter.getPropertyName().equals(SearchableProperties.ENTITY_TYPE)) {
            // The topology graph has an index on entity type.
            // We can use that index to avoid iterating over the whole graph.
            if (startingFilter.hasNumericFilter()) {
                if (startingFilter.getNumericFilter().getComparisonOperator().equals(ComparisonOperator.EQ)) {
                    // In the case where the starting filter is an EQUALS comparison on entity type,
                    // we can accelerate the lookup using the graph.entitiesOfType method.
                    return graph.entitiesOfType((int) startingFilter.getNumericFilter().getValue());
                }
            } else if (startingFilter.hasStringFilter()) {
                StringFilter strFilter = startingFilter.getStringFilter();
                if (!StringUtils.isEmpty(strFilter.getStringPropertyRegex())) {
                    final ApiEntityType entityType = ApiEntityType.fromString(
                        startingFilter.getStringFilter().getStringPropertyRegex());
                    if (startingFilter.getStringFilter().getPositiveMatch()) {
                        return graph.entitiesOfType(entityType.typeNumber());
                    }
                } else {
                    // No regex and no options = no results.
                    if (strFilter.getOptionsCount() == 0) {
                        return Stream.empty();
                    } else {
                        // Use the indices for the types.
                        return strFilter.getOptionsList().stream()
                            .map(ApiEntityType::fromString)
                            .flatMap(type -> graph.entitiesOfType(type.typeNumber()));
                    }
                }
            } else {
                throw new IllegalArgumentException("Entity type filter should be string or numeric. Got: " +
                    startingFilter);
            }
        } else if (startingFilter.getPropertyName().equals(SearchableProperties.OID)) {
            // The entities in the topology graph are arranged by OID.
            // We can use that index to avoid iterating over the whole graph.
            if (startingFilter.hasNumericFilter()) {
                return graph.getEntity(startingFilter.getNumericFilter().getValue())
                    .map(Stream::of)
                    .orElse(Stream.empty());
            } else if (startingFilter.hasStringFilter()) {
                final StringFilter idStrFilder = startingFilter.getStringFilter();
                if (!StringUtils.isEmpty(idStrFilder.getStringPropertyRegex())) {
                    if (StringUtils.isNumeric(idStrFilder.getStringPropertyRegex())) {
                        final long oid = Long.valueOf(idStrFilder.getStringPropertyRegex());
                        if (idStrFilder.getPositiveMatch()) {
                            return graph.getEntity(oid)
                                .map(Stream::of)
                                .orElse(Stream.empty());
                        } else {
                            // This is kind of a stupid use case for a starting filter.
                            return graph.entities()
                                .filter(entity -> entity.getOid() != oid);
                        }
                    }
                } else {
                    // If no options are provided, nothing matches.
                    if (idStrFilder.getOptionsList().isEmpty()) {
                        return Stream.empty();
                    } else {
                        // The options should be numeric.
                        final Set<Long> targetIds = idStrFilder.getOptionsList().stream()
                            .map(Long::valueOf)
                            .collect(Collectors.toSet());
                        if (idStrFilder.getPositiveMatch()) {
                            return graph.getEntities(targetIds);
                        } else {
                            return graph.entities()
                                .filter(entity -> !targetIds.contains(entity.getOid()));
                        }
                    }
                }
            }
        }

        // The non-optimized case - use a regular property filter.
        return filterFactory.filterFor(startingFilter)
                .apply(graph.entities(), graph);
    }
}
