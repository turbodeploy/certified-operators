package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.TraversalDirection;

/**
 * Utility class with static methods to facilitate the creation of searches and filters.
 */
public class SearchMapper {

    public static final String ENTITY_TYPE_PROPERTY = "entityType";
    public static final String DISPLAY_NAME_PROPERTY = "displayName";
    public static final String STATE_PROPERTY = "state";

    /**
     * Wrap an instance of {@link PropertyFilter} with a {@link SearchFilter}.
     * @param propFilter the property filter to wrap
     * @return a search filter that wraps the argument
     */
    public static SearchFilter searchFilterProperty(PropertyFilter propFilter) {
        return SearchFilter.newBuilder().setPropertyFilter(propFilter).build();
    }

    /**
     * Wrap an instance of {@link ClusterMembershipFilter} with a {@link SearchFilter}.
     * @param clusterFilter the cluster membership filter to wrap
     * @return a search filter that wraps the argument
     */
    public static SearchFilter searchFilterCluster(ClusterMembershipFilter clusterFilter) {
        return SearchFilter.newBuilder()
                .setClusterMembershipFilter(clusterFilter)
                .build();
    }

    /**
     * Wrap an instance of {@link TraversalFilter} with a {@link SearchFilter}.
     * @param traversalFilter the traversal filter to wrap
     * @return a search filter that wraps the argument
     */
    public static SearchFilter searchFilterTraversal(TraversalFilter traversalFilter) {
        return SearchFilter.newBuilder().setTraversalFilter(traversalFilter).build();
    }

    /**
     * Create a property filter for the specified property name and specified search value.
     * @param propName property name to use for the search
     * @param value the value to search for
     * @return a property filter
     */
    public static PropertyFilter stringFilter(String propName, String value) {
        return stringFilter(propName, value, true);
    }

    /**
     * Create a property filter for the specified property name and specified search value.
     * If match is false, negates the result of the filter. That is, the search results
     * will include objects that do not match, rather than those that do.
     *
     * @param propName property name to use for the search
     * @param value the value to search for
     * @param match If true, the property should match. If false, should return true only
     *              when the match fails.
     * @return a property filter
     */
    public static PropertyFilter stringFilter(String propName, String value, boolean match) {
        return PropertyFilter.newBuilder()
            .setPropertyName(propName)
            .setStringFilter(StringFilter.newBuilder()
                    .setStringPropertyRegex(value)
                    .setMatch(match)
                    .build())
            .build();
    }

    /**
     * Create a property filter for the specified property name and specified search value.
     * @param propName property name to use for the search
     * @param value the value to search for
     * @return a property filter
     */
    public static PropertyFilter numericPropertyFilter(String propName, int value) {
        return PropertyFilter.newBuilder()
                        .setPropertyName(propName)
                        .setNumericFilter(NumericFilter.newBuilder()
                            .setValue(value)
                            .setComparisonOperator(ComparisonOperator.EQ)
                            .build())
                        .build();
    }

    /**
     * Create a property filter that searches for instances of the given entity type.
     * @param entityType the entity type to search for, e.g. VirtualMachine, Storage.
     * @return a property filter
     */
    public static PropertyFilter entityTypeFilter(String entityType) {
        return numericPropertyFilter(ENTITY_TYPE_PROPERTY, ServiceEntityMapper.fromUIEntityType(entityType));
    }

    /**
     * Creates a property filter that searches for entity by their display name.
     * @param displayName the display name to use for the search
     * @return a property filter
     */
    public static PropertyFilter nameFilter(String displayName) {
        return nameFilter(displayName, true);
    }

    /**
     * Creates a property filter that searches for entity by their display name.
     * @param displayName the display name to use for the search
     * @param match If true, the property should match. If false, should return true only
     *              when the match fails.
     * @return a property filter
     */
    public static PropertyFilter nameFilter(String displayName, boolean match) {
        return stringFilter(DISPLAY_NAME_PROPERTY, displayName, match);
    }

    /**
     * Creates a property filter that searches for entities by their state.
     * @param state the entity state to use for the search
     * @param match If true, the property should match. If false, should return true only
     *              when the match fails.
     * @return a property filter
     */
    @Nonnull
    public static PropertyFilter stateFilter(@Nonnull String state, boolean match) {
        return stringFilter(STATE_PROPERTY, state, match);
    }

    /**
     * Create a traversal filter that searches for instances of the provided type
     * in the given direction.
     * @param direction either PRODUCES or CONSUMES
     * @param stopType the entity type to stop at
     * @return a traversal filter
     */
    public static TraversalFilter traverseToType(TraversalDirection direction, String stopType) {
        StoppingCondition stopAtType = StoppingCondition.newBuilder()
                        .setStoppingPropertyFilter(entityTypeFilter(stopType))
                        .build();
        return TraversalFilter.newBuilder()
                        .setTraversalDirection(direction)
                        .setStoppingCondition(stopAtType)
                        .build();
    }

    /**
     * Create a traversal filter that stops after number of hops in the given direction.
     * @param direction either PRODUCES or CONSUMES
     * @param hops number of hops to traverse
     * @return a traversal filter
     */
    public static TraversalFilter numberOfHops(TraversalDirection direction, int hops) {
        StoppingCondition numHops = StoppingCondition.newBuilder()
                        .setNumberHops(hops)
                        .build();
        return TraversalFilter.newBuilder()
                        .setTraversalDirection(direction)
                        .setStoppingCondition(numHops)
                        .build();
    }

    /**
     * Create a {@link ClusterMembershipFilter} based on a cluster property type filter.
     * @return a {@link ClusterMembershipFilter}
     */
    public static ClusterMembershipFilter clusterFilter(PropertyFilter propertyFilter) {
        return ClusterMembershipFilter.newBuilder()
                .setClusterSpecifier(propertyFilter)
                .build();
    }


    /**
     * Convert a {@link Search.Entity} to a {@link ServiceEntityApiDTO}.
     * @param entity the entity to convert
     * @return the to resulting service entity API DTO.
     */
    public static ServiceEntityApiDTO seDTO(Entity entity) {
        ServiceEntityApiDTO seDTO = new ServiceEntityApiDTO();
        seDTO.setDisplayName(entity.getDisplayName());
        seDTO.setState(ServiceEntityMapper.toState(entity.getState()));
        seDTO.setClassName(ServiceEntityMapper.toUIEntityType(entity.getType()));
        seDTO.setUuid(String.valueOf(entity.getOid()));
        return seDTO;
    }

    private static final ImmutableList<UIEntityType> EXCLUDE_FROM_SEARCH_ALL =
                    ImmutableList.of(UIEntityType.INTERNET, UIEntityType.UNKNOWN);

    public static final List<String> SEARCH_ALL_TYPES =
                    ServiceEntityMapper.ENTITY_TYPE_MAPPINGS.values().stream()
                    .filter(e -> !EXCLUDE_FROM_SEARCH_ALL.contains(e))
                    .map(UIEntityType::getValue)
                    .collect(Collectors.toList());

}
