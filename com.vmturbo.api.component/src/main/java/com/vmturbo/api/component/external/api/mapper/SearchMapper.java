package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Utility class with static methods to facilitate the creation of searches and filters.
 */
public class SearchMapper {

    private static final Logger logger = LogManager.getLogger();

    public static final String ENTITY_TYPE_PROPERTY = "entityType";
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
    public static PropertyFilter stringPropertyFilterRegex(String propName, String value) {
        return stringPropertyFilterRegex(propName, value, true, false);
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
    @Nonnull
    public static PropertyFilter stringPropertyFilterRegex(@Nonnull final String propName,
                                                           @Nonnull final String value,
                                                           final boolean match,
                                                           final boolean caseSensitive) {
        return PropertyFilter.newBuilder()
            .setPropertyName(propName)
            .setStringFilter(stringFilterRegex(value, match, caseSensitive))
            .build();
    }

    /**
     * Create a {@link StringFilter} for a particular search.
     * The resulting filter will attempt to match the given value
     * with that in the corresponding entity property.
     *
     * @param regex The value to search for.
     * @param positiveMatch If true, the value of the entity property should match.
     *                       If false, the value of the entity property  should not match
     * @return A {@link SearchFilter} with the above described properties.
     */
    @Nonnull
    public static StringFilter stringFilterRegex(
            @Nonnull final String regex,
            final boolean positiveMatch,
            final boolean caseSensitive) {
        // in regular expression mapping, we add prefixes and suffixes
        // that Arango understands, to avoid matching only part of the value
        return StringFilter.newBuilder()
            .setStringPropertyRegex(makeFullArangoRegex(regex))
            .setPositiveMatch(positiveMatch)
            .setCaseSensitive(caseSensitive)
            .build();
    }

    /**
     * Create a property filter for exact string matching.
     *
     * @param propName property name to use for the search
     * @param values the list of values to search for
     * @return a property filter
     */
    public static PropertyFilter stringPropertyFilterExact(String propName, Collection<String> values) {
        return stringPropertyFilterExact(propName, values, true, false);
    }

    /**
     * Create a property filter for exact string matching.
     * If match is false, negates the result of the filter. That is, the search results
     * will include objects that do not match, rather than those that do.
     *
     * @param propName property name to use for the search
     * @param values the value to search for
     * @param match If true, the property should match. If false, should return true only
     *              when the match fails.
     * @return a property filter
     */
    @Nonnull
    public static PropertyFilter stringPropertyFilterExact(
            @Nonnull final String propName,
            @Nonnull final Collection<String> values,
            final boolean match,
            final boolean caseSensitive) {
        return PropertyFilter.newBuilder()
                .setPropertyName(propName)
                .setStringFilter(stringFilterExact(values, match, caseSensitive))
                .build();
    }

    /**
     * Create a {@link StringFilter} for a particular exact match search.
     * The resulting filter will attempt to find the value of the corresponding entity
     * property in the provided list of values
     *
     * @param valuesToMatch The value to search for.
     * @param positiveMatch If true, the value of the entity property should match.
     *                      If false, the value of the entity property should not match
     * @return A {@link SearchFilter} with the above described properties.
     */
    @Nonnull
    public static StringFilter stringFilterExact(
            @Nonnull final Collection<String> valuesToMatch,
            final boolean positiveMatch,
            final boolean caseSensitive) {
        return StringFilter.newBuilder()
                .addAllOptions(valuesToMatch)
                .setPositiveMatch(positiveMatch)
                .setCaseSensitive(caseSensitive)
                .build();
    }

    /**
     * Create a property filter for the specified property name and specified search value.
     * @param propName property name to use for the search
     * @param value the value to search for
     * @return a property filter
     */
    @Nonnull
    public static PropertyFilter numericPropertyFilter(
            @Nonnull String propName,
            final long value,
            @Nonnull final ComparisonOperator comparisonOperator) {
        return PropertyFilter.newBuilder()
            .setPropertyName(propName)
            .setNumericFilter(numericFilter(value, comparisonOperator))
            .build();
    }

    @Nonnull
    public static NumericFilter numericFilter(final long value,
                                              @Nonnull final ComparisonOperator comparisonOperator) {
        return NumericFilter.newBuilder()
            .setValue(value)
            .setComparisonOperator(comparisonOperator)
            .build();
    }

    /**
     * Create a map filter for the specified property name and
     * specified expression field coming from the UI.
     *
     * The form of the value of the expression field is expected to be
     * "k=v1|k=v2|...", where k is the key and v1, v2, ... are the possible values.
     * If the expression does not conform to the expected format,
     * then a filter with empty key and values fields is generated.
     *
     * TODO: the expression value coming from the UI is currently unsanitized.
     * It is assumed that the tag keys and values do not contain the characters
     * '=' and '|'.  This is reported as a JIRA issue OM-39039.
     *
     * The filter created allows for multimap properties.  The values of such
     * properties are maps, in which multiple values may correspond to a single key.
     * For example key "user" -> ["peter" and "paul"].
     *
     * @param propName property name to use for the search.
     * @param expField expression field coming from the UI.
     * @param positiveMatch if false, then negate the result of the filter.
     * @return the property filter.
     */
    public static PropertyFilter mapPropertyFilterForMultimapsExact(
            @Nonnull String propName, @Nonnull String expField, boolean positiveMatch
    ) {
        final String[] keyValuePairs = expField.split("\\|");
        String key = null;
        final List<String> values = new ArrayList<>();
        for (String kvp : keyValuePairs) {
            final String[] kv = kvp.split("=");
            if (key == null) {
                key = kv[0];
            } else if (!key.equals(kv[0])) {
                logger.error("Map filter {} contains more than one keys.", expField);
                return emptyMapPropertyFilter(propName);
            }
            if (kv.length == 2 && !kv[1].isEmpty()) {
                values.add(kv[1]);
            }
        }

        final PropertyFilter result =
                PropertyFilter.newBuilder()
                    .setPropertyName(propName)
                    .setMapFilter(
                        MapFilter.newBuilder()
                            .setKey(key == null ? "" : key)
                            .addAllValues(values)
                            .setPositiveMatch(positiveMatch)
                            .build())
                    .build();
        logger.debug("Property filter constructed: {}", result);
        return result;
    }

    /**
     * Create a map filter for the specified property name
     * and specified regex coming from the UI.
     *
     * This filter should match values to the regex expression.
     *
     * TODO: the expression value coming from the UI is currently unsanitized.
     * It is assumed that the tag keys and values do not contain the characters '=' and '|'.
     * This is reported as a JIRA issue OM-39039.
     *
     * The filter created allows for multimap properties.  The values of such
     * properties are maps, in which multiple values may correspond to a single key.
     * For example key "user" -> ["peter" and "paul"].
     *
     * @param key property name to use for the search.
     * @param regex expression field coming from the UI.
     * @param positiveMatch if false, then negate the result of the filter.
     * @return the property filter.
     */
    public static PropertyFilter mapPropertyFilterForMultimapsRegex(
            @Nonnull String propName, @Nonnull String key, @Nonnull String regex, boolean positiveMatch) {
        final PropertyFilter result =
            PropertyFilter.newBuilder()
                    .setPropertyName(propName)
                    .setMapFilter(
                            MapFilter.newBuilder()
                                    .setKey(key)
                                    .setRegex(makeFullArangoRegex(regex))
                                    .setPositiveMatch(positiveMatch)
                                    .build())
                    .build();
        logger.debug("Property filter constructed: {}", result);

        return result;
    }

    /**
     * Regex matching in ArangoDB's query language will match any part
     * of the candidate string to the regex pattern. To force matching
     * of the full string, one needs the regex to start with "^" (matches
     * "start of string" in AQL) and end with "$" (matches "end of string"
     * in AQL). This method wraps a regex so that it represents
     * full-length string matching in AQL.
     *
     * @param regex an AQL regular expression.
     * @return a regex that will make only full matches in AQL.
     */
    @Nonnull
    private static String makeFullArangoRegex(@Nonnull String regex) {
        final String prefix = regex.startsWith("^") ? "" : "^";
        final String suffix = regex.endsWith("$") ? "" : "$";
        return prefix + regex + suffix;
    }

    @Nonnull
    private static PropertyFilter emptyMapPropertyFilter(String propName) {
        return
            PropertyFilter.newBuilder()
                .setPropertyName(propName)
                .setMapFilter(
                    MapFilter.newBuilder().setKey("").build())
                .build();
    }

    /**
     * Create a property filter that searches for instances of the given entity type.
     * @param entityType the entity type to search for, e.g. VirtualMachine, Storage.
     * @return a property filter
     */
    public static PropertyFilter entityTypeFilter(String entityType) {
        return numericPropertyFilter(ENTITY_TYPE_PROPERTY,
                ServiceEntityMapper.fromUIEntityType(entityType), ComparisonOperator.EQ);
    }

    /**
     * Creates a property filter that searches for entity by their display name.
     * The display name is given as an exact match.
     *
     * @param displayName the display name to use for the search
     * @return a property filter
     */
    public static PropertyFilter nameFilterExact(String displayName) {
        return nameFilterExact(displayName, true, false);
    }

    /**
     * Creates a property filter that searches for entity by their display name.
     *
     * @param displayName the display name to use for the search
     * @param match If true, the property should match. If false, should return true only
     *              when the match fails.
     * @return a property filter
     */
    public static PropertyFilter nameFilterExact(
            @Nonnull final String displayName,
            final boolean match,
            final boolean caseSensitive) {
        return
            stringPropertyFilterExact(
                StringConstants.DISPLAY_NAME_ATTR, Collections.singleton(displayName), match, caseSensitive);
    }

    /**
     * Creates a property filter that searches for entity by their display name.
     * The display name is given as a regular expression.
     *
     * @param displayName the display name to use for the search
     * @return a property filter
     */
    public static PropertyFilter nameFilterRegex(String displayName) {
        return nameFilterRegex(displayName, true, false);
    }

    /**
     * Creates a property filter that searches for entity by their display name.
     * @param displayName the display name to use for the search
     * @param match If true, the property should match. If false, should return true only
     *              when the match fails.
     * @return a property filter
     */
    public static PropertyFilter nameFilterRegex(@Nonnull final String displayName,
                                                 final boolean match,
                                                 final boolean caseSensitive) {
        return
            stringPropertyFilterRegex(StringConstants.DISPLAY_NAME_ATTR, displayName, match, caseSensitive);
    }

    /**
     * Creates a property filter that searches for entities by their state.
     * @param state the entity states to use for the search, separated by |
     * @param match If true, the property should match. If false, should return true only
     *              when the match fails.
     * @return a property filter
     */
    @Nonnull
    public static PropertyFilter stateFilter(@Nonnull String state, boolean match) {
        return
            stringPropertyFilterExact(
                    STATE_PROPERTY, Arrays.stream(state.split("\\|")).collect(Collectors.toList()),
                    match, false);
    }

    /**
     * Creates a property filter that finds a specific entity by oid.
     *
     * @param oid the oid of the entity to search.
     * @return the filter.
     */
    public static PropertyFilter idFilter(long oid) {
        return stringPropertyFilterExact(StringConstants.OID, Collections.singleton(Long.toString(oid)));
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
     * Convert a {@link com.vmturbo.common.protobuf.search.Search.Entity} to a {@link ServiceEntityApiDTO}.
     * @param entity the entity to convert
     * @return the to resulting service entity API DTO.
     */
    public static ServiceEntityApiDTO seDTO(@Nonnull Entity entity,
                                            @Nonnull Map<Long, String> targetIdToProbeType) {
        ServiceEntityApiDTO seDTO = new ServiceEntityApiDTO();
        seDTO.setDisplayName(entity.getDisplayName());
        seDTO.setState(UIEntityState.fromEntityState(EntityState.forNumber(entity.getState())).getValue());
        seDTO.setClassName(ServiceEntityMapper.toUIEntityType(entity.getType()));
        seDTO.setUuid(String.valueOf(entity.getOid()));
        // set discoveredBy
        if (entity.getTargetIdsCount() > 0) {
            seDTO.setDiscoveredBy(createDiscoveredBy(String.valueOf(entity.getTargetIdsList().get(0)),
                targetIdToProbeType));
        } else if (targetIdToProbeType.size() > 0) {
            seDTO.setDiscoveredBy(createDiscoveredBy(
                Long.toString(targetIdToProbeType.keySet().iterator().next()),
                targetIdToProbeType));
        }
        return seDTO;
    }

    /**
     * Create a discoveredBy for the se, based on the given target id and probe type map.
     *
     * @param targetId id of the target
     * @param targetIdToProbeType the map from target id to probe type
     * @return TargetApiDTO which represents the discoveredBy field of se
     */
    public static TargetApiDTO createDiscoveredBy(@Nonnull String targetId,
                                                  @Nonnull Map<Long, String> targetIdToProbeType) {
        final TargetApiDTO discoveredBy = new TargetApiDTO();
        discoveredBy.setUuid(targetId);
        discoveredBy.setType(targetIdToProbeType.get(Long.valueOf(targetId)));
        return discoveredBy;
    }

    private static final ImmutableList<UIEntityType> EXCLUDE_FROM_SEARCH_ALL =
                    ImmutableList.of(UIEntityType.INTERNET, UIEntityType.UNKNOWN);

    public static final List<String> SEARCH_ALL_TYPES =
                    ServiceEntityMapper.ENTITY_TYPE_MAPPINGS.values().stream()
                    .filter(e -> !EXCLUDE_FROM_SEARCH_ALL.contains(e))
                    .map(UIEntityType::getValue)
                    .collect(Collectors.toList());
    /**
     * Creates a {@link SearchParameters} builder, and gives it a start filter.
     *
     * @param startFilter the start filter.
     * @return a {@link SearchParameters} filter.
     */
    public static SearchParameters.Builder makeSearchParameters(PropertyFilter startFilter) {
        return SearchParameters.newBuilder().setStartingFilter(startFilter);
    }

    /**
     * Creates a {@link SearchParameters} objects that begins from a specific entity
     * and fetches all its neighbors according to a specific traversal direction.
     *
     * @param oid the oid of the starting entity.
     * @param direction the traversal direction.
     * @return the constructed {@link SearchParameters} filter.
     */
    public static SearchParameters neighbors(long oid, TraversalDirection direction) {
        return
            makeSearchParameters(idFilter(oid))
                .addSearchFilter(
                    SearchFilter.newBuilder().setTraversalFilter(numberOfHops(direction, 1)))
                .build();
    }
}
