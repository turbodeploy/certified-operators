package com.vmturbo.common.protobuf.search;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class SearchProtoUtil {

    private static final ImmutableList<ApiEntityType> EXCLUDE_FROM_SEARCH_ALL =
                    ImmutableList.of(ApiEntityType.INTERNET, ApiEntityType.HYPERVISOR_SERVER, ApiEntityType.UNKNOWN);

    /**
     * All entity types that can be used in a search.
     */
    public static final List<String> SEARCH_ALL_TYPES = Stream.of(ApiEntityType.values())
                    .filter(e -> !EXCLUDE_FROM_SEARCH_ALL.contains(e))
                    .map(ApiEntityType::apiStr)
                    .collect(Collectors.toList());

    /**
     * Create a filter for environment type.
     * We are using a string filter against the API string
     * that represents the environment type.
     * TODO: environment filters should be made more efficient
     *
     * @param environmentType the environment type the filter matches against
     * @return the environment type filter
     */
    @Nonnull
    public static PropertyFilter environmentTypeFilter(@Nonnull EnvironmentType environmentType) {
        return PropertyFilter.newBuilder()
                    .setPropertyName(SearchableProperties.ENVIRONMENT_TYPE)
                    .setStringFilter(stringFilterExact(
                            Collections.singleton(EnvironmentTypeUtil.toApiString(environmentType)),
                            true, false))
                    .build();
    }

    /**
     * Wrap an instance of {@link PropertyFilter} with a {@link SearchFilter}.
     * @param propFilter the property filter to wrap
     * @return a search filter that wraps the argument
     */
    public static SearchFilter searchFilterProperty(PropertyFilter propFilter) {
        return SearchFilter.newBuilder().setPropertyFilter(propFilter).build();
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
            .setStringPropertyRegex(makeFullRegex(regex))
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
     *
     * @param propName property name to use for the search
     * @param values the list of values to search for
     * @param positiveMatch boolean flag which defined match entity property or not
     * @return a property filter
     */
    public static PropertyFilter stringPropertyFilterExact(String propName,
            Collection<String> values, boolean positiveMatch) {
        return stringPropertyFilterExact(propName, values, positiveMatch, false);
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
     * Strip the start and end character of regular expression
     * @param regex expression to be stripped
     * @return the stripped string
     */
    public static String stripFullRegex(String regex) {
        //check that we are stripping a leading ^ and a trailing $
        if (!regex.isEmpty() && regex.charAt(0) == '^' && regex.charAt(regex.length() - 1) == '$') {
            return regex.substring(1, regex.length() - 1);
        }
        return regex;
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
    public static String makeFullRegex(@Nonnull String regex) {
        final String prefix = regex.startsWith("^") ? "" : "^";
        final String suffix = regex.endsWith("$") ? "" : "$";
        return prefix + regex + suffix;
    }

    /**
     * Create a property filter that searches for instances of the given entity type.
     * @param entityType the entity type to search for, e.g. VirtualMachine, Storage.
     * @return a property filter
     */
    public static PropertyFilter entityTypeFilter(Collection<String> entityType) {
        if (entityType.size() == 1) {
            return numericPropertyFilter(SearchableProperties.ENTITY_TYPE,
                ApiEntityType.fromString(entityType.iterator().next()).typeNumber(), ComparisonOperator.EQ);
        } else {
            return SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.ENTITY_TYPE,
                entityType);
        }
    }

    public static PropertyFilter entityTypeFilter(ApiEntityType entityType) {
        return numericPropertyFilter(SearchableProperties.ENTITY_TYPE,
            entityType.typeNumber(), ComparisonOperator.EQ);
    }

    public static PropertyFilter entityTypeFilter(String entityType) {
        return numericPropertyFilter(SearchableProperties.ENTITY_TYPE,
            ApiEntityType.fromString(entityType).typeNumber(), ComparisonOperator.EQ);
    }

    public static PropertyFilter entityTypeFilter(int entityType) {
        return SearchProtoUtil.numericPropertyFilter(SearchableProperties.ENTITY_TYPE,
            entityType, ComparisonOperator.EQ);
    }

    @Nonnull
    public static PropertyFilter stateFilter(@Nonnull final UIEntityState state) {
        return numericPropertyFilter(SearchableProperties.ENTITY_STATE,
            state.toEntityState().getNumber(), ComparisonOperator.EQ);
    }

    public static PropertyFilter stateFilter(@Nonnull final String state) {
        return stringPropertyFilterExact(SearchableProperties.ENTITY_STATE,
            Arrays.asList(state.split("\\|")),
        true, false);
    }

    public static PropertyFilter discoveredBy(final long targetId) {
        return discoveredBy(Collections.singleton(targetId));
    }

    public static PropertyFilter discoveredBy(final Collection<Long> targetIds) {
        final StringFilter.Builder strFilterBldr = StringFilter.newBuilder();
        for (long targetId : targetIds) {
            strFilterBldr.addOptions(Long.toString(targetId));
        }

        return PropertyFilter.newBuilder()
            .setPropertyName(SearchableProperties.DISCOVERED_BY_TARGET)
            .setListFilter(ListFilter.newBuilder()
                .setStringFilter(strFilterBldr))
            .build();
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
    private static PropertyFilter nameFilterExact(
            @Nonnull final String displayName,
            final boolean match,
            final boolean caseSensitive) {
        return
            stringPropertyFilterExact(SearchableProperties.DISPLAY_NAME, Collections.singleton(displayName), match, caseSensitive);
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
            stringPropertyFilterRegex(SearchableProperties.DISPLAY_NAME, displayName, match, caseSensitive);
    }

    /**
     * Creates a property filter that finds a specific entity by oid.
     *
     * @param oid the oid of the entity to search.
     * @return the filter.
     */
    public static PropertyFilter idFilter(long oid) {
        return stringPropertyFilterExact(SearchableProperties.OID, Collections.singleton(Long.toString(oid)));
    }

    public static PropertyFilter idFilter(Collection<Long> oids) {
        return stringPropertyFilterExact(SearchableProperties.OID, oids.stream()
            .map(oid -> Long.toString(oid))
            .collect(Collectors.toSet()));
    }

    /**
     * Creates a property filter that finds specific entities by oids and positiveMatch flag.
     *
     * @param oids the oid of the entity to search
     * @param positiveMatch positiveMatch flag
     * @return the filter
     */
    public static PropertyFilter idFilter(Collection<Long> oids, boolean positiveMatch) {
        return stringPropertyFilterExact(SearchableProperties.OID, oids.stream()
                .map(oid -> Long.toString(oid))
                .collect(Collectors.toSet()), positiveMatch);
    }

    /**
     * Creates a property filter that finds business accounts which have set associatedTargetId
     * property (monitored by probe).
     *
     * @return the filter
     */
    public static PropertyFilter associatedTargetFilter() {
        return PropertyFilter.newBuilder()
                .setNumericFilter(NumericFilter.getDefaultInstance())
                .setPropertyName(SearchableProperties.ASSOCIATED_TARGET_ID)
                .build();
    }

    /**
     * Create a traversal filter that searches for instances of the provided type
     * in the given direction.
     *
     * @param direction traversal direction
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
     * Create a traversal filter that searches for instances of the provided type
     * in the given direction.
     *
     * @param direction traversal direction
     * @param stopType the entity type to stop at
     * @return a traversal filter
     */
    public static TraversalFilter traverseToType(TraversalDirection direction, EntityType stopType) {
        StoppingCondition stopAtType = StoppingCondition.newBuilder()
                .setStoppingPropertyFilter(entityTypeFilter(stopType.getNumber()))
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
     * Creates a {@link SearchParameters} objects that begins from a specific entity
     * and fetches all its neighbors according to a specific traversal direction.
     *
     * @param oid the oid of the starting entity.
     * @param direction the traversal direction.
     * @return the constructed {@link SearchParameters} filter.
     */
    public static SearchParameters neighbors(long oid, TraversalDirection direction) {
        return
            makeSearchParameters(SearchProtoUtil.idFilter(oid))
                .addSearchFilter(
                    SearchFilter.newBuilder().setTraversalFilter(SearchProtoUtil.numberOfHops(direction, 1)))
                .build();
    }

    /**
     * Creates a {@link SearchParameters} objects that begins from a specific entity
     * and fetches all its neighbors according to a specific traversal direction.
     *
     * @param oid the oid of the starting entity.
     * @param direction the traversal direction.
     * @return the constructed {@link SearchParameters} filter.
     */
    public static SearchParameters neighborsOfType(long oid, TraversalDirection direction, final ApiEntityType type) {
        return
            makeSearchParameters(SearchProtoUtil.idFilter(oid))
                .addSearchFilter(
                    SearchFilter.newBuilder().setTraversalFilter(SearchProtoUtil.numberOfHops(direction, 1)))
                .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                    SearchProtoUtil.entityTypeFilter(type.apiStr())))
                .build();
    }

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
     * Get the entity type from the search parameters, if exists.
     * If the search parameters don't have an entity type - return UNKNOWN.
     *
     * @param searchParameters the search parameters
     * @return the entity type of this search parameter, if exists, UNKNOWN otherwise
     */
    public static ApiEntityType getEntityTypeFromSearchParameters(SearchParameters searchParameters) {
        if (searchParameters.hasStartingFilter()) {
            PropertyFilter startingFilter = searchParameters.getStartingFilter();
            if (SearchableProperties.ENTITY_TYPE.equals(startingFilter.getPropertyName())) {
                if (startingFilter.hasNumericFilter()) {
                    return ApiEntityType.fromType((int)startingFilter.getNumericFilter().getValue());
                } else if (startingFilter.hasStringFilter()) {
                    return ApiEntityType.fromString(startingFilter.getStringFilter().getStringPropertyRegex());
                }
            }
        }
        return ApiEntityType.UNKNOWN;
    }
}
