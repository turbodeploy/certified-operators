package com.vmturbo.common.protobuf.search;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.UIEntityType;

public class SearchProtoUtil {

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
        if (regex.charAt(0) == '^' && regex.charAt(regex.length() - 1) == '$') {
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
        return SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.ENTITY_TYPE,
            entityType);
    }

    public static PropertyFilter entityTypeFilter(String entityType) {
        return numericPropertyFilter(SearchableProperties.ENTITY_TYPE,
            UIEntityType.fromString(entityType).typeNumber(), ComparisonOperator.EQ);
    }

    public static PropertyFilter entityTypeFilter(int entityType) {
        return SearchProtoUtil.numericPropertyFilter(SearchableProperties.ENTITY_TYPE,
            entityType, ComparisonOperator.EQ);
    }

    public static PropertyFilter stateFilter(@Nonnull final String state) {
        return stringPropertyFilterExact(SearchableProperties.ENTITY_STATE,
            Arrays.asList(state.split("\\|")),
        true, false);
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
     * Creates a {@link SearchParameters} builder, and gives it a start filter.
     *
     * @param startFilter the start filter.
     * @return a {@link SearchParameters} filter.
     */
    public static SearchParameters.Builder makeSearchParameters(PropertyFilter startFilter) {
        return SearchParameters.newBuilder().setStartingFilter(startFilter);
    }
}
