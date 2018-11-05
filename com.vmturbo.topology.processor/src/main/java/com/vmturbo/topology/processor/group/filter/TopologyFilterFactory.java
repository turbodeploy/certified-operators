package com.vmturbo.topology.processor.group.filter;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.filter.TraversalFilter.TraversalToDepthFilter;
import com.vmturbo.topology.processor.group.filter.TraversalFilter.TraversalToPropertyFilter;

/**
 * A factory for constructing an appropriate filter to perform a search against the topology.
 *
 * TODO: For now only supports filtering on Oid, EntityType, displayName, and tags (these are probably the
 * most commonly used properties in any case).
 *
 * TODO: A more extensible means of property filter creation.
 */
public class TopologyFilterFactory {
    public static final String ENTITY_TYPE_PROPERTY_NAME = "entityType";
    public static final String TAGS_TYPE_PROPERTY_NAME = "tags";

    public TopologyFilterFactory() {
        // Nothing to do
    }

    /**
     * Construct a filter for a generic {@link SearchFilter}.
     *
     * @param searchCriteria The criteria that define the search that should be created.
     * @return A Filter that corresponds to the input criteria.
     */
    @Nonnull
    public TopologyFilter filterFor(@Nonnull final SearchFilter searchCriteria) {
        switch (searchCriteria.getFilterTypeCase()) {
            case PROPERTY_FILTER:
                return filterFor(searchCriteria.getPropertyFilter());
            case TRAVERSAL_FILTER:
                return filterFor(searchCriteria.getTraversalFilter());
            default:
                throw new IllegalArgumentException("Unknown FilterTypeCase: " + searchCriteria.getFilterTypeCase());
        }
    }

    /**
     * Construct a filter for a particular {@link com.vmturbo.common.protobuf.search.Search.PropertyFilter}.
     *
     * @param propertyFilterCriteria The criteria that define the property filter that should be created.
     * @return A filter that corresponds to the input criteria.
     */
    @Nonnull
    public PropertyFilter filterFor(@Nonnull final Search.PropertyFilter propertyFilterCriteria) {
        switch (propertyFilterCriteria.getPropertyTypeCase()) {
            case NUMERIC_FILTER:
                return numericFilter(propertyFilterCriteria.getPropertyName(),
                    propertyFilterCriteria.getNumericFilter());
            case STRING_FILTER:
                return stringFilter(propertyFilterCriteria.getPropertyName(),
                    propertyFilterCriteria.getStringFilter());
            case MAP_FILTER:
                return mapFilter(
                        propertyFilterCriteria.getPropertyName(),
                        propertyFilterCriteria.getMapFilter());
            default:
                throw new IllegalArgumentException("Unknown PropertyTypeCase: " +
                    propertyFilterCriteria.getPropertyTypeCase());
        }
    }

    /**
     * Construct a filter for a particular {@link SearchFilter.TraversalFilter}.
     *
     * @param traversalCriteria The criteria that define the traversal filter that should be created.
     * @return A filter that corresponds to the input criteria.
     */
    @Nonnull
    private TraversalFilter filterFor(@Nonnull final SearchFilter.TraversalFilter traversalCriteria) {
        final StoppingCondition stoppingCondition = traversalCriteria.getStoppingCondition();
        switch (stoppingCondition.getStoppingConditionTypeCase()) {
            case NUMBER_HOPS:
                return new TraversalToDepthFilter(traversalCriteria.getTraversalDirection(),
                    stoppingCondition.getNumberHops());
            case STOPPING_PROPERTY_FILTER:
                return new TraversalToPropertyFilter(traversalCriteria.getTraversalDirection(),
                    filterFor(stoppingCondition.getStoppingPropertyFilter()));
            default:
                throw new IllegalArgumentException("Unknown StoppingConditionTypeCase: " +
                    stoppingCondition.getStoppingConditionTypeCase());
        }
    }

    @Nonnull
    private PropertyFilter numericFilter(@Nonnull final String propertyName,
                                        @Nonnull final Search.PropertyFilter.NumericFilter numericCriteria) {
        switch (propertyName) {
            case "oid":
                return new PropertyFilter(longPredicate(
                    numericCriteria.getValue(),
                    numericCriteria.getComparisonOperator(),
                    TopologyEntity::getOid
                ));
            case ENTITY_TYPE_PROPERTY_NAME:
                return new PropertyFilter(intPredicate(
                    (int) numericCriteria.getValue(),
                    numericCriteria.getComparisonOperator(),
                    TopologyEntity::getEntityType
                ));
            default:
                throw new IllegalArgumentException("Unknown numeric property named: " + propertyName);
        }
    }

    @Nonnull
    private PropertyFilter stringFilter(@Nonnull final String propertyName,
                                        @Nonnull final Search.PropertyFilter.StringFilter stringCriteria) {
        switch (propertyName) {
            case "displayName":
                return new PropertyFilter(stringPredicate(
                    stringCriteria.getStringPropertyRegex(),
                    entity -> entity.getTopologyEntityDtoBuilder().getDisplayName(),
                    !stringCriteria.getMatch(),
                    stringCriteria.getCaseSensitive()
                ));
            // Support oid either as a string or as a numeric filter.
            case "oid":
                if (StringUtils.isNumeric(stringCriteria.getStringPropertyRegex())) {
                    // the string regex is an oid and can be represented as a numeric equals filter.
                    return new PropertyFilter(longPredicate(
                            Long.valueOf(stringCriteria.getStringPropertyRegex()),
                            stringCriteria.getMatch() ? ComparisonOperator.EQ : ComparisonOperator.NE,
                            TopologyEntity::getOid
                    ));
                } else {
                    // the string regex is really a regex instead of a string-encoded long -- create
                    // a string predicate to handle the oid matching.
                    return new PropertyFilter(stringPredicate(
                            stringCriteria.getStringPropertyRegex(),
                            entity -> String.valueOf(entity.getOid()),
                            !stringCriteria.getMatch(),
                            stringCriteria.getCaseSensitive()
                    ));
                }
            case "state":
                // Note - we are ignoring the case-sensitivity parameter. Since the state is an
                // enum it doesn't really make sense to be case-sensitive.
                final UIEntityState targetState =
                        UIEntityState.fromString(stringCriteria.getStringPropertyRegex());

                // If the target state resolves to "UNKNOWN" but "UNKNOWN" wasn't what the user
                // explicitly wanted, we throw an exception to avoid weird behaviour.
                if (targetState == UIEntityState.UNKNOWN &&
                        !StringUtils.equalsIgnoreCase(UIEntityState.UNKNOWN.getValue(),
                                stringCriteria.getStringPropertyRegex())) {
                    throw new IllegalArgumentException("Desired state: " +
                            stringCriteria.getStringPropertyRegex() +
                            " doesn't match a known/valid entity state.");
                }
                // It's more efficient to compare the numeric value of the enum.
                return new PropertyFilter(intPredicate(targetState.toEntityState().getNumber(),
                        stringCriteria.getMatch() ? ComparisonOperator.EQ : ComparisonOperator.NE,
                        entity -> entity.getEntityState().getNumber()));
            case "environmentType":
                final UIEnvironmentType targetType =
                        UIEnvironmentType.fromString(stringCriteria.getStringPropertyRegex());

                // If the target type resolves to "UNKNOWN" but "UNKNOWN" wasn't what the user
                // explicitly wanted, throw an exception to get an early-exit.
                if (targetType == UIEnvironmentType.UNKNOWN &&
                    !StringUtils.equalsIgnoreCase(UIEnvironmentType.UNKNOWN.getApiEnumStringValue(),
                        stringCriteria.getStringPropertyRegex())) {
                    throw new IllegalArgumentException("Desired env type: " +
                            stringCriteria.getStringPropertyRegex() +
                            " doesn't match a known/valid env type.");
                }
                // It's more efficient to compare the numeric value of the enum.
                return new PropertyFilter(intPredicate(targetType.toEnvType().getNumber(),
                        stringCriteria.getMatch() ? ComparisonOperator.EQ : ComparisonOperator.NE,
                        entity -> entity.getEnvironmentType().getNumber()));
            default:
                throw new IllegalArgumentException("Unknown string property: " + propertyName
                        + " with criteria: " + stringCriteria);
        }
    }

    @Nonnull
    private PropertyFilter mapFilter(
            @Nonnull final String propertyName,
            @Nonnull final Search.PropertyFilter.MapFilter mapCriteria) {
        if (!mapCriteria.hasKey()) {
            throw new IllegalArgumentException("Map filter without key value: " + mapCriteria.toString());
        }
        final Predicate<String> valueFilter;
        if (!mapCriteria.getValuesList().isEmpty()) {
            valueFilter = v -> mapCriteria.getValuesList().contains(v);
        } else {
            valueFilter = v -> true;
        }

        // currently only entity tags is a property of type map
        if (propertyName.equals(TAGS_TYPE_PROPERTY_NAME)) {
            return new PropertyFilter(te ->
                te.getTopologyEntityDtoBuilder().getTagsMap().entrySet().stream()
                        .anyMatch(e ->
                                e.getKey().equals(mapCriteria.getKey()) &&
                                e.getValue().getValuesList().stream().anyMatch(valueFilter)));
        } else {
            throw new IllegalArgumentException("Unknown map property named: " + propertyName);
        }
    }

    /**
     * Compose a int-based predicate for use in a numeric filter based on a given comparison value,
     * operation, and lookup method.
     *
     * @param comparisonValue The value to compare the lookup value against.
     * @param operator The operation to apply in the comparison.
     * @param propertyLookup The function to use to lookup an int-value from a given {@link TopologyEntity}.
     * @return A predicate.
     */
    @Nonnull
    private Predicate<TopologyEntity> intPredicate(final int comparisonValue,
                                                 @Nonnull final ComparisonOperator operator,
                                                 @Nonnull final ToIntFunction<TopologyEntity> propertyLookup) {
        Objects.requireNonNull(propertyLookup);

        switch (operator) {
            case EQ:
                return entity -> propertyLookup.applyAsInt(entity) == comparisonValue;
            case NE:
                return entity -> propertyLookup.applyAsInt(entity) != comparisonValue;
            case GT:
                return entity -> propertyLookup.applyAsInt(entity) > comparisonValue;
            case GTE:
                return entity -> propertyLookup.applyAsInt(entity) >= comparisonValue;
            case LT:
                return entity -> propertyLookup.applyAsInt(entity) < comparisonValue;
            case LTE:
                return entity -> propertyLookup.applyAsInt(entity) <= comparisonValue;
            default:
                throw new IllegalArgumentException("Unknown operator type: " + operator);
        }
    }

    /**
     * Compose a long-based predicate for use in a numeric filter based on a given comparison value,
     * operation, and lookup method.
     *
     * @param comparisonValue The value to compare the lookup value against.
     * @param operator The operation to apply in the comparison.
     * @param propertyLookup The function to use to lookup an int-value from a given {@link TopologyEntity}.
     * @return A predicate.
     */
    @Nonnull
    private Predicate<TopologyEntity> longPredicate(final long comparisonValue,
                                                  @Nonnull final ComparisonOperator operator,
                                                  @Nonnull final ToLongFunction<TopologyEntity> propertyLookup) {
        Objects.requireNonNull(propertyLookup);

        switch (operator) {
            case EQ:
                return entity -> propertyLookup.applyAsLong(entity) == comparisonValue;
            case NE:
                return entity -> propertyLookup.applyAsLong(entity) != comparisonValue;
            case GT:
                return entity -> propertyLookup.applyAsLong(entity) > comparisonValue;
            case GTE:
                return entity -> propertyLookup.applyAsLong(entity) >= comparisonValue;
            case LT:
                return entity -> propertyLookup.applyAsLong(entity) < comparisonValue;
            case LTE:
                return entity -> propertyLookup.applyAsLong(entity) <= comparisonValue;
            default:
                throw new IllegalArgumentException("Unknown operator type: " + operator);
        }
    }

    /**
     * Compose a string-based predicate for use in a string filter that filters based on a regex.
     *
     * @param regex The regular expression to use when filtering entities.
     * @param propertyLookup The function to use to lookup an int-value from a given {@link TopologyEntity}.
     * @param negate If true, return the opposite of the match. That is, if true return false
     *               if the match succeeds. If false, return the same as the match.
     * @param caseSensitive If true, match the case of the regex. If false, do a case-insensitive comparison.
     * @return A predicate.
     */
    @Nonnull
    private Predicate<TopologyEntity> stringPredicate(final String regex,
                                              @Nonnull final Function<TopologyEntity, String> propertyLookup,
                                              final boolean negate,
                                              final boolean caseSensitive) {
        final Pattern pattern = Pattern.compile(regex, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
        return negate ?
            entity -> !pattern.matcher(propertyLookup.apply(entity)).find() :
            entity -> pattern.matcher(propertyLookup.apply(entity)).find();
    }
}
