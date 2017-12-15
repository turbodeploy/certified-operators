package com.vmturbo.topology.processor.group.filter;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.TraversalFilter.StoppingCondition;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.filter.TraversalFilter.TraversalToDepthFilter;
import com.vmturbo.topology.processor.group.filter.TraversalFilter.TraversalToPropertyFilter;
import com.vmturbo.topology.processor.topology.TopologyEntity;

/**
 * A factory for constructing an appropriate filter to perform a search against the topology.
 *
 * TODO: For now only supports filtering on Oid, EntityType, and displayName (these are probably the
 * most commonly used properties in any case.
 *
 * TODO: A more extensible means of property filter creation.
 */
public class TopologyFilterFactory {
    public TopologyFilterFactory() {
        // Nothing to do
    }

    /**
     * Create a group resolver that uses this topology filter factory when creating filters to resolve groups.
     *
     * @return A group resolver that uses this topology filter factory when creating filters to resolve groups.
     */
    public GroupResolver newGroupResolver() {
        return new GroupResolver(this);
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
            case "entityType":
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
                    !stringCriteria.getMatch()
                ));
            // Support oid either as a string or as a numeric filter.
            case "oid":
                return new PropertyFilter(longPredicate(
                    Long.valueOf(stringCriteria.getStringPropertyRegex()),
                    stringCriteria.getMatch() ? ComparisonOperator.EQ : ComparisonOperator.NE,
                    TopologyEntity::getOid
                ));
            default:
                throw new IllegalArgumentException("Unknown string property named: " + stringCriteria);
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
     * @return A predicate.
     */
    @Nonnull
    private Predicate<TopologyEntity> stringPredicate(final String regex,
                                              @Nonnull final Function<TopologyEntity, String> propertyLookup,
                                              final boolean negate) {
        final Pattern pattern = Pattern.compile(regex);
        return negate ?
            entity -> !pattern.matcher(propertyLookup.apply(entity)).find() :
            entity -> pattern.matcher(propertyLookup.apply(entity)).find();
    }
}
