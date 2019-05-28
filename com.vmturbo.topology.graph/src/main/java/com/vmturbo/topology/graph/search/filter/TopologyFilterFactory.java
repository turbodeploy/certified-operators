package com.vmturbo.topology.graph.search.filter;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter.ListElementTypeCase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.PropertyTypeCase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToDepthFilter;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToPropertyFilter;

/**
 * A factory for constructing an appropriate filter to perform a search against the topology.
 *
 * @param <E> The type of {@link TopologyGraphEntity} the filters produced by this factory work with.
 */
@ThreadSafe
public class TopologyFilterFactory<E extends TopologyGraphEntity<E>> {

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
    public TopologyFilter<E> filterFor(@Nonnull final SearchFilter searchCriteria) {
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
    public PropertyFilter<E> filterFor(@Nonnull final Search.PropertyFilter propertyFilterCriteria) {
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
            case LIST_FILTER:
                return listFilter(propertyFilterCriteria.getPropertyName(),
                        propertyFilterCriteria.getListFilter());
            case OBJECT_FILTER:
                return objectFilter(propertyFilterCriteria.getPropertyName(),
                        propertyFilterCriteria.getObjectFilter());
            default:
                throw new IllegalArgumentException("Unknown PropertyTypeCase: " +
                    propertyFilterCriteria.getPropertyTypeCase());
        }
    }

    /**
     * Construct a filter for a particular {@link TraversalFilter}.
     *
     * @param traversalCriteria The criteria that define the traversal filter that should be created.
     * @return A filter that corresponds to the input criteria.
     */
    @Nonnull
    private TraversalFilter<E> filterFor(@Nonnull final Search.TraversalFilter traversalCriteria) {
        final StoppingCondition stoppingCondition = traversalCriteria.getStoppingCondition();
        switch (stoppingCondition.getStoppingConditionTypeCase()) {
            case NUMBER_HOPS:
                return new TraversalToDepthFilter<>(traversalCriteria.getTraversalDirection(),
                    stoppingCondition.getNumberHops(), stoppingCondition.hasVerticesCondition() ?
                        stoppingCondition.getVerticesCondition() : null);
            case STOPPING_PROPERTY_FILTER:
                return new TraversalToPropertyFilter<>(traversalCriteria.getTraversalDirection(),
                    filterFor(stoppingCondition.getStoppingPropertyFilter()));
            default:
                throw new IllegalArgumentException("Unknown StoppingConditionTypeCase: " +
                    stoppingCondition.getStoppingConditionTypeCase());
        }
    }

    @Nonnull
    private PropertyFilter<E> numericFilter(@Nonnull final String propertyName,
                                        @Nonnull final Search.PropertyFilter.NumericFilter numericCriteria) {
        switch (propertyName) {
            case SearchableProperties.OID:
                return new PropertyFilter<>(longPredicate(
                    numericCriteria.getValue(),
                    numericCriteria.getComparisonOperator(),
                    TopologyGraphEntity::getOid
                ));
            case SearchableProperties.ENTITY_TYPE:
                return new PropertyFilter<>(intPredicate(
                    (int) numericCriteria.getValue(),
                    numericCriteria.getComparisonOperator(),
                    TopologyGraphEntity::getEntityType
                ));
            default:
                throw new IllegalArgumentException("Unknown numeric property named: " + propertyName);
        }
    }

    @Nonnull
    private PropertyFilter<E> stringFilter(@Nonnull final String propertyName,
                                        @Nonnull final Search.PropertyFilter.StringFilter stringCriteria) {
        switch (propertyName) {
            case SearchableProperties.DISPLAY_NAME:
                if (!StringUtils.isEmpty(stringCriteria.getStringPropertyRegex())) {
                    return new PropertyFilter<>(stringPredicate(
                        stringCriteria.getStringPropertyRegex(),
                        TopologyGraphEntity::getDisplayName,
                        !stringCriteria.getPositiveMatch(),
                        stringCriteria.getCaseSensitive()
                    ));
                } else {
                    return new PropertyFilter<>(stringOptionsPredicate(
                        stringCriteria.getOptionsList(),
                        TopologyGraphEntity::getDisplayName,
                        !stringCriteria.getPositiveMatch(),
                        stringCriteria.getCaseSensitive()
                    ));
                }
            // Support oid either as a string or as a numeric filter.
            case SearchableProperties.OID:
                if (StringUtils.isNumeric(stringCriteria.getStringPropertyRegex())) {
                    // the string regex is an oid and can be represented as a numeric equals filter.
                    return new PropertyFilter<>(longPredicate(
                        Long.valueOf(stringCriteria.getStringPropertyRegex()),
                        stringCriteria.getPositiveMatch() ? ComparisonOperator.EQ : ComparisonOperator.NE,
                        TopologyGraphEntity::getOid
                    ));
                } else if (!StringUtils.isEmpty(stringCriteria.getStringPropertyRegex())) {
                    // the string regex is really a regex instead of a string-encoded long -- create
                    // a string predicate to handle the oid matching.
                    return new PropertyFilter<>(stringPredicate(
                        stringCriteria.getStringPropertyRegex(),
                        entity -> String.valueOf(entity.getOid()),
                        !stringCriteria.getPositiveMatch(),
                        stringCriteria.getCaseSensitive()
                    ));
                } else {
                    // TODO (roman, May 16 2019): Should be able to convert to numeric.
                    // However, we already optimize it in the starting filter so this might be okay.
                    return new PropertyFilter<>(stringOptionsPredicate(
                        stringCriteria.getOptionsList(),
                        entity -> String.valueOf(entity.getOid()),
                        !stringCriteria.getPositiveMatch(),
                        stringCriteria.getCaseSensitive()
                    ));
                }
            case SearchableProperties.ENTITY_STATE: {
                final boolean regex = !StringUtils.isEmpty(stringCriteria.getStringPropertyRegex());
                if (regex || stringCriteria.getOptionsCount() == 1) {
                    // Note - we are ignoring the case-sensitivity parameter. Since the state is an
                    // enum it doesn't really make sense to be case-sensitive.
                    final String targetStateStr = regex ? stringCriteria.getStringPropertyRegex() :
                        stringCriteria.getOptions(0);
                    final UIEntityState targetState = UIEntityState.fromString(targetStateStr);

                    // If the target state resolves to "UNKNOWN" but "UNKNOWN" wasn't what the user
                    // explicitly wanted, we throw an exception to avoid weird behaviour.
                    if (targetState == UIEntityState.UNKNOWN &&
                        !StringUtils.equalsIgnoreCase(UIEntityState.UNKNOWN.apiStr(),
                            stringCriteria.getStringPropertyRegex())) {
                        throw new IllegalArgumentException("Desired state: " +
                            stringCriteria.getStringPropertyRegex() +
                            " doesn't match a known/valid entity state.");
                    }
                    // It's more efficient to compare the numeric value of the enum.
                    return new PropertyFilter<>(intPredicate(targetState.toEntityState().getNumber(),
                        stringCriteria.getPositiveMatch() ? ComparisonOperator.EQ : ComparisonOperator.NE,
                        entity -> entity.getEntityState().getNumber()));
                } else {
                    // TODO (roman, May 16 2019): Should be able to convert to numeric.
                    return new PropertyFilter<>(stringOptionsPredicate(
                        stringCriteria.getOptionsList(),
                        entity -> UIEntityState.fromEntityState(entity.getEntityState()).apiStr(),
                        !stringCriteria.getPositiveMatch(),
                        stringCriteria.getCaseSensitive()
                    ));
                }
            }
            case SearchableProperties.ENVIRONMENT_TYPE: {
                final boolean regex = !StringUtils.isEmpty(stringCriteria.getStringPropertyRegex());
                if (regex || stringCriteria.getOptionsCount() == 1) {
                    final String targetTypeStr = regex ? stringCriteria.getStringPropertyRegex() :
                        stringCriteria.getOptions(0);
                    final UIEnvironmentType targetType = UIEnvironmentType.fromString(targetTypeStr);

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
                    return targetType.toEnvType()
                        .map(envType -> new PropertyFilter<>(intPredicate(envType.getNumber(),
                            stringCriteria.getPositiveMatch() ? ComparisonOperator.EQ : ComparisonOperator.NE,
                            entity -> entity.getEnvironmentType().getNumber())))
                        // If we're not looking for a specific environment type, all entities match.
                        .orElseGet(() -> new PropertyFilter<>((entity) -> true));
                } else {
                    // TODO (roman, May 16 2019): Should be able to convert to numeric.
                    return new PropertyFilter<>(stringOptionsPredicate(
                        stringCriteria.getOptionsList(),
                        entity -> UIEnvironmentType.fromEnvType(entity.getEnvironmentType()).getApiEnumStringValue(),
                        !stringCriteria.getPositiveMatch(),
                        stringCriteria.getCaseSensitive()
                    ));
                }
            }
            case SearchableProperties.ENTITY_TYPE: {
                final boolean regex = !StringUtils.isEmpty(stringCriteria.getStringPropertyRegex());
                if (regex || stringCriteria.getOptionsCount() == 1) {
                    final String targetTypeStr = regex ? stringCriteria.getStringPropertyRegex() :
                        stringCriteria.getOptions(0);
                    final UIEntityType entityType = UIEntityType.fromString(targetTypeStr);

                    // If the target entity type resolves to "UNKNOWN" but "UNKNOWN" wasn't what the
                    // user explicitly wanted, throw an exception to get an early exit.
                    if (entityType == UIEntityType.UNKNOWN &&
                        !StringUtils.equalsIgnoreCase(UIEntityType.UNKNOWN.apiStr(),
                            stringCriteria.getStringPropertyRegex())) {
                        throw new IllegalArgumentException("Desired entity type type: " +
                            stringCriteria.getStringPropertyRegex() +
                            " doesn't match a known/valid entity type.");
                    }

                    // Get the numeric value, and filter based on that.
                    return new PropertyFilter<>(intPredicate(entityType.typeNumber(),
                        stringCriteria.getPositiveMatch() ? ComparisonOperator.EQ : ComparisonOperator.NE,
                        TopologyGraphEntity::getEntityType));
                } else {
                    // TODO (roman, May 16 2019): Should be able to convert to numeric.
                    return new PropertyFilter<>(stringOptionsPredicate(
                        stringCriteria.getOptionsList(),
                        entity -> UIEntityType.fromType(entity.getEntityType()).apiStr(),
                        !stringCriteria.getPositiveMatch(),
                        stringCriteria.getCaseSensitive()
                    ));
                }
            }
            default:
                throw new IllegalArgumentException("Unknown string property: " + propertyName
                        + " with criteria: " + stringCriteria);
        }
    }

    @Nonnull
    private PropertyFilter<E> mapFilter(
            @Nonnull final String propertyName,
            @Nonnull final Search.PropertyFilter.MapFilter mapCriteria) {
        if (!mapCriteria.hasKey()) {
            throw new IllegalArgumentException("Map filter without key value: " + mapCriteria.toString());
        }
        final Predicate<String> valueFilter;
        if (!StringUtils.isEmpty(mapCriteria.getRegex())) {
            final Pattern pattern = Pattern.compile(mapCriteria.getRegex());
            valueFilter = v -> pattern.matcher(v).matches();
        } else if (!mapCriteria.getValuesList().isEmpty()) {
            valueFilter = v -> mapCriteria.getValuesList().contains(v);
        } else {
            valueFilter = v -> true;
        }

        // currently only entity tags is a property of type map
        if (propertyName.equals(SearchableProperties.TAGS_TYPE_PROPERTY_NAME)) {
            return new PropertyFilter<>(te -> te.getTags().entrySet().stream()
                    .anyMatch(e -> e.getKey().equals(mapCriteria.getKey()) &&
                            e.getValue().stream().anyMatch(valueFilter)));
        } else {
            throw new IllegalArgumentException("Unknown map property named: " + propertyName);
        }
    }

    @Nonnull
    private PropertyFilter<E> listFilter(@Nonnull final String propertyName,
            @Nonnull final Search.PropertyFilter.ListFilter listCriteria) {
        // This is incredibly brittle.
        // Should refactor the list filter implementation in the future.
        if (propertyName.equals(SearchableProperties.COMMODITY_SOLD_LIST_PROPERTY_NAME)) {
            if (listCriteria.getListElementTypeCase() == ListElementTypeCase.OBJECT_FILTER) {
                final ObjectFilter objectFilter = listCriteria.getObjectFilter();
                if (objectFilter.getFiltersCount() != 2) {
                    throw new IllegalArgumentException("Expecting 2 filters in ObjectFilter," +
                            " but got " + objectFilter.getFiltersCount() + ": " +
                            objectFilter.getFiltersList());
                }

                final Search.PropertyFilter firstProperty = objectFilter.getFilters(0);
                if (SearchableProperties.COMMODITY_TYPE_PROPERTY_NAME.equals(firstProperty.getPropertyName())) {
                    if (firstProperty.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                        throw new IllegalArgumentException("Unknown property type: " +
                                firstProperty.getPropertyTypeCase());
                    }

                    final Search.PropertyFilter secondProperty = objectFilter.getFilters(1);
                    if (secondProperty.getPropertyTypeCase() != PropertyTypeCase.NUMERIC_FILTER) {
                        throw new IllegalArgumentException("Unknown property type: " +
                                secondProperty.getPropertyTypeCase());
                    }

                    final NumericFilter valueFilter = secondProperty.getNumericFilter();
                    final String commodityType = firstProperty.getStringFilter().getStringPropertyRegex();
                    if (SearchableProperties.COMMODITY_CAPACITY_PROPERTY_NAME.equals(secondProperty.getPropertyName())) {
                        final Pattern commTypeRegex = Pattern.compile(commodityType);
                        if (commTypeRegex.matcher(UICommodityType.VMEM.apiStr()).matches()) {
                            return new PropertyFilter<>(longPredicate(valueFilter.getValue(),
                                valueFilter.getComparisonOperator(),
                                entity -> Optional.ofNullable(entity.soldCommoditiesByType().get(CommodityType.VMEM.getNumber()))
                                    .map(sold -> (long)sold.getCapacity())
                                    .orElse(-1L))
                            );
                        } else if (commTypeRegex.matcher(UICommodityType.MEM.apiStr()).matches()) {
                            return new PropertyFilter<>(longPredicate(valueFilter.getValue(),
                                valueFilter.getComparisonOperator(),
                                entity -> Optional.ofNullable(entity.soldCommoditiesByType().get(CommodityType.MEM.getNumber()))
                                    .map(sold -> (long)sold.getCapacity())
                                    .orElse(-1L)));
                        } else {
                            throw new IllegalArgumentException("Unsupported commodity type for search: " + commodityType);
                        }
                    }
                }
            }
        }

        throw new IllegalArgumentException("Unknown property: " + propertyName + " for ListFilter "
                + listCriteria);
    }

    @Nonnull
    private PropertyFilter<E> objectFilter(@Nonnull final String propertyName,
            @Nonnull final Search.PropertyFilter.ObjectFilter objectCriteria) {
        switch (propertyName) {
            case SearchableProperties.VM_INFO_REPO_DTO_PROPERTY_NAME:
                List<Search.PropertyFilter> filters = objectCriteria.getFiltersList();
                if (filters.size() != 1) {
                    throw new IllegalArgumentException("Expecting one PropertyFilter for " +
                            propertyName + ", but got " + filters.size() + ": " + filters);
                }

                Search.PropertyFilter filter = objectCriteria.getFilters(0);
                switch (filter.getPropertyName()) {
                    case SearchableProperties.VM_INFO_GUEST_OS_TYPE:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        StringFilter stringFilter = filter.getStringFilter();
                        return new PropertyFilter<>(entity -> hasVirtualMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTypeSpecificInfo()
                                                        .getVirtualMachine().getGuestOsInfo().getGuestOsName(),
                                !stringFilter.getPositiveMatch(),
                                stringFilter.getCaseSensitive()).test(entity));
                    case SearchableProperties.VM_INFO_NUM_CPUS:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.NUMERIC_FILTER) {
                            throw new IllegalArgumentException("Expecting NumericFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        return new PropertyFilter<>(entity -> hasVirtualMachineInfoPredicate().test(entity) &&
                                intPredicate((int) filter.getNumericFilter().getValue(),
                                        filter.getNumericFilter().getComparisonOperator(), topologyEntity ->
                                                topologyEntity.getTypeSpecificInfo()
                                                        .getVirtualMachine().getNumCpus()).test(entity));
                    default:
                        throw new IllegalArgumentException("Unknown property: " +
                                filter.getPropertyName() + " on " + propertyName);
                }

            case SearchableProperties.PM_INFO_REPO_DTO_PROPERTY_NAME:
                filters = objectCriteria.getFiltersList();
                if (filters.size() != 1) {
                    throw new IllegalArgumentException("Expecting one PropertyFilter for " +
                            propertyName + ", but got " + filters.size() + ": " + filters);
                }

                filter = objectCriteria.getFilters(0);
                switch (filter.getPropertyName()) {
                    case SearchableProperties.PM_INFO_NUM_CPUS:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.NUMERIC_FILTER) {
                            throw new IllegalArgumentException("Expecting NumericFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        return new PropertyFilter<>(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                intPredicate((int) filter.getNumericFilter().getValue(),
                                        filter.getNumericFilter().getComparisonOperator(), topologyEntity ->
                                                topologyEntity.getTypeSpecificInfo()
                                                        .getPhysicalMachine().getNumCpus()).test(entity));
                    case SearchableProperties.PM_INFO_VENDOR:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        StringFilter stringFilter = filter.getStringFilter();
                        return new PropertyFilter<>(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTypeSpecificInfo()
                                                        .getPhysicalMachine().getVendor(),
                                        !stringFilter.getPositiveMatch(),
                                        stringFilter.getCaseSensitive()).test(entity));

                    case SearchableProperties.PM_INFO_CPU_MODEL:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        stringFilter = filter.getStringFilter();
                        return new PropertyFilter<>(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTypeSpecificInfo()
                                                        .getPhysicalMachine().getCpuModel(),
                                        !stringFilter.getPositiveMatch(),
                                        stringFilter.getCaseSensitive()).test(entity));

                    case SearchableProperties.PM_INFO_MODEL:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        stringFilter = filter.getStringFilter();
                        return new PropertyFilter<>(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTypeSpecificInfo()
                                                        .getPhysicalMachine().getModel(),
                                        !stringFilter.getPositiveMatch(),
                                        stringFilter.getCaseSensitive()).test(entity));

                    case SearchableProperties.PM_INFO_TIMEZONE:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        stringFilter = filter.getStringFilter();
                        return new PropertyFilter<>(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTypeSpecificInfo()
                                                        .getPhysicalMachine().getTimezone(),
                                        !stringFilter.getPositiveMatch(),
                                        stringFilter.getCaseSensitive()).test(entity));

                    default:
                        throw new IllegalArgumentException("Unknown property: " +
                                filter.getPropertyName() + " on " + propertyName);
                }

            default:
                throw new IllegalArgumentException("Unknown object property: " + propertyName
                        + " with criteria: " + objectCriteria);
        }
    }

    /**
     * Compose a int-based predicate for use in a numeric filter based on a given comparison value,
     * operation, and lookup method.
     *
     * @param comparisonValue The value to compare the lookup value against.
     * @param operator The operation to apply in the comparison.
     * @param propertyLookup The function to use to lookup an int-value from a given {@link TopologyGraphEntity}.
     * @return A predicate.
     */
    @Nonnull
    private Predicate<E> intPredicate(final int comparisonValue,
                                                 @Nonnull final ComparisonOperator operator,
                                                 @Nonnull final ToIntFunction<E> propertyLookup) {
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
     * @param propertyLookup The function to use to lookup an int-value from a given {@link TopologyGraphEntity}.
     * @return A predicate.
     */
    @Nonnull
    public Predicate<E> longPredicate(final long comparisonValue,
                                      @Nonnull final ComparisonOperator operator,
                                      @Nonnull final ToLongFunction<E> propertyLookup) {
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
     * @param propertyLookup The function to use to lookup an int-value from a given {@link TopologyGraphEntity}.
     * @param negate If true, return the opposite of the match. That is, if true return false
     *               if the match succeeds. If false, return the same as the match.
     * @param caseSensitive If true, match the case of the regex. If false, do a case-insensitive comparison.
     * @return A predicate.
     */
    @Nonnull
    private Predicate<E> stringPredicate(final String regex,
                                         @Nonnull final Function<E, String> propertyLookup,
                                         final boolean negate,
                                         final boolean caseSensitive) {
        final Pattern pattern = Pattern.compile(regex, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
        return negate ?
            entity -> !pattern.matcher(propertyLookup.apply(entity)).find() :
            entity -> pattern.matcher(propertyLookup.apply(entity)).find();
    }

    @Nonnull
    private Predicate<E> stringOptionsPredicate(final Collection<String> options,
                                                @Nonnull final Function<E, String> propertyLookup,
                                                final boolean negate,
                                                final boolean caseSensitive) {
        final Set<String> expectedSet = options.stream()
            .map(option -> caseSensitive ? option : option.toLowerCase())
            .collect(Collectors.toSet());
        return entity -> {
            final String propValue;
            if (caseSensitive) {
                propValue = propertyLookup.apply(entity);
            } else {
                propValue = propertyLookup.apply(entity).toLowerCase();
            }

            return negate ? !expectedSet.contains(propValue) : expectedSet.contains(propValue);
        };
    }

    private Predicate<E> hasVirtualMachineInfoPredicate() {
        return entity -> entity.getTypeSpecificInfo().hasVirtualMachine();
    }

    private Predicate<E> hasPhysicalMachineInfoPredicate() {
        return entity -> entity.getTypeSpecificInfo().hasPhysicalMachine();
    }
}
