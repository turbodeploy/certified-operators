package com.vmturbo.topology.processor.group.filter;

import java.util.List;
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
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter.ListElementTypeCase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.PropertyTypeCase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
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

    // name of the property inside ServiceEntityRepoDTO or nested object
    public static final String ENTITY_TYPE_PROPERTY_NAME = "entityType";
    public static final String TAGS_TYPE_PROPERTY_NAME = "tags";
    private static final String COMMODITY_SOLD_LIST_PROPERTY_NAME = "commoditySoldList";
    private static final String COMMODITY_TYPE_PROPERTY_NAME = "type";
    private static final String COMMODITY_CAPACITY_PROPERTY_NAME = "capacity";
    private static final String VM_INFO_REPO_DTO_PROPERTY_NAME = "virtualMachineInfoRepoDTO";
    private static final String VM_INFO_GUEST_OS_TYPE = "guestOsType";
    private static final String VM_INFO_NUM_CPUS = "numCpus";
    private static final String PM_INFO_REPO_DTO_PROPERTY_NAME = "physicalMachineInfoRepoDTO";
    private static final String PM_INFO_NUM_CPUS = "numCpus";
    private static final String PM_INFO_VENDOR = "vendor";
    private static final String PM_INFO_CPU_MODEL = "cpuModel";
    private static final String PM_INFO_MODEL = "model";
    private static final String PM_INFO_TIMEZONE = "timezone";

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
    private TraversalFilter filterFor(@Nonnull final Search.TraversalFilter traversalCriteria) {
        final StoppingCondition stoppingCondition = traversalCriteria.getStoppingCondition();
        switch (stoppingCondition.getStoppingConditionTypeCase()) {
            case NUMBER_HOPS:
                return new TraversalToDepthFilter(traversalCriteria.getTraversalDirection(),
                    stoppingCondition.getNumberHops(), stoppingCondition.hasVerticesCondition() ?
                        stoppingCondition.getVerticesCondition() : null);
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
                return targetType.toEnvType()
                    .map(envType -> new PropertyFilter(intPredicate(envType.getNumber(),
                        stringCriteria.getMatch() ? ComparisonOperator.EQ : ComparisonOperator.NE,
                        entity -> entity.getEnvironmentType().getNumber())))
                    // If we're not looking for a specific environment type, all entities match.
                    .orElseGet(() -> new PropertyFilter((entity) -> true));
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

    @Nonnull
    private PropertyFilter listFilter(@Nonnull final String propertyName,
            @Nonnull final Search.PropertyFilter.ListFilter listCriteria) {
        if (propertyName.equals(COMMODITY_SOLD_LIST_PROPERTY_NAME)) {
            if (listCriteria.getListElementTypeCase() == ListElementTypeCase.OBJECT_FILTER) {
                ObjectFilter objectFilter = listCriteria.getObjectFilter();
                if (objectFilter.getFiltersCount() != 2) {
                    throw new IllegalArgumentException("Expecting 2 filters in ObjectFilter," +
                            " but got " + objectFilter.getFiltersCount() + ": " +
                            objectFilter.getFiltersList());
                }

                Search.PropertyFilter firstProperty = objectFilter.getFilters(0);
                if (COMMODITY_TYPE_PROPERTY_NAME.equals(firstProperty.getPropertyName())) {
                    if (firstProperty.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                        throw new IllegalArgumentException("Unknown property type: " +
                                firstProperty.getPropertyTypeCase());
                    }

                    Search.PropertyFilter secondProperty = objectFilter.getFilters(1);
                    if (secondProperty.getPropertyTypeCase() != PropertyTypeCase.NUMERIC_FILTER) {
                        throw new IllegalArgumentException("Unknown property type: " +
                                secondProperty.getPropertyTypeCase());
                    }

                    NumericFilter valueFilter = secondProperty.getNumericFilter();
                    String commodityType = firstProperty.getStringFilter().getStringPropertyRegex();
                    if (COMMODITY_CAPACITY_PROPERTY_NAME.equals(secondProperty.getPropertyName())) {
                        if (commodityType.equals("^VMem$")) {
                            return new PropertyFilter(longPredicate(valueFilter.getValue(),
                                    valueFilter.getComparisonOperator(),
                                    entity -> entity.getTopologyEntityDtoBuilder()
                                            .getCommoditySoldListList().stream()
                                            .filter(sold -> sold.getCommodityType().getType() ==
                                                    CommodityType.VMEM.getNumber())
                                            .map(sold -> (long)sold.getCapacity())
                                            .findAny().orElse(-1L))
                            );
                        } else if (commodityType.equals("^Mem$")) {
                            return new PropertyFilter(longPredicate(valueFilter.getValue(),
                                    valueFilter.getComparisonOperator(),
                                    entity -> entity.getTopologyEntityDtoBuilder()
                                            .getCommoditySoldListList().stream()
                                            .filter(sold -> sold.getCommodityType().getType() ==
                                                    CommodityType.MEM.getNumber())
                                            .map(sold -> (long)sold.getCapacity())
                                            .findAny().orElse(-1L))
                            );
                        }
                    }
                }
            }
        }

        throw new IllegalArgumentException("Unknown property: " + propertyName + " for ListFilter "
                + listCriteria);
    }

    @Nonnull
    private PropertyFilter objectFilter(@Nonnull final String propertyName,
            @Nonnull final Search.PropertyFilter.ObjectFilter objectCriteria) {
        switch (propertyName) {
            case VM_INFO_REPO_DTO_PROPERTY_NAME:
                List<Search.PropertyFilter> filters = objectCriteria.getFiltersList();
                if (filters.size() != 1) {
                    throw new IllegalArgumentException("Expecting one PropertyFilter for " +
                            propertyName + ", but got " + filters.size() + ": " + filters);
                }

                Search.PropertyFilter filter = objectCriteria.getFilters(0);
                switch (filter.getPropertyName()) {
                    case VM_INFO_GUEST_OS_TYPE:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        StringFilter stringFilter = filter.getStringFilter();
                        return new PropertyFilter(entity -> hasVirtualMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTopologyEntityDtoBuilder().getTypeSpecificInfo()
                                                        .getVirtualMachine().getGuestOsType().toString(),
                                !stringFilter.getMatch(),
                                stringFilter.getCaseSensitive()).test(entity));
                    case VM_INFO_NUM_CPUS:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.NUMERIC_FILTER) {
                            throw new IllegalArgumentException("Expecting NumericFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        return new PropertyFilter(entity -> hasVirtualMachineInfoPredicate().test(entity) &&
                                intPredicate((int) filter.getNumericFilter().getValue(),
                                        filter.getNumericFilter().getComparisonOperator(), topologyEntity ->
                                                topologyEntity.getTopologyEntityDtoBuilder().getTypeSpecificInfo()
                                                        .getVirtualMachine().getNumCpus()).test(entity));
                    default:
                        throw new IllegalArgumentException("Unknown property: " +
                                filter.getPropertyName() + " on " + propertyName);
                }

            case PM_INFO_REPO_DTO_PROPERTY_NAME:
                filters = objectCriteria.getFiltersList();
                if (filters.size() != 1) {
                    throw new IllegalArgumentException("Expecting one PropertyFilter for " +
                            propertyName + ", but got " + filters.size() + ": " + filters);
                }

                filter = objectCriteria.getFilters(0);
                switch (filter.getPropertyName()) {
                    case PM_INFO_NUM_CPUS:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.NUMERIC_FILTER) {
                            throw new IllegalArgumentException("Expecting NumericFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        return new PropertyFilter(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                intPredicate((int) filter.getNumericFilter().getValue(),
                                        filter.getNumericFilter().getComparisonOperator(), topologyEntity ->
                                                topologyEntity.getTopologyEntityDtoBuilder().getTypeSpecificInfo()
                                                        .getPhysicalMachine().getNumCpus()).test(entity));
                    case PM_INFO_VENDOR:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        StringFilter stringFilter = filter.getStringFilter();
                        return new PropertyFilter(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTopologyEntityDtoBuilder().getTypeSpecificInfo()
                                                        .getPhysicalMachine().getVendor(),
                                        !stringFilter.getMatch(),
                                        stringFilter.getCaseSensitive()).test(entity));

                    case PM_INFO_CPU_MODEL:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        stringFilter = filter.getStringFilter();
                        return new PropertyFilter(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTopologyEntityDtoBuilder().getTypeSpecificInfo()
                                                        .getPhysicalMachine().getCpuModel(),
                                        !stringFilter.getMatch(),
                                        stringFilter.getCaseSensitive()).test(entity));

                    case PM_INFO_MODEL:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        stringFilter = filter.getStringFilter();
                        return new PropertyFilter(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTopologyEntityDtoBuilder().getTypeSpecificInfo()
                                                        .getPhysicalMachine().getModel(),
                                        !stringFilter.getMatch(),
                                        stringFilter.getCaseSensitive()).test(entity));

                    case PM_INFO_TIMEZONE:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        stringFilter = filter.getStringFilter();
                        return new PropertyFilter(entity -> hasPhysicalMachineInfoPredicate().test(entity) &&
                                stringPredicate(stringFilter.getStringPropertyRegex(), topologyEntity ->
                                                topologyEntity.getTopologyEntityDtoBuilder().getTypeSpecificInfo()
                                                        .getPhysicalMachine().getTimezone(),
                                        !stringFilter.getMatch(),
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
    public static Predicate<TopologyEntity> longPredicate(final long comparisonValue,
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

    private Predicate<TopologyEntity> hasVirtualMachineInfoPredicate() {
        return entity -> entity.getTopologyEntityDtoBuilder().hasTypeSpecificInfo() &&
                entity.getTopologyEntityDtoBuilder().getTypeSpecificInfo().hasVirtualMachine();
    }

    private Predicate<TopologyEntity> hasPhysicalMachineInfoPredicate() {
        return entity -> entity.getTopologyEntityDtoBuilder().hasTypeSpecificInfo() &&
                entity.getTopologyEntityDtoBuilder().getTypeSpecificInfo().hasPhysicalMachine();
    }
}
