package com.vmturbo.topology.graph.search.filter;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter.ListElementTypeCase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.PropertyTypeCase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.topology.graph.SearchableProps;
import com.vmturbo.topology.graph.SearchableProps.BusinessAccountProps;
import com.vmturbo.topology.graph.SearchableProps.PmProps;
import com.vmturbo.topology.graph.SearchableProps.StorageProps;
import com.vmturbo.topology.graph.SearchableProps.VmProps;
import com.vmturbo.topology.graph.SearchableProps.VolumeProps;
import com.vmturbo.topology.graph.SearchableProps.WorkloadControllerProps;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.graph.TopologyGraphSearchableEntity;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToDepthFilter;
import com.vmturbo.topology.graph.search.filter.TraversalFilter.TraversalToPropertyFilter;

/**
 * A factory for constructing an appropriate filter to perform a search against the topology.
 *
 * @param <E> The type of {@link TopologyGraphEntity} the filters produced by this factory work with.
 */
@ThreadSafe
public class TopologyFilterFactory<E extends TopologyGraphSearchableEntity<E>> {

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
            case SearchableProperties.ENTITY_STATE:
                return new PropertyFilter<>(intPredicate(
                        (int) numericCriteria.getValue(),
                        numericCriteria.getComparisonOperator(),
                        entity -> entity.getEntityState().getNumber()
                ));
            case SearchableProperties.ASSOCIATED_TARGET_ID:
                return PropertyFilter.typeSpecificFilter(
                        BusinessAccountProps::hasAssociatedTargetId, BusinessAccountProps.class);
            default:
                throw new IllegalArgumentException("Unknown numeric property named: " + propertyName);
        }
    }

    @Nonnull
    private PropertyFilter<E> stringFilter(@Nonnull final String propertyName,
                                        @Nonnull final Search.PropertyFilter.StringFilter stringCriteria) {
        Predicate<String> stringPredicate = stringPredicate(stringCriteria);
        switch (propertyName) {
            case SearchableProperties.DISPLAY_NAME:
                return new PropertyFilter<>(e -> stringPredicate.test(e.getDisplayName()));
            // Support oid either as a string or as a numeric filter.
            case SearchableProperties.OID:
                if (StringUtils.isNumeric(stringCriteria.getStringPropertyRegex())) {
                    // the string regex is an oid and can be represented as a numeric equals filter.
                    return new PropertyFilter<>(longPredicate(Long.parseLong(stringCriteria.getStringPropertyRegex()),
                            stringCriteria.getPositiveMatch()
                                ? ComparisonOperator.EQ : ComparisonOperator.NE,
                            TopologyGraphEntity::getOid));
                } else {
                    // TODO (roman, May 16 2019): In the options case, we should be able to
                    // the options to numbers.
                    // However, we already optimize it in the starting filter so this might be okay.
                    return new PropertyFilter<>(e -> stringPredicate.test(String.valueOf(e.getOid())));
                }
            case SearchableProperties.ENTITY_STATE: {
                final boolean regex = !StringUtils.isEmpty(stringCriteria.getStringPropertyRegex());
                if (regex || stringCriteria.getOptionsCount() == 1) {
                    // Note - we are ignoring the case-sensitivity parameter. Since the state is an
                    // enum it doesn't really make sense to be case-sensitive.
                    final String targetStateStr = regex ?
                        SearchProtoUtil.stripFullRegex(stringCriteria.getStringPropertyRegex()) :
                        stringCriteria.getOptions(0);
                    final UIEntityState targetState = UIEntityState.fromString(targetStateStr);

                    // If the target state resolves to "UNKNOWN" but "UNKNOWN" wasn't what the user
                    // explicitly wanted, we throw an exception to avoid weird behaviour.
                    if (targetState == UIEntityState.UNKNOWN && regex &&
                        !StringUtils.equalsIgnoreCase(UIEntityState.UNKNOWN.apiStr(),
                            SearchProtoUtil.stripFullRegex(stringCriteria.getStringPropertyRegex())))
                    {
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
                if (!stringCriteria.getPositiveMatch()) {
                    throw new IllegalArgumentException("Environment type filter with negative match");
                }
                final boolean regex = !StringUtils.isEmpty(stringCriteria.getStringPropertyRegex());
                if (regex || stringCriteria.getOptionsCount() == 1) {
                    final String targetTypeStr = regex ? SearchProtoUtil
                        .stripFullRegex(stringCriteria.getStringPropertyRegex()) :
                        stringCriteria.getOptions(0);
                    final EnvironmentType targetType =
                        EnvironmentTypeUtil.fromApiString(targetTypeStr)
                            .orElseThrow(() ->
                                new IllegalArgumentException("Unknown environment type " + targetTypeStr));
                    final Predicate<EnvironmentType> environmentTypePredicate =
                            EnvironmentTypeUtil.matchingPredicate(targetType);
                    return new PropertyFilter<>(e -> environmentTypePredicate.test(e.getEnvironmentType()));
                } else {
                    throw new IllegalArgumentException("Illegal environment type filter");
                }
            }
            case SearchableProperties.ENTITY_TYPE: {
                final boolean regex = !StringUtils.isEmpty(stringCriteria.getStringPropertyRegex());
                if (regex || stringCriteria.getOptionsCount() == 1) {
                    final String targetTypeStr = regex ?
                        SearchProtoUtil.stripFullRegex(stringCriteria.getStringPropertyRegex()) :
                        stringCriteria.getOptions(0);
                    final ApiEntityType entityType = ApiEntityType.fromString(targetTypeStr);

                    // If the target entity type resolves to "UNKNOWN" but "UNKNOWN" wasn't what the
                    // user explicitly wanted, throw an exception to get an early exit.
                    if (entityType == ApiEntityType.UNKNOWN &&
                        !StringUtils.equalsIgnoreCase(ApiEntityType.UNKNOWN.apiStr(),
                            SearchProtoUtil.stripFullRegex(stringCriteria.getStringPropertyRegex()))) {
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
                        entity -> ApiEntityType.fromType(entity.getEntityType()).apiStr(),
                        !stringCriteria.getPositiveMatch(),
                        stringCriteria.getCaseSensitive()
                    ));
                }
            }
            case SearchableProperties.ENCRYPTED: {
                if (stringCriteria.getOptionsCount() == 1) {
                    final boolean targetBool = Boolean.parseBoolean(stringCriteria.getOptions(0));
                    return PropertyFilter.typeSpecificFilter(v -> v.isEncrypted() == targetBool, VolumeProps.class);
                }
            }
            case SearchableProperties.EPHEMERAL: {
                if (stringCriteria.getOptionsCount() == 1) {
                    final boolean targetEphemeral = Boolean.parseBoolean(stringCriteria.getOptions(0));
                    return PropertyFilter.typeSpecificFilter(v -> v.isEphemeral() == targetEphemeral, VolumeProps.class);
                }
            }
            case SearchableProperties.VM_DESKTOP_POOL_ACTIVE_SESSIONS: {
                if (stringCriteria.getOptionsCount() == 1) {
                    final boolean targetActiveSessions =
                        Boolean.parseBoolean(stringCriteria.getOptions(0));
                    final Predicate<SearchableProps> propsTest =
                        targetActiveSessions
                            ? e -> (long)e.getCommodityUsed(CommodityType.ACTIVE_SESSIONS_VALUE) == 1L
                            : e -> (long)e.getCommodityUsed(CommodityType.ACTIVE_SESSIONS_VALUE) < 1L;
                        return PropertyFilter.typeSpecificFilter(propsTest, SearchableProps.class);
                }
            }
            case SearchableProperties.DELETABLE:
                // to use this filter, presentation layer needs to send true or false for options,
                // we need to only depend on the optionsList. When both true and false are send then
                // we should return all the entities.
                if (stringCriteria.getOptionsCount() == 1) {
                    boolean optionValue =
                            Boolean.parseBoolean(stringCriteria.getOptionsList().get(0)) == stringCriteria.getPositiveMatch();
                    return PropertyFilter.typeSpecificFilter(v -> v.isDeletable() == optionValue, VolumeProps.class);
                } else {
                    return new PropertyFilter<>(entity -> true);
                }
            case SearchableProperties.EXCLUSIVE_DISCOVERING_TARGET: {
                final List<Long> targetIds = stringCriteria.getOptionsList()
                        .stream()
                        .map(Long::valueOf)
                        .collect(Collectors.toList());
                final boolean positive = stringCriteria.getPositiveMatch();
                return new PropertyFilter<>(e ->
                    (e.getDiscoveringTargetIds().collect(Collectors.toList()).equals(targetIds))
                            == positive);
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
        final Predicate<Entry<String, List<String>>> entryFilter;


        // currently only entity tags is a property of type map
        if (propertyName.equals(SearchableProperties.TAGS_TYPE_PROPERTY_NAME)) {
            return new PropertyFilter<>(te ->
                // Check tags for the match, and negate the result if we're not actually looking
                // for a positive match.
                mapCriteria.getPositiveMatch() == te.getSearchableProps(SearchableProps.class)
                        .getTagIndex().isMatchingEntity(te.getOid(), mapCriteria));
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
                    final String commodityType;
                    if (firstProperty.getStringFilter().hasStringPropertyRegex()) {
                        commodityType = firstProperty.getStringFilter().getStringPropertyRegex();
                    } else {
                        commodityType = String.join("|", firstProperty.getStringFilter().getOptionsList());
                    }
                    if (SearchableProperties.COMMODITY_CAPACITY_PROPERTY_NAME.equals(secondProperty.getPropertyName())) {
                        final Pattern commTypeRegex = Pattern.compile(commodityType);
                        if (commTypeRegex.matcher(UICommodityType.VMEM.apiStr()).matches()) {
                            return new PropertyFilter<>(longPredicate(valueFilter.getValue(),
                                valueFilter.getComparisonOperator(),
                                entity -> {
                                    SearchableProps props = entity.getSearchableProps(SearchableProps.class);
                                    if (props == null) {
                                        return -1L;
                                    } else {
                                        return (long)props.getCommodityCapacity(CommodityType.VMEM.getNumber());
                                    }
                                }
                            ));
                        } else if (commTypeRegex.matcher(UICommodityType.MEM.apiStr()).matches()) {
                            return new PropertyFilter<>(longPredicate(valueFilter.getValue(),
                                valueFilter.getComparisonOperator(),
                                    entity -> {
                                        SearchableProps props = entity.getSearchableProps(SearchableProps.class);
                                        if (props == null) {
                                            return -1L;
                                        } else {
                                            return (long)props.getCommodityCapacity(CommodityType.MEM.getNumber());
                                        }
                                    }
                            ));
                        } else {
                            throw new IllegalArgumentException("Unsupported commodity type for search: " + commodityType);
                        }
                    }
                }
            }
        } else if (propertyName.equals(SearchableProperties.DISCOVERED_BY_TARGET)) {
            // if the original request contains a string filter with a single option
            // or it contains a numerical filter,
            // then the newly created property filter will be numerical
            if ((listCriteria.hasNumericFilter()
                    && listCriteria.getNumericFilter().getComparisonOperator() == ComparisonOperator.EQ) ||
                 (listCriteria.hasStringFilter() && listCriteria.getStringFilter().getOptionsCount() == 1)
                    && listCriteria.getStringFilter().getPositiveMatch()) {
                long targetId = listCriteria.hasStringFilter() ?
                        Long.valueOf(listCriteria.getStringFilter().getOptions(0)) :
                        listCriteria.getNumericFilter().getValue();
                return new PropertyFilter<>(entity ->
                             entity.getDiscoveringTargetIds().anyMatch(t -> targetId == t));
            } else if (listCriteria.hasStringFilter()) {
                return new PropertyFilter<>(entity ->
                             entity.getDiscoveringTargetIds()
                                 .map(t -> Long.toString(t))
                                 .anyMatch(
                                     makeCaseSensitivePredicate(listCriteria.getStringFilter())::test));
            }
        } else if (propertyName.equals(SearchableProperties.VM_CONNECTED_NETWORKS)) {
            if (listCriteria.hasStringFilter()) {
                final Predicate<String> str = stringPredicate(listCriteria.getStringFilter());
                return PropertyFilter.typeSpecificFilter(vmProps ->
                        vmProps.getConnectedNetworkNames().stream().anyMatch(str), VmProps.class);
            }
        } else if (propertyName.equals(SearchableProperties.VENDOR_ID)) {
            if (listCriteria.hasStringFilter() && listCriteria.getStringFilter().hasStringPropertyRegex()) {
                final String regex = listCriteria.getStringFilter().getStringPropertyRegex();
                final boolean positiveMatch = listCriteria.getStringFilter().getPositiveMatch();
                final boolean caseSensitive = listCriteria.getStringFilter().getCaseSensitive();
                final Pattern pattern = Pattern.compile(regex, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
                return new PropertyFilter<E>(entity -> entity.getAllVendorIds()
                    .anyMatch(vendorId -> pattern.matcher(vendorId).find()) == positiveMatch);
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
                        final Predicate<String> str = stringPredicate(filter.getStringFilter());
                        return PropertyFilter.typeSpecificFilter(vmProps ->
                                str.test(vmProps.getGuestOsName()), VmProps.class);
                    case SearchableProperties.VM_INFO_NUM_CPUS:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.NUMERIC_FILTER) {
                            throw new IllegalArgumentException("Expecting NumericFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        IntPredicate predicate = intPredicate(filter.getNumericFilter());
                        return PropertyFilter.typeSpecificFilter(vmProps ->
                                predicate.test(vmProps.getNumCpus()), VmProps.class);
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
                    case SearchableProperties.PM_INFO_NUM_CPUS: {
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.NUMERIC_FILTER) {
                            throw new IllegalArgumentException(
                                    "Expecting NumericFilter for " + filter.getPropertyName() + ", but got " + filter);
                        }
                        IntPredicate predicate = intPredicate(filter.getNumericFilter());
                        return PropertyFilter.typeSpecificFilter(pmProps -> predicate.test(pmProps.getNumCpus()), PmProps.class);
                    }
                    case SearchableProperties.PM_INFO_VENDOR: {
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                    filter.getPropertyName() + ", but got " + filter);
                        }
                        Predicate<String> predicate = stringPredicate(filter.getStringFilter());
                        return PropertyFilter.typeSpecificFilter(pmProps ->
                                predicate.test(pmProps.getVendor()), PmProps.class);
                    }
                    case SearchableProperties.PM_INFO_CPU_MODEL: {
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException(
                                    "Expecting StringFilter for " + filter.getPropertyName() + ", but got " + filter);
                        }
                        Predicate<String> predicate = stringPredicate(filter.getStringFilter());
                        return PropertyFilter.typeSpecificFilter(pmProps ->
                                predicate.test(pmProps.getCpuModel()), PmProps.class);
                    }
                    case SearchableProperties.PM_INFO_MODEL: {
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException(
                                    "Expecting StringFilter for " + filter.getPropertyName() + ", but got " + filter);
                        }
                        Predicate<String> predicate = stringPredicate(filter.getStringFilter());
                        return PropertyFilter.typeSpecificFilter(pmProps ->
                                predicate.test(pmProps.getModel()), PmProps.class);
                    }
                    case SearchableProperties.PM_INFO_TIMEZONE: {
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException(
                                    "Expecting StringFilter for " + filter.getPropertyName() + ", but got " + filter);
                        }
                        Predicate<String> predicate = stringPredicate(filter.getStringFilter());
                        return PropertyFilter.typeSpecificFilter(pmProps
                                -> predicate.test(pmProps.getTimezone()), PmProps.class);
                    }
                    default:
                        throw new IllegalArgumentException("Unknown property: " +
                                filter.getPropertyName() + " on " + propertyName);
                }

            case SearchableProperties.DS_INFO_REPO_DTO_PROPERTY_NAME:
                filters = objectCriteria.getFiltersList();
                if (filters.size() != 1) {
                    throw new IllegalArgumentException("Expecting one PropertyFilter for " +
                        propertyName + ", but got " + filters.size() + ": " + filters);
                }

                filter = objectCriteria.getFilters(0);
                switch (filter.getPropertyName()) {
                    case SearchableProperties.DS_LOCAL:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                filter.getPropertyName() + ", but got " + filter);
                        }

                        // TODO (roman, May 22 2020): We should check if it's a boolean encoded as
                        // a string, and optimize that case.
                        Predicate<String> predicate = stringPredicate(filter.getStringFilter());
                        return PropertyFilter.typeSpecificFilter(stProps
                                -> predicate.test(Boolean.toString(stProps.isLocal())), StorageProps.class);
                    default:
                        throw new IllegalArgumentException("Unknown property: " +
                            filter.getPropertyName() + " on " + propertyName);
                }

            case SearchableProperties.BUSINESS_ACCOUNT_INFO_REPO_DTO_PROPERTY_NAME:
                filters = objectCriteria.getFiltersList();
                if (filters.size() != 1) {
                    throw new IllegalArgumentException("Expecting one PropertyFilter for " +
                        propertyName + ", but got " + filters.size() + ": " + filters);
                }

                filter = objectCriteria.getFilters(0);
                switch (filter.getPropertyName()) {
                    case SearchableProperties.BUSINESS_ACCOUNT_INFO_ACCOUNT_ID:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                filter.getPropertyName() + ", but got " + filter);
                        }

                        Predicate<String> predicate = stringPredicate(filter.getStringFilter());
                        return PropertyFilter.typeSpecificFilter(baProps
                                -> predicate.test(baProps.getAccountId()), BusinessAccountProps.class);
                    default:
                        throw new IllegalArgumentException("Unknown property: " +
                            filter.getPropertyName() + " on " + propertyName);
                }
            case SearchableProperties.VOLUME_REPO_DTO:
                filters = objectCriteria.getFiltersList();
                if (filters.size() != 1) {
                    throw new IllegalArgumentException("Expecting one PropertyFilter for " +
                        propertyName + ", but got " + filters.size() + ": " + filters);
                }
                filter = objectCriteria.getFilters(0);
                switch (filter.getPropertyName()) {
                    case SearchableProperties.VOLUME_ATTACHMENT_STATE:
                        if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                            throw new IllegalArgumentException("Expecting StringFilter for " +
                                filter.getPropertyName() + ", but got " + filter);
                        }
                        final StringFilter stringFilter = filter.getStringFilter();
                        final Predicate<String> stringPredicate = stringPredicate(stringFilter);
                        return PropertyFilter.typeSpecificFilter(vol ->
                            stringPredicate.test(vol.attachmentState().name()), VolumeProps.class);
                    default:
                        throw new IllegalArgumentException("Unknown property: " +
                            filter.getPropertyName() + " on " + propertyName);
                }
            case SearchableProperties.WC_INFO_REPO_DTO_PROPERTY_NAME:
                return workloadControllerObjectFilter(propertyName, objectCriteria);

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
        IntPredicate intPredicate = intPredicate(comparisonValue, operator);
        return entity -> intPredicate.test(propertyLookup.applyAsInt(entity));
    }

    private IntPredicate intPredicate(NumericFilter numericFilter) {
        return intPredicate((int)numericFilter.getValue(), numericFilter.getComparisonOperator());
    }

    private IntPredicate intPredicate(final int comparisonValue,
                                      @Nonnull final ComparisonOperator comparisonOperator) {
        return value -> {
            switch (comparisonOperator) {
                case EQ:
                    return value == comparisonValue;
                case NE:
                    return value != comparisonValue;
                case GT:
                    return value > comparisonValue;
                case GTE:
                    return value >= comparisonValue;
                case LT:
                    return value < comparisonValue;
                case LTE:
                    return value <= comparisonValue;
                default:
                    throw new IllegalArgumentException("Unknown operator type: " + comparisonOperator);
            }
        };
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
     * Takes a {@link StringFilter} and returns an case-sensitive string predicate counterpart.
     * Case-sensitivity of the {@link StringFilter} object is ignored.
     *
     * @param stringFilter the {@link StringFilter} object.
     * @return the equivalent string predicate.
     */
    private static Predicate<String> makeCaseSensitivePredicate(@Nonnull StringFilter stringFilter) {
        if (stringFilter.hasStringPropertyRegex() && !stringFilter.getStringPropertyRegex().isEmpty()) {
            final Pattern pattern = Pattern.compile(stringFilter.getStringPropertyRegex());
            return stringFilter.getPositiveMatch() ?
                     string -> pattern.matcher(string).find() :
                     string -> !pattern.matcher(string).find();
        } else if (stringFilter.getOptionsList() != null) {
            return stringFilter.getPositiveMatch() ?
                     string -> stringFilter.getOptionsList().contains(string) :
                     string -> !stringFilter.getOptionsList().contains(string);
        }

        throw new IllegalArgumentException(
                "Cannot transform to predicate the string filter " + stringFilter);
    }

    /**
     * Compose a string-based predicate for use in a string filter that filters based on options.
     *
     * @param options The options for the string.
     * @param propertyLookup The function to use to lookup an string value
     *                       from a given {@link TopologyGraphEntity}.
     * @param negate If true, return the opposite of the match. That is, if true return false
     *               if the match succeeds. If false, return the same as the match.
     * @param caseSensitive If true, match the case of the regex. If false, do a case-insensitive comparison.
     * @return A predicate.
     */
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

    /**
     * Get property filter for WorkloadController based on given property name and filter criteria.
     *
     * @param propertyName   Name of the property that we want to compare
     * @param objectCriteria The filter criteria used to check if an object matches.
     * @return WorkloadController object filter that corresponds to the input criteria.
     */
    private PropertyFilter<E> workloadControllerObjectFilter(@Nonnull final String propertyName,
                                                             @Nonnull final Search.PropertyFilter.ObjectFilter objectCriteria) {
        List<Search.PropertyFilter> filters = objectCriteria.getFiltersList();
        if (filters.size() != 1) {
            throw new IllegalArgumentException("Expecting one PropertyFilter for "
                + propertyName + ", but got " + filters.size() + ": " + filters);
        }
        Search.PropertyFilter filter = objectCriteria.getFilters(0);
        if (filter.getPropertyName().equals(SearchableProperties.CONTROLLER_TYPE)) {
            if (filter.getPropertyTypeCase() != PropertyTypeCase.STRING_FILTER) {
                throw new IllegalArgumentException(
                        "Expecting StringFilter for " + filter.getPropertyName() + ", but got " + filter);
            }

            final StringFilter stringFilter = filter.getStringFilter();
            // If stringFilter options list contains OTHER_CONTROLLER_TYPE, then we are searching
            // for WorkloadController entities with customControllerInfo.
            final boolean matchCustom = stringFilter.getOptionsList().stream()
                    .anyMatch(option -> option.toLowerCase().equals(SearchableProperties.OTHER_CONTROLLER_TYPE.toLowerCase()));
            Predicate<String> predicate = stringPredicate(stringFilter);
            return PropertyFilter.typeSpecificFilter(wcProps -> {
                if (matchCustom) {
                    return wcProps.isCustom();
                } else {
                    return predicate.test(wcProps.getControllerType());
                }
            }, WorkloadControllerProps.class);
        } else {
            throw new IllegalArgumentException("Unknown property: "
                + filter.getPropertyName() + " on " + propertyName);
        }
    }

    private Predicate<String> stringPredicate(StringFilter filter) {
        // Use options.
        final Set<String> expectedSet = filter.getOptionsList().stream()
                .map(option -> filter.getCaseSensitive() ? option : option.toLowerCase())
                .collect(Collectors.toSet());
        // Use regex.
        final Pattern pattern = Pattern.compile(filter.getStringPropertyRegex(),
                filter.getCaseSensitive() ? 0 : Pattern.CASE_INSENSITIVE);
        return str -> {
            if (!StringUtils.isEmpty(filter.getStringPropertyRegex())) {
                return filter.getPositiveMatch() == pattern.matcher(str).find();
            } else {
                return filter.getPositiveMatch() == expectedSet.contains(filter.getCaseSensitive() ? str : str.toLowerCase());
            }
        };
    }
}
