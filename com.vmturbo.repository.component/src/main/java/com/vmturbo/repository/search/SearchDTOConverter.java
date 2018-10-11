package com.vmturbo.repository.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javaslang.control.Either;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.repository.constant.RepoObjectType;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;

public class SearchDTOConverter {

    /**
     * Convert a {@link SearchParameters} to a list of
     * {@link AQLRepr}.
     *
     * @param searchParameters The SearchParameters to convert.
     * @return A list of {@link AQLRepr}.
     * @throws Throwable Any problem during conversion. For example, missing data in the protobuf.
     */
    @SuppressWarnings("unchecked")
    public static List<AQLRepr> toAqlRepr(final SearchParameters searchParameters) throws Throwable {

        final Either<Throwable, Filter<? extends AnyFilterType>> startingFilter =
                convertPropertyFilter(searchParameters.getStartingFilter()).map(Filter::widen);

        final List<Either<Throwable, Filter<? extends AnyFilterType>>> filters =
                searchParameters.getSearchFilterList().stream()
                        .map(SearchDTOConverter::convertSearchFilter)
                        .collect(Collectors.toList());

        final ArrayList<Either<Throwable, Filter<? extends AnyFilterType>>> allFilters = new ArrayList<>();
        allFilters.add(startingFilter);
        allFilters.addAll(filters);

        final List<AQLRepr> aqlReprs = new ArrayList<>();
        for (final Either<Throwable, Filter<? extends AnyFilterType>> eitherFilter : allFilters) {
            final AQLRepr aqlRepr = eitherFilter.map(AQLRepr::fromFilters).getOrElseThrow(Function.identity());
            aqlReprs.add(aqlRepr);
        }

        return aqlReprs;
    }

    private static Either<Throwable, Filter<? extends AnyFilterType>> convertSearchFilter(final Search.SearchFilter searchFilter) {
        switch (searchFilter.getFilterTypeCase()) {
            case PROPERTY_FILTER:
                final Either<Throwable, Filter<PropertyFilterType>> propertyFilter =
                        convertPropertyFilter(searchFilter.getPropertyFilter());
                return propertyFilter.map(Filter::widen);

            case TRAVERSAL_FILTER:
                final Either<Throwable, Filter<TraversalFilterType>> traversalFilter =
                        convertTraversalFilter(searchFilter.getTraversalFilter());
                return traversalFilter.map(Filter::widen);

            default:
                return Either.left(new IllegalArgumentException("Missing filter type"));
        }
    }

    private static Either<Throwable, Filter<TraversalFilterType>> convertTraversalFilter(
            final Search.SearchFilter.TraversalFilter traversalFilter) {
        final Filter.TraversalDirection direction;

        if (!traversalFilter.hasTraversalDirection()) {
            return Either.left(new IllegalArgumentException("Traversal direction is not set"));
        }

        // Say, our current entity is a VM, and we have a PM -- PROVIDES/PRODUCES --> VM relationship.
        // If the current entity is a VM, VM `consumes` means what entities is the VM consuming.
        // Based on our scenario, the VM is consuming from a PM. So PM is the PROVIDER or VM.
        // Hence, if we need to find what entities is a VM consuming, we need to traverse to the VM's
        // providers.
        switch (traversalFilter.getTraversalDirection()) {
            case CONSUMES:
                direction = Filter.TraversalDirection.PROVIDER;
                break;

            case PRODUCES:
                direction = Filter.TraversalDirection.CONSUMER;
                break;

            default:
                return Either.left(new IllegalArgumentException("Traversal direction is not set"));
        }

        switch (traversalFilter.getStoppingCondition().getStoppingConditionTypeCase()) {
            case NUMBER_HOPS:
                final int hops = traversalFilter.getStoppingCondition().getNumberHops();
                return Either.right(Filter.traversalHopFilter(direction, hops));

            case STOPPING_PROPERTY_FILTER:
                final Search.PropertyFilter stoppingPropertyFilter =
                        traversalFilter.getStoppingCondition().getStoppingPropertyFilter();
                final Either<Throwable, Filter<PropertyFilterType>> stoppingFilter = convertPropertyFilter(stoppingPropertyFilter);
                return stoppingFilter.map(sFilter -> Filter.traversalCondFilter(direction, sFilter));

            case STOPPINGCONDITIONTYPE_NOT_SET:
            default:
                return Either.left(new IllegalArgumentException("Missing traversal stopping condition"));
        }
    }

    private static Either<Throwable, Filter<PropertyFilterType>> convertPropertyFilter(final Search.PropertyFilter propertyFilter) {
        final String propertyName = propertyFilter.getPropertyName();
        final Filter<PropertyFilterType> filter;

        switch (propertyFilter.getPropertyTypeCase()) {
            case NUMERIC_FILTER:
                if (!propertyFilter.getNumericFilter().hasValue()) {
                    return Either.left(new IllegalArgumentException("Numeric filter value is not set"));
                }

                if (!propertyFilter.getNumericFilter().hasComparisonOperator()) {
                    return Either.left(new IllegalArgumentException("Numeric filter comparison operator is not set"));
                }

                final long value = propertyFilter.getNumericFilter().getValue();
                if (propertyName.equals("entityType")) {
                    // translate the entityType number to a string; make sure matches entire value
                    // Match the case as well - entity type is driven by enum.
                    filter = Filter.stringPropertyFilter(propertyName,
                        StringFilter.newBuilder()
                            .setStringPropertyRegex('^' + RepoObjectType.mapEntityType(Math.toIntExact(value)) + '$')
                            .setMatch(true)
                            .setCaseSensitive(true)
                            .build());
                }
                else {
                    final Filter.NumericOperator numOp;
                    switch (propertyFilter.getNumericFilter().getComparisonOperator()) {
                        case EQ:
                            numOp = Filter.NumericOperator.EQ;
                            break;

                        case NE:
                            numOp = Filter.NumericOperator.NEQ;
                            break;

                        case GT:
                            numOp = Filter.NumericOperator.GT;
                            break;

                        case GTE:
                            numOp = Filter.NumericOperator.GTE;
                            break;

                        case LT:
                            numOp = Filter.NumericOperator.LT;
                            break;

                        case LTE:
                            numOp = Filter.NumericOperator.LTE;
                            break;

                        default:
                            return Either.left(new IllegalArgumentException("ComparisonOperator is not set"));
                    }

                    filter = Filter.numericPropertyFilter(propertyName, numOp, value);
                }
                break;

            case STRING_FILTER:
                final StringFilter stringFilter = propertyFilter.getStringFilter();
                if (!stringFilter.hasStringPropertyRegex()) {
                    return Either.left(new IllegalArgumentException("String filter regex is not set"));
                }
                filter = Filter.stringPropertyFilter(propertyName, stringFilter);
                break;

            case MAP_FILTER:
                filter =
                        Filter.mapPropertyFilter(
                                propertyName,
                                propertyFilter.getMapFilter().getKey(),
                                propertyFilter.getMapFilter().getValuesList(),
                                propertyFilter.getMapFilter().getIsMultimap()
                        );
                break;

            case PROPERTYTYPE_NOT_SET:
            default:
                return Either.left(
                    new IllegalArgumentException("Property filter does not contain a String or Numeric filter"));
        }

        return Either.right(filter);
    }

    /**
     * Converts object from {@link ServiceEntityRepoDTO} to {@link Search.Entity}.
     *
     * @param serviceEntityRepoDTO The ServiceEntityRepoDTO to convert.
     * @return The Search.SearchParameters converted.
     */
    public static Search.Entity toSearchEntity(final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        Objects.requireNonNull(serviceEntityRepoDTO, "serviceEntityRepoDTO must not be null");

        final int state = UIEntityState.fromString(serviceEntityRepoDTO.getState()).toEntityState().getNumber();
        final int type = RepoObjectType.toTopologyEntityType(serviceEntityRepoDTO.getEntityType());

        return Search.Entity.newBuilder().setDisplayName(serviceEntityRepoDTO.getDisplayName())
                                         .setOid(Long.parseLong(serviceEntityRepoDTO.getOid()))
                                         .setState(state)
                                         .setType(type)
                                         .build();
    }
}
