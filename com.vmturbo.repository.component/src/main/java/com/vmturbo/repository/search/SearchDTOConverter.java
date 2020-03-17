package com.vmturbo.repository.search;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import javaslang.control.Either;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.PropertyTypeCase;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
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
            final Search.TraversalFilter traversalFilter) {
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

            case CONNECTED_TO:
                direction = Filter.TraversalDirection.CONNECTED_TO;
                break;

            case CONNECTED_FROM:
                direction = Filter.TraversalDirection.CONNECTED_FROM;
                break;

            default:
                return Either.left(new IllegalArgumentException("Traversal direction is not set"));
        }

        final StoppingCondition stoppingCondition = traversalFilter.getStoppingCondition();
        switch (stoppingCondition.getStoppingConditionTypeCase()) {
            case NUMBER_HOPS:
                final int hops = stoppingCondition.getNumberHops();
                if (stoppingCondition.hasVerticesCondition()) {
                    return Either.right(Filter.traversalHopNumConnectedVerticesFilter(direction,
                            hops, stoppingCondition.getVerticesCondition()));
                }
                return Either.right(Filter.traversalHopFilter(direction, hops));

            case STOPPING_PROPERTY_FILTER:
                final Search.PropertyFilter stoppingPropertyFilter =
                        stoppingCondition.getStoppingPropertyFilter();
                final Either<Throwable, Filter<PropertyFilterType>> stoppingFilter =
                        convertPropertyFilter(stoppingPropertyFilter);
                if (stoppingCondition.hasVerticesCondition()) {
                    return stoppingFilter.map(sFilter -> Filter.traversalCondNumConnectedVerticesFilter(
                            direction, sFilter, stoppingCondition.getVerticesCondition()));
                }
                return stoppingFilter.map(sFilter -> Filter.traversalCondFilter(direction, sFilter));

            case STOPPINGCONDITIONTYPE_NOT_SET:
            default:
                return Either.left(new IllegalArgumentException("Missing traversal stopping condition"));
        }
    }

    public static Either<Throwable, Filter<PropertyFilterType>> convertPropertyFilter(
            @Nonnull final PropertyFilter propertyFilter) {
        if (propertyFilter.getPropertyTypeCase() == PropertyTypeCase.PROPERTYTYPE_NOT_SET) {
            return Either.left(new IllegalArgumentException("Property type filter is not set"));
        }
        return Either.right(Filter.propertyFilter(propertyFilter));
    }

    @Nonnull
    public static MinimalEntity toSearchEntity(@Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        MinimalEntity.Builder eBldr = MinimalEntity.newBuilder()
            .setDisplayName(serviceEntityRepoDTO.getDisplayName())
            .setEntityType(UIEntityType.fromString(serviceEntityRepoDTO.getEntityType()).typeNumber())
            .setOid(Long.parseLong(serviceEntityRepoDTO.getOid()));
        EnvironmentTypeUtil.fromApiString(serviceEntityRepoDTO.getEnvironmentType())
            .ifPresent(eBldr::setEnvironmentType);
        return eBldr.build();
    }
}
