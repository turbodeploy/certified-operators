package com.vmturbo.common.protobuf.search;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.GroupMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc.TargetSearchServiceBlockingStub;

/**
 * This is an object able to resolve filters. There are some filters that could not be executed
 * by repository component (search grpc service) because repository does not know anything about
 * groups.
 *
 * <p></p>This object is to convert filters that repository is not aware of to filters that it can
 * consume substituting complicated filters with an oid filter, where oids are resolved using
 * another components.
 */
public abstract class SearchFilterResolver {

    private final Logger logger = LogManager.getLogger(getClass());
    private final TargetSearchServiceBlockingStub targetSearchService;

    /**
     * Constructs search filter resolver.
     *
     * @param targetSearchService gRPC to resolve target searches
     */
    protected SearchFilterResolver(@Nonnull TargetSearchServiceBlockingStub targetSearchService) {
        this.targetSearchService = Objects.requireNonNull(targetSearchService);
    }

    /**
     * Returns a set of OIDs of all the members for the groups specified by a group filter.
     *
     * @param groupFilter group filter to apply
     * @return set of member OIDs
     */
    @Nonnull
    protected abstract Set<Long> getGroupMembers(@Nonnull GroupFilter groupFilter);

    /**
     * Provided an input SearchParameters object, resolve any group membership filters contained
     * inside and return a new SearchParameters object with the resolved filters. If there are no
     * group membership filters inside, return the original object.
     *
     * @param searchParameters A SearchParameters object that may contain cluster filters.
     * @return A SearchParameters object that has had any cluster filters in it resolved. Will be
     *         the original object if there were no group filters inside.
     */
    @Nonnull
    public SearchParameters resolveExternalFilters(@Nonnull SearchParameters searchParameters) {
        // return the original object if no group member filters inside
        if (searchParameters.getSearchFilterList()
                .stream()
                .noneMatch(filter -> filter.hasGroupMembershipFilter() ||
                        isTargetFilter(filter))) {
            return searchParameters;
        }
        // We have one or more Group Member Filters to resolve. Rebuild the SearchParameters.
        final SearchParameters.Builder searchParamBuilder =
                SearchParameters.newBuilder(searchParameters);
        // we will rebuild the search filters, resolving any group member filters we encounter.
        searchParamBuilder.clearSearchFilter();
        for (SearchFilter sf : searchParameters.getSearchFilterList()) {
            searchParamBuilder.addSearchFilter(convertGroupMemberFilter(sf));
        }

        return convertTargetFilters(searchParamBuilder.build());
    }

    /**
     * Convert a group member filter to a static entity property filter.
     * If the input filter does not contain a group member filter, the input filter will
     * be returned, unchanged.
     *
     * @param inputFilter The group membership filter to convert to convert.
     * @return A new SearchFilter with any ClusterMembershipFilter converted to property filters.
     *         If there weren't any ClusterMembershipFilter to convert, the original filter is
     *         returned.
     */
    @Nonnull
    private SearchFilter convertGroupMemberFilter(@Nonnull SearchFilter inputFilter) {
        if (!inputFilter.hasGroupMembershipFilter()) {
            return inputFilter;
        }
        // this has a group membership filter.
        // We are only supporting static group lookups in this filter. Theoretically we could call
        // back to getMembers() to get generic group resolution, which would be more flexible,
        // but has the huge caveat of allowing circular references to happen. We'll stick to
        // just handling groups here and open it up later, when/if needed.
        final GroupMembershipFilter groupSrcFilter = inputFilter.getGroupMembershipFilter();
        final PropertyFilter groupSpecifierFilter = groupSrcFilter.getGroupSpecifier();
        logger.debug("Resolving group filter {}", groupSpecifierFilter);

        final GroupFilter.Builder groupFilter =
                GroupFilter.newBuilder().addPropertyFilters(groupSpecifierFilter);
        if (groupSrcFilter.hasGroupType()) {
            groupFilter.setGroupType(groupSrcFilter.getGroupType());
        }
        final Set<Long> matchingGroupMembers = getGroupMembers(groupFilter.build());
        // build the replacement filter - a set of options, holding the oids that we've
        // fetched from groups.
        final SearchFilter searchFilter = SearchFilter.newBuilder()
                .setPropertyFilter(SearchProtoUtil.idFilter(matchingGroupMembers))
                .build();
        return searchFilter;
    }

    /**
     * For any Cloud Provider filter within the given search parameters, convert it to a
     * Discovered By Target filter. Fetch the ids of all targets belonging to the cloud provider(s)
     * indicated in the original filter, and add them to the converted filter as options.
     *
     * @param searchParams original search parameters, possibly containing cloud provider
     *         filter
     * @return search parameters with any cloud provider filters converted to target filters
     */
    @Nonnull
    private SearchParameters convertTargetFilters(@Nonnull SearchParameters searchParams) {
        if (searchParams.getSearchFilterList().stream().noneMatch(this::isTargetFilter)) {
            return searchParams;
        }
        final SearchParameters.Builder paramBuilder = SearchParameters.newBuilder(searchParams);
        paramBuilder.clearSearchFilter();
        final Iterator<SearchFilter> iterator = searchParams.getSearchFilterList().iterator();
        while (iterator.hasNext()) {
            final SearchFilter filter = iterator.next();
            if (isTargetFilter(filter)) {
                final SearchFilter targetFilter = iterator.next();
                if (!targetFilter.hasPropertyFilter()) {
                    throw new IllegalArgumentException(
                            "Target search property must have a property filter: " + targetFilter);
                }
                final Collection<Long> matchingTargets =
                        targetSearchService.searchTargets(targetFilter.getPropertyFilter())
                                .getTargetsList();
                paramBuilder.addSearchFilter(SearchFilter.newBuilder()
                        .setPropertyFilter(SearchProtoUtil.discoveredBy(matchingTargets)));
            } else {
                paramBuilder.addSearchFilter(filter);
            }
        }
        return paramBuilder.build();
    }

    private boolean isTargetFilter(@Nonnull final SearchFilter searchFilter) {
        return searchFilter.hasPropertyFilter() && searchFilter.getPropertyFilter()
                .getPropertyName()
                .equals(SearchableProperties.TARGET_FILTER_MARKER);
    }
}

