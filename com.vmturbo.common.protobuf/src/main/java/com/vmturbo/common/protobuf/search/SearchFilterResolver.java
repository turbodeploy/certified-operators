package com.vmturbo.common.protobuf.search;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter.Builder;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

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

    /**
     * Returns a set of OIDs of all the members for the groups specified by a group filter.
     *
     * @param groupFilter group filter to apply
     * @return set of member OIDs
     */
    @Nonnull
    protected abstract Set<Long> getGroupMembers(@Nonnull GroupFilter groupFilter);

    /**
     * Returns a set of OIDs of all owners for requested groups.
     *
     * @param groupIds group ids to query
     * @param groupType group type to query
     * @return set of owners OIDs
     */
    @Nonnull
    protected abstract Set<Long> getGroupOwners(@Nonnull Collection<Long> groupIds,
            @Nullable GroupType groupType);

    /**
     * Get all the target Ids that match the property filter.
     *
     * @param filter the target filter.
     * @return the target IDs that match the given filter.
     */
    @Nonnull
    protected abstract Collection<Long> getTargetIdsFromFilter(@Nonnull PropertyFilter filter);

    /**
     * Provided an input SearchParameters object, resolve any group filters contained
     * inside and return a new SearchParameters object with the resolved filters. If there are no
     * group filters inside, return the original object.
     *
     * @param searchParameters A SearchParameters object that may contain cluster filters.
     * @return A SearchParameters object that has had any group filters in it resolved. Will be
     *         the original object if there were no group filters inside.
     */
    @Nonnull
    public SearchParameters resolveExternalFilters(@Nonnull SearchParameters searchParameters) {
        // return the original object if no group member filters inside
        if (searchParameters.getSearchFilterList()
                .stream()
                .noneMatch(filter -> filter.hasGroupFilter() ||
                        isTargetFilter(filter))) {
            return searchParameters;
        }
        // We have one or more Group Member Filters to resolve. Rebuild the SearchParameters.
        final SearchParameters.Builder searchParamBuilder =
                SearchParameters.newBuilder(searchParameters);
        // we will rebuild the search filters, resolving any group member filters we encounter.
        searchParamBuilder.clearSearchFilter();
        for (SearchFilter sf : searchParameters.getSearchFilterList()) {
            searchParamBuilder.addSearchFilter(convertGroupFilter(sf));
        }

        return convertTargetFilters(searchParamBuilder.build());
    }

    /**
     * Convert a group filter to a static entity property filter.
     * If the input filter does not contain a group filter, the input filter will
     * be returned, unchanged.
     *
     * @param inputFilter The group filter to convert.
     * @return A new SearchFilter with any GroupFilter converted to property filters.
     *         If there weren't any GroupFilter to convert, the original filter is
     *         returned.
     */
    @Nonnull
    private SearchFilter convertGroupFilter(@Nonnull SearchFilter inputFilter) {
        if (!inputFilter.hasGroupFilter()) {
            return inputFilter;
        }
        final Builder searchFilterBuilder = SearchFilter.newBuilder();
        final Search.GroupFilter groupFilter = inputFilter.getGroupFilter();
        switch (groupFilter.getEntityToGroupType()) {
            case OWNER_OF:
                convertFilterForGroupOwners(searchFilterBuilder, groupFilter);
                break;
            case MEMBER_OF:
                convertFilterForGroupMembers(searchFilterBuilder, groupFilter);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported relationship between entity and group:" + ' ' +
                                groupFilter.getEntityToGroupType());
        }
        return searchFilterBuilder.build();
    }

    private void convertFilterForGroupOwners(@Nonnull final Builder searchFilterBuilder,
            @Nonnull final Search.GroupFilter groupFilter) {
        final PropertyFilter groupSpecifier = groupFilter.getGroupSpecifier();
        if (groupSpecifier.hasStringFilter()) {
            boolean positiveMatch = groupSpecifier.getStringFilter().getPositiveMatch();
            final List<Long> groupIds = groupSpecifier.getStringFilter()
                    .getOptionsList()
                    .stream()
                    .map(Long::valueOf)
                    .collect(Collectors.toList());
            final com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType groupType =
                    groupFilter.getGroupType();
            final Set<Long> groupOwners = getGroupOwners(groupIds, groupType);
            searchFilterBuilder.setPropertyFilter(
                    SearchProtoUtil.idFilter(groupOwners, positiveMatch));
        }
    }

    private void convertFilterForGroupMembers(@Nonnull final Builder searchFilterBuilder,
            @Nonnull final Search.GroupFilter groupFilter) {
        // this has a group membership filter.
        // We are only supporting static group lookups in this filter. Theoretically we could call
        // back to getMembers() to get generic group resolution, which would be more flexible,
        // but has the huge caveat of allowing circular references to happen. We'll stick to
        // just handling groups here and open it up later, when/if needed.
        final PropertyFilter groupSpecifierFilter = groupFilter.getGroupSpecifier();
        logger.debug("Resolving group filter {}", groupSpecifierFilter);

        final GroupFilter.Builder grFilter =
                GroupFilter.newBuilder().addPropertyFilters(groupSpecifierFilter);
        if (groupFilter.hasGroupType()) {
            grFilter.setGroupType(groupFilter.getGroupType());
        }
        final Set<Long> matchingGroupMembers = getGroupMembers(grFilter.build());
        // build the replacement filter - a set of options, holding the oids that we've
        // fetched from groups.
        searchFilterBuilder.setPropertyFilter(SearchProtoUtil.idFilter(matchingGroupMembers));
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
                final Collection<Long> matchingTargets = getTargetIdsFromFilter(targetFilter
                        .getPropertyFilter());
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

