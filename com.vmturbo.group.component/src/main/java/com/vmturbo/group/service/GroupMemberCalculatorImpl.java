package com.vmturbo.group.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc.TargetSearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.group.group.GroupMembersPlain;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * The basic implementation of the {@link GroupMemberCalculator}.
 */
public class GroupMemberCalculatorImpl implements GroupMemberCalculator {
    private static final Logger logger = LogManager.getLogger();

    private final TargetSearchServiceBlockingStub targetSearchService;

    private final SearchServiceBlockingStub searchServiceRpc;

    /**
     * Create a new instance of the calculator.
     *
     * @param targetSearchService Stub to search targets.
     * @param searchServiceRpc Stub to perform searches for dynamic groups.
     */
    public GroupMemberCalculatorImpl(final TargetSearchServiceBlockingStub targetSearchService,
                                     final SearchServiceBlockingStub searchServiceRpc) {
        this.targetSearchService = targetSearchService;
        this.searchServiceRpc = searchServiceRpc;
    }

    @NotNull
    @Override
    public Set<Long> getGroupMembers(@Nonnull final IGroupStore groupStore, @Nonnull final GroupDefinition groupDefinition, final boolean expandNestedGroups) throws StoreOperationException {
        return calculateGroupMembers(groupStore, groupDefinition, expandNestedGroups);
    }

    @NotNull
    @Override
    public Set<Long> getGroupMembers(@Nonnull final IGroupStore groupStore, @Nonnull final Collection<Long> groupIds, final boolean expandNestedGroups) throws StoreOperationException {
        return calculateGroupMembers(groupStore, groupIds, expandNestedGroups);
    }

    @Nonnull
    private Set<Long> calculateGroupMembers(@Nonnull IGroupStore groupStore,
                                            @Nonnull Collection<Long> groupIds, boolean expandNestedGroups)
        throws StoreOperationException {
        final long startTime = System.currentTimeMillis();
        final Set<Long> memberOids = new HashSet<>();
        final GroupMembersPlain members = groupStore.getMembers(groupIds, expandNestedGroups);
        memberOids.addAll(members.getEntityIds());
        if (!expandNestedGroups) {
            memberOids.addAll(members.getGroupIds());
        }
        for (EntityFilters entityFilters : members.getEntityFilters()) {
            memberOids.addAll(getEntities(entityFilters, groupStore));
        }
        logger.debug("Retrieving members for groups {} (recursion: {}) took {}ms", groupIds,
            expandNestedGroups, System.currentTimeMillis() - startTime);
        return memberOids;
    }

    @Nonnull
    private Set<Long> calculateGroupMembers(@Nonnull IGroupStore groupStore,
                                            @Nonnull GroupDefinition groupDefinition, boolean expandNestedGroups)
        throws StoreOperationException {
        final long startTime = System.currentTimeMillis();
        final Set<Long> memberOids = new HashSet<>();

        switch (groupDefinition.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS: {
                final List<StaticMembersByType> staticMembers =
                    groupDefinition.getStaticGroupMembers().getMembersByTypeList();
                final Set<Long> groupIds = new HashSet<>();

                for (final StaticMembersByType member : staticMembers) {
                    switch (member.getType().getTypeCase()) {
                        case ENTITY:
                            memberOids.addAll(member.getMembersList());
                            break;
                        case GROUP:
                            groupIds.addAll(member.getMembersList());
                            break;
                        default:
                            logger.error("Unexpected member type `{}` in group definition `{}`",
                                member.getType().getTypeCase(), groupDefinition);
                    }
                }

                if (expandNestedGroups && !groupIds.isEmpty()) {
                    memberOids.addAll(getGroupMembers(groupStore, groupIds, true));
                } else {
                    memberOids.addAll(groupIds);
                }
                break;
            }
            case ENTITY_FILTERS:
                memberOids.addAll(getEntities(groupDefinition.getEntityFilters(), groupStore));
                break;
            case GROUP_FILTERS:
                final GroupFilters groupFilters = groupDefinition.getGroupFilters();
                final Collection<Long> groupIds = groupStore.getGroupIds(groupFilters);
                if (expandNestedGroups && !groupIds.isEmpty()) {
                    memberOids.addAll(getGroupMembers(groupStore, groupIds, expandNestedGroups));
                } else {
                    memberOids.addAll(groupIds);
                }
                break;
            case SELECTIONCRITERIA_NOT_SET:
                logger.error("Member selection criteria has not been set in group definition `{}`",
                    groupDefinition);
                break;
            default:
                logger.error("Unexpected selection criteria `{}` in group definition `{}`",
                    groupDefinition.getSelectionCriteriaCase(), groupDefinition);
        }
        logger.debug("Retrieving anonymous group members took {}ms",
            System.currentTimeMillis() - startTime);
        return memberOids;
    }

    @Nonnull
    private Set<Long> getEntities(@Nonnull EntityFilters entityFilters, @Nonnull IGroupStore groupStore) {
        final List<EntityFilter> filterList = entityFilters.getEntityFilterList();
        final Set<Long> memberOids = new HashSet<>();
        for (EntityFilter entityFilter : filterList) {
            if (!entityFilter.hasSearchParametersCollection()) {
                logger.error("Search parameter collection is not present in group entity filters `{}`",
                    entityFilters);
            }
            // resolve a dynamic group
            final List<SearchParameters> searchParameters
                = entityFilter.getSearchParametersCollection().getSearchParametersList();

            // Convert any ClusterMemberFilters to static set member checks based
            // on current group membership info
            final List<SearchParameters> finalParams = new ArrayList<>(searchParameters.size());
            final SearchFilterResolver searchFilterResolver =
                new GroupComponentSearchFilterResolver(targetSearchService, groupStore);
            for (SearchParameters params : searchParameters) {
                finalParams.add(searchFilterResolver.resolveExternalFilters(params));
            }
            try {
                final SearchQuery.Builder query = SearchQuery.newBuilder()
                    .addAllSearchParameters(finalParams)
                    .setLogicalOperator(entityFilter.getLogicalOperator());

                if (entityFilter.getEntityType() == CommonDTO.EntityDTO
                    .EntityType.BUSINESS_ACCOUNT.getNumber()) {
                    // Dynamic group of accounts should only include those accounts that have an
                    // associated target
                    query.addSearchParameters(SearchProtoUtil.makeSearchParameters(
                        SearchProtoUtil.entityTypeFilter(ApiEntityType.BUSINESS_ACCOUNT))
                        .addSearchFilter(SearchFilter.newBuilder()
                            .setPropertyFilter(SearchProtoUtil.associatedTargetFilter())
                            .build())
                        .build());
                }

                final Search.SearchEntityOidsResponse searchResponse =
                    searchServiceRpc.searchEntityOids(SearchEntityOidsRequest.newBuilder()
                        .setSearch(query)
                        .build());

                memberOids.addAll(searchResponse.getEntitiesList());
            } catch (StatusRuntimeException e) {
                logger.debug("Error resolving filter {}. Error: {}. Some members may be missing.",
                        entityFilter, e.getMessage());
            }
        }
        return memberOids;
    }
}
