package com.vmturbo.group.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CountGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupForEntityResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsPoliciesSettingsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.group.TemporaryGroupCache;
import com.vmturbo.group.group.TemporaryGroupCache.InvalidTempGroupException;
import com.vmturbo.group.service.TransactionProvider.Stores;
import com.vmturbo.group.stitching.GroupStitchingContext;
import com.vmturbo.group.stitching.GroupStitchingManager;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Implementation of group component services.
 */
public class GroupRpcService extends GroupServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final TemporaryGroupCache tempGroupCache;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final UserSessionContext userSessionContext;

    private final TransactionProvider transactionProvider;

    private final GroupStitchingManager groupStitchingManager;

    public GroupRpcService(@Nonnull final TemporaryGroupCache tempGroupCache,
                           @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                           @Nonnull final UserSessionContext userSessionContext,
                           @Nonnull final GroupStitchingManager groupStitchingManager,
            @Nonnull TransactionProvider transactionProvider) {
        this.tempGroupCache = Objects.requireNonNull(tempGroupCache);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.groupStitchingManager = Objects.requireNonNull(groupStitchingManager);
        this.transactionProvider = Objects.requireNonNull(transactionProvider);
    }

    @Override
    public void countGroups(GetGroupsRequest request,
            StreamObserver<CountGroupsResponse> responseObserver) {
        if (!request.hasGroupFilter()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("No group filter is present.").asException());
            return;
        }
        executeOperation(responseObserver, (stores) -> {
            final List<Grouping> listOfGroups = getListOfGroups(stores.getGroupStore(), request);
            responseObserver.onNext(CountGroupsResponse.newBuilder().setCount(listOfGroups.size()).build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getGroups(GetGroupsRequest request,
            StreamObserver<Grouping> responseObserver) {
        if (!request.hasGroupFilter()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("No group filter is present.").asException());
            return;
        }
        executeOperation(responseObserver, stores -> {
            final List<Grouping> listOfGroups = getListOfGroups(stores.getGroupStore(), request);
            listOfGroups.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        });
    }

    private List<Grouping> getListOfGroups(@Nonnull IGroupStore groupStore, GetGroupsRequest request) {
        boolean resolveGroupBasedFilters =
            request.getReplaceGroupPropertyWithGroupMembershipFilter();

        final Collection<Grouping> groups = groupStore.getGroups(
                request.hasGroupFilter() ? request.getGroupFilter() :
                        GroupFilter.newBuilder().build());

        final Set<Long> requestedIds = new HashSet<>(request.getGroupFilter().getIdList());
        // if the user is scoped, set up a filter to restrict the results based on their scope.
        // if the request is for "all" groups: we will filter results and only return accessible ones.
        // If the request was for a specific set of groups: we will use a filter that will throw an
        // access exception if any groups are deemed "out of scope".
        Predicate<Grouping> userScopeFilter = userSessionContext.isUserScoped()
                ? requestedIds.isEmpty()
                ? group -> userSessionContext.getUserAccessScope().contains(getGroupMembers(groupStore, group.getDefinition(), true))
                : group -> UserScopeUtils.checkAccess(userSessionContext, getGroupMembers(groupStore, group.getDefinition(), true))
                : group -> true;

        // apply property filters because they are not applied in group store
        return groups.stream()
                .filter(group -> matchFilters(request.getGroupFilter().getPropertyFiltersList(),
                        group))
                .map(group -> resolveGroupBasedFilters ?
                    replaceGroupPropertiesWithGroupMembershipFilter(groupStore, group) : group)
                .filter(userScopeFilter)
                .collect(Collectors.toList());
    }

    @Override
    public void deleteGroup(GroupID gid, StreamObserver<DeleteGroupResponse> responseObserver) {
        executeOperation(responseObserver,
                stores -> deleteGroup(stores.getGroupStore(), gid, responseObserver));
    }

    private void deleteGroup(@Nonnull IGroupStore groupStore, GroupID gid,
            StreamObserver<DeleteGroupResponse> responseObserver) throws StoreOperationException {
        if (!gid.hasId()) {
            final String errMsg = "Invalid GroupID input for delete a group: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.ABORTED.withDescription(errMsg).asRuntimeException());
            return;
        }

        final long groupId = gid.getId();

        checkUserAccessToGrouping(groupStore, groupId);

        logger.info("Deleting a group: {}", groupId);
        final Optional<Grouping> group = tempGroupCache.deleteGrouping(groupId);
        if (group.isPresent()) {
            // If the group was a temporary group, it shouldn't have been in any policies, so
            // we don't need to do any other work.
            responseObserver.onNext(DeleteGroupResponse.newBuilder().setDeleted(true).build());
            responseObserver.onCompleted();
        } else {
            groupStore.deleteGroup(gid.getId());
            responseObserver.onNext(DeleteGroupResponse.newBuilder().setDeleted(true).build());
            responseObserver.onCompleted();
        }
    }

    private void executeOperation(@Nonnull StreamObserver<?> responseObserver,
            @Nonnull StoreOperation storeOperation) {
        try {
            transactionProvider.transaction(stores -> {
                storeOperation.execute(stores);
                return true;
            });
        } catch (StoreOperationException e) {
            logger.error("Failed to perform operation", e);
            responseObserver.onError(
                    e.getStatus().withDescription(e.getLocalizedMessage()).asException());
        }
    }

    @Override
    public void getMembers(final GroupDTO.GetMembersRequest request,
            final StreamObserver<GroupDTO.GetMembersResponse> responseObserver) {
        executeOperation(responseObserver,
                (stores) -> getMembers(stores.getGroupStore(), request, responseObserver));
    }

    private void getMembers(@Nonnull IGroupStore groupStore,
            final GroupDTO.GetMembersRequest request,
            final StreamObserver<GroupDTO.GetMembersResponse> responseObserver)
            throws StoreOperationException {
        if (!request.hasId()) {
            final String errMsg = "Group ID is missing for the getMembers request";
            logger.error(errMsg);
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
            return;
        }

        final long groupId = request.getId();
        Optional<Grouping> optGroupInfo;
        try {
            // Check temp group cache first, because it's faster.
            optGroupInfo = tempGroupCache.getGrouping(groupId);
            if (!optGroupInfo.isPresent()) {
                optGroupInfo = transactionProvider.transaction(
                        stores -> stores.getGroupStore().getGroup(groupId));
            }
        } catch (DataAccessException e) {
            logger.error("Failed to get group: " + groupId, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getLocalizedMessage()).asRuntimeException());
            return;
        }

        if (optGroupInfo.isPresent()) {
            final Grouping group = optGroupInfo.get();
            final GetMembersResponse resp;
            final List<Long> members = getGroupMembers(groupStore, group.getDefinition(),
                    request.getExpandNestedGroups());
            // verify the user has access to all of the group members before returning any of them.
            if (request.getEnforceUserScope() && userSessionContext.isUserScoped()) {
                if (!request.getExpandNestedGroups()) {
                    // Need to use the expanded members for checking access, if we didn't already fetch them
                    UserScopeUtils.checkAccess(userSessionContext,
                                    getGroupMembers(groupStore, group.getDefinition(), true));
                } else {
                    UserScopeUtils.checkAccess(userSessionContext, members);
                }
            }
            // return members
            logger.debug("Returning group ({}) with {} members", groupId, members.size());
            resp = GetMembersResponse.newBuilder()
                    .setMembers(Members.newBuilder()
                            .addAllIds(members))
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } else if (!request.getExpectPresent()) {
            logger.debug("Did not find group with id {} ; this may be expected behavior", groupId);
            responseObserver.onNext(GetMembersResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } else {
            final String errMsg = "Cannot find a group with id " + groupId;
            logger.error(errMsg);
            responseObserver.onError(Status.NOT_FOUND.withDescription(errMsg).asRuntimeException());
        }
    }

    @Override
    public void getGroupForEntity(GetGroupForEntityRequest request,
            StreamObserver<GetGroupForEntityResponse> responseObserver) {
        if (!request.hasEntityId()) {
            final String errMsg = "EntityID is missing for the getGroupForEntityRequest";
            logger.error(errMsg);
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }
        if (userSessionContext.isUserScoped()) {
            UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(),
                    Collections.singletonList(request.getEntityId()));
        }
        executeOperation(responseObserver, (stores) -> {
            final IGroupStore groupStore = stores.getGroupStore();
            final Set<Grouping> staticGroupsForEntity =
                    groupStore.getStaticGroupsForEntity(request.getEntityId());
            //  User have access to group if has access to all group members
            final Predicate<Grouping> userScopeFilter = userSessionContext.isUserScoped() ?
                    group -> UserScopeUtils.checkAccess(userSessionContext,
                            getGroupMembers(groupStore, group.getDefinition(), true)) :
                    group -> true;
            final List<Grouping> filteredGroups = staticGroupsForEntity.stream()
                    .filter(userScopeFilter)
                    .collect(Collectors.toList());

            GetGroupForEntityResponse entityResponse =
                    GetGroupForEntityResponse.newBuilder().addAllGroup(filteredGroups).build();

            responseObserver.onNext(entityResponse);
            responseObserver.onCompleted();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<DiscoveredGroupsPoliciesSettings> storeDiscoveredGroupsPoliciesSettings(
            final StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver) {

        return new DiscoveredGroupsPoliciesSettingsStreamObserver(responseObserver);
    }

    @Override
    public void getTags(GetTagsRequest request, StreamObserver<GetTagsResponse> responseObserver) {
        executeOperation(responseObserver, (stores) -> {
            final Map<String, Set<String>> resultMapBuilder = stores.getGroupStore().getTags();
            final Tags.Builder resultBuilder = Tags.newBuilder();

            resultMapBuilder.entrySet()
                    .forEach(e -> resultBuilder.putTags(e.getKey(),
                            TagValuesDTO.newBuilder().addAllValues(e.getValue()).build()));

            responseObserver.onNext(GetTagsResponse.newBuilder().setTags(resultBuilder.build()).build());
            responseObserver.onCompleted();
        });
    }

    /**
     * Given a group, transform any dynamic filters based on group properties it contains into StringFilters
     * that express the group membership filter statically.
     *
     * @param group the group to resolve group property filters for
     * @param groupStore group store to use
     * @return A new group containing the changes if there were any group with property based filters to
     * transform. If not, the original group is returned.
     */
    private Grouping replaceGroupPropertiesWithGroupMembershipFilter(
            @Nonnull IGroupStore groupStore, @Nonnull Grouping group) {
        final GroupDefinition groupDefinition = group.getDefinition();
        if (!groupDefinition.hasEntityFilters()) {
            return group; // not a dynamic group -- return original group
        }

        Grouping.Builder newGrouping = Grouping.newBuilder(group);
        newGrouping.getDefinitionBuilder()
            .getEntityFiltersBuilder().clearEntityFilter();
        for (EntityFilter entityFilter : groupDefinition.getEntityFilters().getEntityFilterList()) {
            final List<SearchParameters> searchParameters
                = entityFilter.getSearchParametersCollection().getSearchParametersList();
            // check if there are any group property filters in the search params
            if (!searchParameters.stream()
                .anyMatch(params -> params.getSearchFilterList().stream()
                    .anyMatch(SearchFilter::hasClusterMembershipFilter))) {
                newGrouping.getDefinitionBuilder().getEntityFiltersBuilder()
                    .addEntityFilter(entityFilter);
                continue; // no group property filters inside -- return original group
            }

            // we have group property filters to resolve -- rebuild the group with the resolved
            // filters
            logger.debug("Resolving group property filters for {}",
                group.getDefinition().getDisplayName());
            final List<SearchParameters> searchParamsBuilder = new ArrayList<>();
            for (SearchParameters params : searchParameters) {
                searchParamsBuilder.add(resolveClusterFilters(groupStore, params));
            }

            newGrouping.getDefinitionBuilder().getEntityFiltersBuilder()
                .addEntityFilter(EntityFilter.newBuilder(entityFilter)
                    .clearSearchParametersCollection()
                    .setSearchParametersCollection(GroupDTO.SearchParametersCollection.newBuilder()
                        .addAllSearchParameters(searchParamsBuilder)));
        }

        return newGrouping.build();
    }

    /**
     * Provided an input SearchParameters object, resolve any cluster membership filters contained
     * inside and return a new SearchParameters object with the resolved filters. If there are no
     * cluster membership filters inside, return the original object.
     *
     * @param groupStore group store to use
     * @param searchParameters A SearchParameters object that may contain cluster filters.
     * @return A SearchParameters object that has had any cluster filters in it resolved. Will be the
     * original object if there were no group filters inside.
     */
    SearchParameters resolveClusterFilters(@Nonnull IGroupStore groupStore,
            @Nonnull SearchParameters searchParameters) {
        // return the original object if no cluster member filters inside
        if (!searchParameters.getSearchFilterList().stream()
                .anyMatch(SearchFilter::hasClusterMembershipFilter)) {
            return searchParameters;
        }
        // We have one or more Cluster Member Filters to resolve. Rebuild the SearchParameters.
        SearchParameters.Builder searchParamBuilder = SearchParameters.newBuilder(searchParameters);
        // we will rebuild the search filters, resolving any cluster member filters we encounter.
        searchParamBuilder.clearSearchFilter();
        for (SearchFilter sf : searchParameters.getSearchFilterList()) {
            searchParamBuilder.addSearchFilter(convertClusterMemberFilter(groupStore, sf));
        }

        return searchParamBuilder.build();
    }

    /**
     * Convert a cluster member filter to a static property filter. If the input filter does not
     * contain a cluster member filter, the input filter will be returned, unchanged.
     *
     * @param inputFilter The ClusterMemberFilter to convert.
     * @param groupStore group store to use
     * @return A new SearchFilter with any ClusterMemberFilters converted to property filters. If
     * there weren't any ClusterMemberFilters to convert, the original filter is returned.
     */
    private SearchFilter convertClusterMemberFilter(@Nonnull IGroupStore groupStore,
            @Nonnull SearchFilter inputFilter) {
        if (!inputFilter.hasClusterMembershipFilter()) {
            return inputFilter;
        }
        // this has a cluster membership filter -- resolve plz
        // We are only supporting cluster lookups in this filter. Theoretically we could call
        // back to getMembers() to get generic group resolution, which would be more flexible,
        // but has the huge caveat of allowing circular references to happen. We'll stick to
        // just handling clusters here and open it up later, when/if needed.
        final PropertyFilter clusterSpecifierFilter =
                inputFilter.getClusterMembershipFilter().getClusterSpecifier();
        logger.debug("Resolving ClusterMemberFilter {}", clusterSpecifierFilter);
        final Set<Long> matchingClusterMembers =
                groupStore.getGroups(GroupFilter.newBuilder().build()).stream()
                .filter(group -> matchFilter(clusterSpecifierFilter, group))
                .filter(group -> GroupProtoUtil.CLUSTER_GROUP_TYPES
                                .contains(group.getDefinition().getType())) // only clusters plz
                .map(Grouping::getDefinition)
                .flatMap(clusterInfo -> GroupProtoUtil.getAllStaticMembers(clusterInfo).stream())
                .collect(Collectors.toSet());
        // build the replacement filter - a regex against /^oid1$|^oid2$|.../
        StringJoiner sj = new StringJoiner("$|^", "^", "$");
        matchingClusterMembers.forEach(oid -> sj.add(oid.toString()));

        SearchFilter searchFilter = SearchFilter.newBuilder()
                .setPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName("oid")
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex(sj.toString())))
                .build();
        return searchFilter;
    }

    private boolean matchFilters(@Nonnull List<PropertyFilter> filters, @Nonnull Grouping group) {
        return filters.stream().allMatch(filter -> matchFilter(filter, group));
    }

    private boolean matchFilter(@Nonnull PropertyFilter filter, @Nonnull Grouping group) {
        if (filter.getPropertyName().equals(SearchableProperties.DISPLAY_NAME) && filter.hasStringFilter()) {
            // filter according to property name
            return GroupProtoUtil.nameFilterMatches(
                    group.getDefinition().getDisplayName(), filter.getStringFilter());
        } else if (filter.getPropertyName().equals(StringConstants.TAGS_ATTR) && filter.hasMapFilter()) {
            // filter according to tags
            // get tags from group object
            final Tags tags;
            if (group.hasDefinition() && group.getDefinition().hasTags()) {
                tags = group.getDefinition().getTags();
            } else {
                return false;
            }

            // get map filter and validate
            final MapFilter mapFilter = filter.getMapFilter();
            if (!mapFilter.hasKey()) {
                throw new IllegalArgumentException("Tags filter without a key: " + mapFilter);
            }

            // get corresponding tag values (empty list if the tag key is not present)
            final List<String> tagValues =
                    Optional.ofNullable(tags.getTagsMap().get(mapFilter.getKey()))
                            .map(x -> (List<String>)x.getValuesList())
                            .orElse(Collections.emptyList());

            if (mapFilter.hasRegex()) {
                // regular expression mapping: there should be one value in tagValues
                // that matches the pattern mapFilter.getRegex()
                final Pattern pattern = Pattern.compile(mapFilter.getRegex());
                return
                        tagValues.stream().anyMatch(v -> pattern.matcher(v).matches()) ==
                                mapFilter.getPositiveMatch();
            } else {
                // exact matching: the lists mapFilter.getValuesList() and tagValues
                // must have a non-empty intersection
                return
                        Collections.disjoint(mapFilter.getValuesList(), tagValues) !=
                                mapFilter.getPositiveMatch();
            }
        }

        throw new IllegalArgumentException("Invalid filter for groups: " + filter);
    }

    @Override
    public void createGroup(@Nonnull CreateGroupRequest request,
            @Nonnull StreamObserver<CreateGroupResponse> responseObserver) {
        executeOperation(responseObserver,
                stores -> createGroup(stores.getGroupStore(), request, responseObserver));
    }

    private void createGroup(@Nonnull IGroupStore groupStore, @Nonnull CreateGroupRequest request,
            @Nonnull StreamObserver<CreateGroupResponse> responseObserver)
            throws StoreOperationException {
        try {
            validateCreateGroupRequest(request);
        } catch (InvalidGroupDefinitionException e) {
            logger.error("Group {} is not valid.", request.getGroupDefinition(), e);
            responseObserver.onError(
                            Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        }

        logger.info("Creating a group: {}", request.getGroupDefinition());

        final GroupDefinition groupDef = request.getGroupDefinition();

        Grouping createdGroup = null;

        final Set<MemberType> expectedTypes = findGroupExpectedTypes(groupStore, groupDef);

        if (groupDef.getIsTemporary()) {
            if (groupDef.hasOptimizationMetadata()
                            && !groupDef.getOptimizationMetadata().getIsGlobalScope()) {
                UserScopeUtils.checkAccess(userSessionContext,
                        getGroupMembers(groupStore, groupDef, true));
            }

            try {
                createdGroup = tempGroupCache.create(groupDef, request.getOrigin(), expectedTypes);
            } catch (InvalidTempGroupException e) {
                final String errorMsg = String.format("Failed to create group: %s as it is invalid. exception: %s.",
                                groupDef, e.getLocalizedMessage());
                logger.error(errorMsg, e);
                responseObserver.onError(Status.ABORTED.withDescription(errorMsg)
                        .asRuntimeException());
                return;
            }
        } else {
            final boolean supportsMemberReverseLookup =
                            determineMemberReverseLookupSupported(groupDef);

            if (userSessionContext.isUserScoped()) {
                // verify that the members of the new group would all be in scope
                UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(),
                                getGroupMembers(groupStore, groupDef, true));
            }

                long groupOid = groupStore
                    .createGroup(request.getOrigin(), groupDef, expectedTypes,
                                    supportsMemberReverseLookup);
                createdGroup = Grouping
                                .newBuilder()
                                .setId(groupOid)
                                .setDefinition(groupDef)
                                .addAllExpectedTypes(expectedTypes)
                                .setSupportsMemberReverseLookup(supportsMemberReverseLookup)
                                .build();

        }

        responseObserver.onNext(CreateGroupResponse.newBuilder()
                            .setGroup(createdGroup)
                            .build());
        responseObserver.onCompleted();

    }

    private void validateCreateGroupRequest(CreateGroupRequest request)
                    throws InvalidGroupDefinitionException {
        if (!request.hasGroupDefinition()) {
            throw new InvalidGroupDefinitionException("No group definition is present.");
        }

        if (!request.hasOrigin()) {
            throw new InvalidGroupDefinitionException("No origin definition is present.");
        }

        validateGroupDefinition(request.getGroupDefinition());

    }

    @Override
    public void updateGroup(@Nonnull UpdateGroupRequest request,
            @Nonnull StreamObserver<UpdateGroupResponse> responseObserver) {
        executeOperation(responseObserver,
                stores -> updateGroup(stores.getGroupStore(), request, responseObserver));
    }

    private void updateGroup(@Nonnull IGroupStore groupStore, @Nonnull UpdateGroupRequest request,
            @Nonnull StreamObserver<UpdateGroupResponse> responseObserver)
            throws StoreOperationException {
        if (!request.hasId()) {
            final String errMsg = "Invalid GroupID input for group update: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription(errMsg)
                            .asException());
            return;
        }

        if (!request.hasNewDefinition()) {
            final String errMsg =
                "Invalid new group definition for group update: No group definition is provided";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription(errMsg)
                            .asException());
            return;
        }

        final GroupDefinition groupDefinition = request.getNewDefinition();

        try {
            validateGroupDefinition(groupDefinition);
        } catch (InvalidGroupDefinitionException e) {
            logger.error("Group {} is not valid.", groupDefinition, e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
            return;
        }

        logger.info("Updating a group: {}", request);

        if (userSessionContext.isUserScoped()) {
            // verify the user has access to the group they are trying to modify
            checkUserAccessToGrouping(groupStore, request.getId());
            // verify the modified version would fit in scope too
            UserScopeUtils.checkAccess(userSessionContext,
                            getGroupMembers(groupStore, groupDefinition, true));
        }

        final boolean supportsMemberReverseLookup =
                        determineMemberReverseLookupSupported(groupDefinition);

        final Set<MemberType> expectedTypes = findGroupExpectedTypes(groupStore, groupDefinition);
        final Grouping newGroup =
                groupStore.updateGroup(request.getId(), groupDefinition, expectedTypes,
                        supportsMemberReverseLookup);
        final UpdateGroupResponse res =
                UpdateGroupResponse.newBuilder().setUpdatedGroup(newGroup).build();
        responseObserver.onNext(res);
        responseObserver.onCompleted();
    }

    @Override
    public void getGroup(@Nonnull GroupID request,
            @Nonnull StreamObserver<GetGroupResponse> responseObserver) {
        executeOperation(responseObserver,
                stores -> getGroup(stores.getGroupStore(), request, responseObserver));
    }

    private void getGroup(@Nonnull IGroupStore groupStore, @Nonnull GroupID request,
                    @Nonnull StreamObserver<GetGroupResponse> responseObserver) {
        if (!request.hasId()) {
            final String errMsg = "Invalid GroupID input for get a group: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }

        logger.debug("Attempting to retrieve group: {}", request);

        try {
            Optional<Grouping> group = getGroupById(groupStore, request.getId());
            // Patrick - removing this check, as it's preventing retrieval of data for plans. We will
            // re-enable this with OM-44360
            /*
            if (userSessionContext.isUserScoped() && group.isPresent()) {
                // verify that the members of the new group would all be in scope
                UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(), getGroupMembers(group.get()));
            }
            */
            GetGroupResponse.Builder builder = GetGroupResponse.newBuilder();
            group.ifPresent(builder::setGroup);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            final String errorMsg = String.format("Failed to retrieve group: %s due to data access error: %s",
                            request.getId(), e.getLocalizedMessage());
            logger.error(errorMsg, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(errorMsg).asRuntimeException());
        }
    }

    @Nonnull
    private Optional<Grouping> getGroupById(@Nonnull IGroupStore groupStore, long groupId) {
        // Check the temporary groups cache first
        Optional<Grouping> group = tempGroupCache.getGrouping(groupId);
        if (!group.isPresent()) {
            group = groupStore.getGroup(groupId);
        }
        return group;
    }

    @Nonnull
    @VisibleForTesting
    Set<MemberType> findGroupExpectedTypes(@Nonnull IGroupStore groupStore,
            @Nonnull GroupDefinition groupDefinition) {
        final Set<MemberType> memberTypes = new HashSet<>();

        switch (groupDefinition.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                final List<StaticMembersByType> staticMembers = groupDefinition.getStaticGroupMembers()
                                .getMembersByTypeList();
                final Set<Long> groupIds = new HashSet<>();
                final Set<GroupType> groupTypes = new HashSet<>();
                final Set<Integer> entityTypes = new HashSet<>();

                for (final StaticMembersByType member : staticMembers) {
                    switch (member.getType().getTypeCase()) {
                        case ENTITY:
                            entityTypes.add(member.getType().getEntity());
                            break;
                        case GROUP:
                            groupTypes.add(member.getType().getGroup());
                            groupIds.addAll(member.getMembersList());
                            break;
                        default:
                            logger.error("Unexpected member type `{}` in group definition `{}`",
                                            member.getType().getTypeCase(), groupDefinition);
                    }
                }

                if (!groupIds.isEmpty()) {
                    // We need to look up the expected types from GroupStore
                    final Collection<Grouping> nestedGroups = groupStore.getGroups(
                            GroupFilter.newBuilder().addAllId(groupIds).build());
                    for (final Grouping nestedGroup : nestedGroups) {
                        for (MemberType memberType : nestedGroup.getExpectedTypesList()) {
                            switch (memberType.getTypeCase()) {
                                case ENTITY:
                                    entityTypes.add(memberType.getEntity());
                                    break;
                                case GROUP:
                                    groupTypes.add(memberType.getGroup());
                                    break;
                                default:
                                    logger.error("Unexpected member type `{}` in group definition `{}`",
                                                    memberType.getTypeCase(), groupDefinition);
                            }
                        }
                    }
                }

                entityTypes
                        .stream()
                        .map(entityType -> MemberType.newBuilder().setEntity(entityType).build())
                        .forEach(memberTypes::add);

                groupTypes
                    .stream()
                    .map(groupType -> MemberType.newBuilder().setGroup(groupType).build())
                    .forEach(memberTypes::add);
                break;
            case ENTITY_FILTERS:
                final List<EntityFilter> filterList = groupDefinition
                                .getEntityFilters().getEntityFilterList();

                 filterList
                        .stream()
                        .map(EntityFilter::getEntityType)
                        .distinct()
                        .map(entityType -> MemberType
                                            .newBuilder().setEntity(entityType).build())
                        .forEach(memberTypes::add);
                 break;
            case GROUP_FILTERS:
                // If the group type is dynamic group of groups we currently cannot determine the type
                // expected in the group so we return empty list
                break;
            case SELECTIONCRITERIA_NOT_SET:
                logger.error("Member selection criteria has not been set in group definition `{}`",
                                groupDefinition);
                break;
            default:
                logger.error("Unexpected selection criteria `{}` in group definition `{}`",
                                groupDefinition.getSelectionCriteriaCase(), groupDefinition);
        }

        return memberTypes;
    }

    private boolean determineMemberReverseLookupSupported(@Nonnull GroupDefinition groupDefinition) {
        // We currently only support reverse lookup for on-level (i.e., not a group of group)
        // static groups.
        return (groupDefinition.getSelectionCriteriaCase()
                        == SelectionCriteriaCase.STATIC_GROUP_MEMBERS)
                    && groupDefinition.getStaticGroupMembers()
                        .getMembersByTypeList()
                        .stream()
                        .map(StaticMembersByType::getType)
                        .map(MemberType::getTypeCase)
                        .allMatch(type -> type == MemberType.TypeCase.ENTITY);
    }

    @Nonnull
    private List<Long> getGroupMembers(@Nonnull IGroupStore groupStore, @Nonnull GroupDefinition groupDefinition, boolean expandNestedGroups) {
        final Set<Long> memberOids = new HashSet<>();

        switch (groupDefinition.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                final List<StaticMembersByType> staticMembers = groupDefinition.getStaticGroupMembers()
                    .getMembersByTypeList();
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

                if (expandNestedGroups) {
                    if (!groupIds.isEmpty()) {
                        // We need to expand the nested groups
                        final Collection<Grouping> nestedGroups = groupStore.getGroups(
                                GroupFilter.newBuilder().addAllId(groupIds).build());

                        for (final Grouping nestedGroup : nestedGroups) {
                            memberOids.addAll(
                                    getGroupMembers(groupStore, nestedGroup.getDefinition(), true));
                        }
                    }
                } else {
                    memberOids.addAll(groupIds);
                }

                break;
            case ENTITY_FILTERS:
                final List<EntityFilter> filterList = groupDefinition
                                .getEntityFilters().getEntityFilterList();
                for (EntityFilter entityFilter : filterList) {
                    if (!entityFilter.hasSearchParametersCollection()) {
                        logger.error("Search parameter collection is not present in group definition `{}`",
                                        groupDefinition);
                    }
                    // resolve a dynamic group
                    final List<SearchParameters> searchParameters
                            = entityFilter.getSearchParametersCollection().getSearchParametersList();

                    // Convert any ClusterMemberFilters to static set member checks based
                    // on current group membership info
                    Search.SearchEntityOidsRequest.Builder searchRequestBuilder =
                            Search.SearchEntityOidsRequest.newBuilder();
                    for (SearchParameters params : searchParameters) {
                        searchRequestBuilder.addSearchParameters(
                                resolveClusterFilters(groupStore, params));
                    }
                    final Search.SearchEntityOidsRequest searchRequest = searchRequestBuilder.build();
                    final Search.SearchEntityOidsResponse searchResponse = searchServiceRpc.searchEntityOids(searchRequest);
                    memberOids.addAll(searchResponse.getEntitiesList());
                }
                break;
           case GROUP_FILTERS:
               final List<GroupFilter> groupFilterList = groupDefinition
                       .getGroupFilters().getGroupFilterList();
                for (GroupFilter groupFilter : groupFilterList) {
                    // We need to look up the expected types from GroupStore
                    final Collection<Grouping> groups = groupStore.getGroups(groupFilter);
                    if (expandNestedGroups) {
                        groups
                            .stream()
                            .map(Grouping::getDefinition)
                            .map(group -> getGroupMembers(groupStore, group, true))
                            .forEach(memberOids::addAll);
                    } else {
                        groups
                            .stream()
                            .map(Grouping::getId)
                            .forEach(memberOids::add);
                    }

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

        return new ArrayList<>(memberOids);
    }

    /**
     * Check if the user has access to the group identified by Id. A user has access to the group by
     * default, but if the user is a "scoped" user, they will only have access to the group if all
     * members of the group are in the user's scope.
     * This method will trigger a {@link UserAccessScopeException} if the group exists and the user
     * does not have access to it, otherwise it will return quietly.
     *
     * @param groupStore group store to use
     * @param groupId the group id to check
     *
     */
    private void checkUserAccessToGrouping(@Nonnull IGroupStore groupStore, long groupId) {
        if (!userHasAccessToGrouping(groupStore, groupId)) {
            throw new UserAccessScopeException("User does not have access to group " + groupId);
        }
    }

    /**
     * Check if the user has access to the group identified by Id. A user has access to the group by
     * default, but if the user is a "scoped" user, they will only have access to the group if all
     * members of the group are in the user's scope, or if the group itself is explicitly in the user's
     * scope groups. (in the case of a group that no longer exists)
     *
     * @param groupId the group id to check access for
     * @param groupStore group store to use
     * @return true, if the user definitely has access to the group. false, if not.
     */
    public boolean userHasAccessToGrouping(@Nonnull IGroupStore groupStore, long groupId) {
        if (!userSessionContext.isUserScoped()) {
            return true;
        }
        // if the user scope groups contains the group id directly, we don't even need to expand the
        // group.
        final EntityAccessScope entityAccessScope = userSessionContext.getUserAccessScope();
        if (entityAccessScope.getScopeGroupIds().contains(groupId)) {
            return true;
        }
        Optional<Grouping> optionalGroup = getGroupById(groupStore, groupId);
        if (optionalGroup.isPresent()) {
            // check membership
            return entityAccessScope.contains(
                    getGroupMembers(groupStore, optionalGroup.get().getDefinition(), true));
        } else {
            // the group does not exist any more - we'll return false to be safe, although it is
            // possible that the user had access when the group did exist.
            return false;
        }
    }

    @VisibleForTesting
    void validateGroupDefinition(@Nonnull GroupDefinition groupDefinition)
                    throws InvalidGroupDefinitionException {

        if (!groupDefinition.hasDisplayName()
                        || StringUtils.isEmpty(groupDefinition.getDisplayName())) {
            throw new InvalidGroupDefinitionException("Group display name is blank or not set.");
        }

        switch (groupDefinition.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                if (groupDefinition.getStaticGroupMembers().getMembersByTypeList().isEmpty()) {
                    throw new InvalidGroupDefinitionException(
                                    "No static member list has been set.");
                }
                break;
            case ENTITY_FILTERS:
                final List<EntityFilter> filterList =
                                groupDefinition.getEntityFilters().getEntityFilterList();
                if (filterList.isEmpty()) {
                    throw new InvalidGroupDefinitionException(
                                    "No filter has been set for dynamic entity group.");
                }
                for (EntityFilter entityFilter : filterList) {
                    if (!entityFilter.hasSearchParametersCollection()
                                    || entityFilter.getSearchParametersCollection()
                                                    .getSearchParametersList().isEmpty()) {
                        throw new InvalidGroupDefinitionException(
                                        "Dynamic entities group filter has a filter with no criteria.");
                    }
                }
                break;
            case GROUP_FILTERS:
                if (groupDefinition.getGroupFilters().getGroupFilterList().isEmpty()) {
                    throw new InvalidGroupDefinitionException(
                                    "No filter has been set for dynamic group of group.");
                }
                break;
            case SELECTIONCRITERIA_NOT_SET:
                throw new InvalidGroupDefinitionException("Selection criteria is not set.");
            default:
                throw new InvalidGroupDefinitionException(
                                "Unsupported selection criteria has been set.");
        }
    }

    /**
     * An exception thrown when the {@link GroupDefinition} describing a group is illegal.
     */
    @VisibleForTesting
    static class InvalidGroupDefinitionException extends Exception {

        InvalidGroupDefinitionException(String message) {
            super(message);
        }

    }

    /**
     * A stream observer which stores all groups/policies uploaded from topology processor,
     * performs stitching operations and save to db.
     */
    private class DiscoveredGroupsPoliciesSettingsStreamObserver implements
            StreamObserver<DiscoveredGroupsPoliciesSettings> {

        private final StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver;

        private final Map<Long, List<DiscoveredPolicyInfo>> policiesByTarget = new HashMap<>();
        private final Map<Long, List<DiscoveredSettingPolicyInfo>> settingPoliciesByTarget = new HashMap<>();
        private final GroupStitchingContext groupStitchingContext = new GroupStitchingContext();

        /**
         * Constructor for {@link DiscoveredGroupsPoliciesSettingsStreamObserver}.
         *
         * @param responseObserver the observer which notifies client of any result
         */
        DiscoveredGroupsPoliciesSettingsStreamObserver(
                StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(final DiscoveredGroupsPoliciesSettings record) {
            if (!record.hasTargetId()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Request must have a target ID.").asException());
                return;
            }
            final long targetId = record.getTargetId();
            groupStitchingContext.setTargetGroups(targetId, record.getProbeType(),
                    record.getUploadedGroupsList());
            policiesByTarget.put(targetId, record.getDiscoveredPolicyInfosList());
            settingPoliciesByTarget.put(targetId, record.getDiscoveredSettingPoliciesList());
        }

        @Override
        public void onError(final Throwable t) {
            logger.error("Error uploading discovered non-entities", t);
        }

        @Override
        public void onCompleted() {
            executeOperation(responseObserver, this::onCompleted);
        }

        private void onCompleted(@Nonnull Stores stores) throws StoreOperationException {
            // stitch all groups, e.g. merge same groups from different targets into one
            final GroupStitchingContext stitchedContext =
                    groupStitchingManager.stitch(groupStitchingContext);

            List<DiscoveredGroup> groups =
                    stitchedContext.getAllStitchingGroups().stream().map(stitchingGroup -> {
                        final GroupDefinition groupDefinition =
                                stitchingGroup.buildGroupDefinition();
                        return new DiscoveredGroup(groupDefinition, stitchingGroup.getSourceId(),
                                stitchingGroup.getAllTargetIds(),
                                findGroupExpectedTypes(stores.getGroupStore(), groupDefinition),
                                determineMemberReverseLookupSupported(groupDefinition));
                    }).collect(Collectors.toList());
            final Map<String, Long> allGroupsMap =
                    stores.getGroupStore().updateDiscoveredGroups(groups);
            for (Entry<Long, List<DiscoveredPolicyInfo>> entry : policiesByTarget.entrySet()) {
                stores.getPlacementPolicyStore()
                        .updateTargetPolicies(entry.getKey(), entry.getValue(), allGroupsMap);
            }
            for (Entry<Long, List<DiscoveredSettingPolicyInfo>> entry : settingPoliciesByTarget.entrySet()) {
                stores.getSettingPolicyStore()
                        .updateTargetSettingPolicies(entry.getKey(), entry.getValue(),
                                allGroupsMap);
            }
            responseObserver.onNext(StoreDiscoveredGroupsPoliciesSettingsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    /**
     * Operation with stores to be executed in a transaction.
     */
    @FunctionalInterface
    private interface StoreOperation {
        /**
         * Executes an operation.
         *
         * @param stores stores that are available within a transaction
         * @throws StoreOperationException exception to be thrown if something failed.
         */
        void execute(@Nonnull Stores stores) throws StoreOperationException;
    }
}
