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
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;
import org.springframework.util.StopWatch;

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
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.GroupFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.Groupings;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsPoliciesSettingsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc.TargetSearchServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.group.GroupMembersPlain;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.group.TemporaryGroupCache;
import com.vmturbo.group.group.TemporaryGroupCache.InvalidTempGroupException;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.service.TransactionProvider.Stores;
import com.vmturbo.group.stitching.GroupStitchingContext;
import com.vmturbo.group.stitching.GroupStitchingManager;
import com.vmturbo.group.stitching.StitchingGroup;
import com.vmturbo.group.stitching.StitchingResult;
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
    private final IdentityProvider identityProvider;

    private final TargetSearchServiceBlockingStub targetSearchService;

    /**
     * Constructs group gRPC service.
     * @param tempGroupCache temporary groups cache to store temp groups
     * @param searchServiceRpc search gRPC service client to resolve dynamic groups
     * @param userSessionContext user session context
     * @param groupStitchingManager groups stitching manager
     * @param transactionProvider transaction provider
     * @param identityProvider identity provider to assign OIDs to user groups
     * @param targetSearchService target search service for dynamic groups
     */
    public GroupRpcService(@Nonnull final TemporaryGroupCache tempGroupCache,
                           @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                           @Nonnull final UserSessionContext userSessionContext,
                           @Nonnull final GroupStitchingManager groupStitchingManager,
            @Nonnull TransactionProvider transactionProvider,
            @Nonnull IdentityProvider identityProvider,
            @Nonnull TargetSearchServiceBlockingStub targetSearchService) {
        this.tempGroupCache = Objects.requireNonNull(tempGroupCache);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.groupStitchingManager = Objects.requireNonNull(groupStitchingManager);
        this.transactionProvider = Objects.requireNonNull(transactionProvider);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.targetSearchService = Objects.requireNonNull(targetSearchService);
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
            final Collection<Long> listOfGroups = getGroupIds(stores.getGroupStore(), request);
            responseObserver.onNext(
                    CountGroupsResponse.newBuilder().setCount(listOfGroups.size()).build());
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
            final Collection<Grouping> listOfGroups = getListOfGroups(stores.getGroupStore(), request);
            listOfGroups.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        });
    }

    private Collection<Long> getGroupIds(@Nonnull IGroupStore groupStore,
            @Nonnull GetGroupsRequest request) {
        final GroupFilters.Builder filter = GroupFilters.newBuilder();
        if (request.hasGroupFilter()) {
            filter.addGroupFilter(request.getGroupFilter());
        }
        return groupStore.getGroupIds(filter.build());
    }

    @Nonnull
    private Collection<Grouping> getListOfGroups(@Nonnull IGroupStore groupStore,
            GetGroupsRequest request) throws StoreOperationException {
        final StopWatch stopWatch = new StopWatch("GetListOfGroups");
        stopWatch.start("replace group filter");
        final boolean resolveGroupBasedFilters =
            request.getReplaceGroupPropertyWithGroupMembershipFilter();
        stopWatch.stop();
        stopWatch.start("get group ids");
        final Collection<Long> groupIds = getGroupIds(groupStore, request);
        stopWatch.stop();
        stopWatch.start("apply user scope");
        final Set<Long> requestedIds = new HashSet<>(request.getGroupFilter().getIdList());
        final List<Long> filteredIds = new ArrayList<>(groupIds.size());
        for (long groupId: groupIds) {
            if (userScopeFilter(groupId, requestedIds, request.getScopesList(), groupStore)) {
                filteredIds.add(groupId);
            }
        }
        stopWatch.stop();
        stopWatch.start("get groups by ids");
        final Collection<Grouping> filteredGroups = groupStore.getGroupsById(filteredIds);
        stopWatch.stop();
        stopWatch.start("replace group properties filter");
        final Collection<Grouping> groupsResult;
        if (resolveGroupBasedFilters) {
            groupsResult = filteredGroups.stream()
                    .map(group -> replaceGroupPropertiesWithGroupMembershipFilter(groupStore,
                            group))
                    .collect(Collectors.toSet());
        } else {
            groupsResult = filteredGroups;
        }
        stopWatch.stop();
        logger.debug(stopWatch::prettyPrint);
        return groupsResult;
    }

    private boolean userScopeFilter(long groupId, @Nonnull Set<Long> requestedIds,
            @Nonnull List<Long> scopes, @Nonnull IGroupStore groupStore) throws StoreOperationException {
        // if the user is scoped, set up a filter to restrict the results based on their scope.
        // if the request contains scopes limit, set up a filter to restrict the results based on it.
        // if the request is for "all" groups: we will filter results and only return accessible ones.
        // If the request was for a specific set of groups: we will use a filter that will throw an
        // access exception if any groups are deemed "out of scope".
        if (!userSessionContext.isUserScoped() && scopes.isEmpty()) {
            return true;
        }

        final Collection<Long> members = getGroupMembers(groupStore, Collections.singleton(groupId), true);
        boolean result = true;
        // filter by user scopes
        if (userSessionContext.isUserScoped()) {
            if (requestedIds.isEmpty()) {
                result = userSessionContext.getUserAccessScope().contains(members);
            } else {
                // trigger an access denied exception if an requested id is inaccessible
                // if user is not scoped, just not return it, no need to throw exception
                result = UserScopeUtils.checkAccess(userSessionContext, members);
            }
        }
        // filter by limited scopes in request to ensure all results are within those scopes
        if (!scopes.isEmpty()) {
            result = result && userSessionContext.getAccessScope(scopes).contains(members);
        }

        return result;
    }

    @Override
    public void deleteGroup(GroupID gid, StreamObserver<DeleteGroupResponse> responseObserver) {
        executeOperation(responseObserver,
                stores -> deleteGroup(stores, gid, responseObserver));
    }

    private void deleteGroup(@Nonnull Stores stores, GroupID gid,
            StreamObserver<DeleteGroupResponse> responseObserver) throws StoreOperationException {
        if (!gid.hasId()) {
            final String errMsg = "Invalid GroupID input for delete a group: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.ABORTED.withDescription(errMsg).asRuntimeException());
            return;
        }

        final long groupId = gid.getId();

        checkUserAccessToGrouping(stores.getGroupStore(), groupId);

        logger.info("Deleting a group: {}", groupId);
        final Optional<Grouping> group = tempGroupCache.deleteGrouping(groupId);
        if (group.isPresent()) {
            // If the group was a temporary group, it shouldn't have been in any policies, so
            // we don't need to do any other work.
            responseObserver.onNext(DeleteGroupResponse.newBuilder().setDeleted(true).build());
            responseObserver.onCompleted();
        } else {
            stores.getSettingPolicyStore().onGroupDeleted(gid.getId());
            stores.getPlacementPolicyStore()
                    .deletePoliciesForGroupBeingRemoved(Collections.singleton(groupId));
            stores.getGroupStore().deleteGroup(gid.getId());
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
                optGroupInfo = getGroup(groupStore, groupId);
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
    public void getGroupsForEntities(GetGroupsForEntitiesRequest request,
            StreamObserver<GetGroupsForEntitiesResponse> responseObserver) {
        if (request.getEntityIdCount() == 0) {
            responseObserver.onNext(GetGroupsForEntitiesResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }
        if (userSessionContext.isUserScoped()) {
            UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(),
                    request.getEntityIdList());
        }
        executeOperation(responseObserver,
                (stores) -> getGroupForEntity(stores.getGroupStore(), request, responseObserver));
    }

    private void getGroupForEntity(@Nonnull IGroupStore groupStore,
            @Nonnull GetGroupsForEntitiesRequest request,
            @Nonnull StreamObserver<GetGroupsForEntitiesResponse> responseObserver)
            throws StoreOperationException {
        final Map<Long, Set<Long>> staticGroupsPerEntity =
                groupStore.getStaticGroupsForEntities(request.getEntityIdList(),
                        request.getGroupTypeList());
        final Map<Long, Set<Long>> filteredGroups;
        if (!userSessionContext.isUserScoped()) {
            filteredGroups = staticGroupsPerEntity;
        } else {
            filteredGroups = new HashMap<>(staticGroupsPerEntity.size());
            for (Entry<Long, Set<Long>> entityGroups : staticGroupsPerEntity.entrySet()) {
                final Set<Long> filtered = new HashSet<>(entityGroups.getValue().size());
                for (Long staticGroup : entityGroups.getValue()) {
                    //  User have access to group if has access to all group members
                    if (userHasAccessToGrouping(groupStore, staticGroup)) {
                        filtered.add(staticGroup);
                    }
                }
                filteredGroups.put(entityGroups.getKey(), filtered);
            }
        }
        final GetGroupsForEntitiesResponse.Builder response =
                GetGroupsForEntitiesResponse.newBuilder();
        for (Entry<Long, Set<Long>> entityGroups : filteredGroups.entrySet()) {
            final Groupings groupings =
                    Groupings.newBuilder().addAllGroupId(entityGroups.getValue()).build();
            response.putEntityGroup(entityGroups.getKey(), groupings);
        }
        if (request.getLoadGroupObjects()) {
            final Set<Long> groupIds = filteredGroups.values()
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            final Collection<Grouping> groups = groupStore.getGroupsById(groupIds);
            response.addAllGroups(groups);
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
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
            final Map<Long, Map<String, Set<String>>> tagsToGroups =
                    stores.getGroupStore().getTags(request.getGroupIdList());
            final Map<Long, Tags> tagsMap = tagsToGroups.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Entry::getKey, el -> Tags.newBuilder()
                            .putAllTags(el.getValue()
                                    .entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(Entry::getKey,
                                            tag -> TagValuesDTO.newBuilder()
                                                    .addAllValues(tag.getValue())
                                                    .build())))
                            .build()));
            responseObserver.onNext(
                    GetTagsResponse.newBuilder().putAllTags(tagsMap).build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getOwnersOfGroups(GetOwnersRequest request,
            StreamObserver<GetOwnersResponse> responseObserver) {
        executeOperation(responseObserver, stores -> {
            final List<Long> groupIdList = request.getGroupIdList();
            if (groupIdList != null) {
                final Set<Long> ownersForGroups = stores.getGroupStore()
                        .getOwnersOfGroups(request.getGroupIdList(), request.getGroupType());
                responseObserver.onNext(
                        GetOwnersResponse.newBuilder().addAllOwnerId(ownersForGroups).build());
            }

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
        final GroupComponentSearchFilterResolver filterResolver =
                new GroupComponentSearchFilterResolver(targetSearchService, groupStore);
        Grouping.Builder newGrouping = Grouping.newBuilder(group);
        newGrouping.getDefinitionBuilder()
            .getEntityFiltersBuilder().clearEntityFilter();
        for (EntityFilter entityFilter : groupDefinition.getEntityFilters().getEntityFilterList()) {
            final List<SearchParameters> searchParameters
                = entityFilter.getSearchParametersCollection().getSearchParametersList();
            // check if there are any group property filters in the search params
            if (!searchParameters.stream()
                .anyMatch(params -> params.getSearchFilterList().stream()
                    .anyMatch(SearchFilter::hasGroupFilter))) {
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
                searchParamsBuilder.add(filterResolver.resolveExternalFilters(params));
            }

            newGrouping.getDefinitionBuilder().getEntityFiltersBuilder()
                .addEntityFilter(EntityFilter.newBuilder(entityFilter)
                    .clearSearchParametersCollection()
                    .setSearchParametersCollection(GroupDTO.SearchParametersCollection.newBuilder()
                        .addAllSearchParameters(searchParamsBuilder)));
        }

        return newGrouping.build();
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

        logger.info("Creating group {}", request.getGroupDefinition().getDisplayName());

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

            long groupOid = identityProvider.next();
            groupStore.createGroup(groupOid, request.getOrigin(), groupDef, expectedTypes,
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
    private Optional<Grouping> getGroup(@Nonnull IGroupStore groupStore, long groupId) {
        final Collection<Grouping> groups = groupStore.getGroupsById(Collections.singleton(groupId));
        if (groups.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(groups.iterator().next());
        }
    }

    @Nonnull
    private Optional<Grouping> getGroupById(@Nonnull IGroupStore groupStore, long groupId) {
        // Check the temporary groups cache first
        Optional<Grouping> group = tempGroupCache.getGrouping(groupId);
        if (!group.isPresent()) {
            return getGroup(groupStore, groupId);
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
                groupDefinition
                        .getEntityFilters()
                        .getEntityFilterList()
                        .stream()
                        .map(GroupProtoUtil::getEntityTypesFromEntityFilter)
                        .flatMap(Collection::stream)
                        .map(entityType -> MemberType
                                .newBuilder().setEntity(entityType.typeNumber()).build())
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
            Search.SearchEntityOidsRequest.Builder searchRequestBuilder =
                    Search.SearchEntityOidsRequest.newBuilder();
            final SearchFilterResolver searchFilterResolver =
                    new GroupComponentSearchFilterResolver(targetSearchService, groupStore);
            for (SearchParameters params : searchParameters) {
                searchRequestBuilder.addSearchParameters(
                        searchFilterResolver.resolveExternalFilters(params));
            }
            final Search.SearchEntityOidsRequest searchRequest = searchRequestBuilder.build();
            final Search.SearchEntityOidsResponse searchResponse =
                    searchServiceRpc.searchEntityOids(searchRequest);
            memberOids.addAll(searchResponse.getEntitiesList());
        }
        return memberOids;
    }

    /**
     * Method returns a set of entity members for a specified groups collection.
     *
     * @param groupStore group store to use
     * @param groupId ids of groups to get members for
     * @param expandNestedGroups whether to expand nested groups
     * @return members of the specified groups.
     * @throws StoreOperationException if exception occurred operating with a group store
     */
    @Nonnull
    private Set<Long> getGroupMembers(@Nonnull IGroupStore groupStore,
            @Nonnull Collection<Long> groupId, boolean expandNestedGroups)
            throws StoreOperationException {
        final Set<Long> memberOids = new HashSet<>();
        final GroupMembersPlain members = groupStore.getMembers(groupId, expandNestedGroups);
        memberOids.addAll(members.getEntityIds());
        if (!expandNestedGroups) {
            memberOids.addAll(members.getGroupIds());
        }
        for (EntityFilters entityFilters : members.getEntityFilters()) {
            memberOids.addAll(getEntities(entityFilters, groupStore));
        }
        return memberOids;
    }

    @Nonnull
    private List<Long> getGroupMembers(@Nonnull IGroupStore groupStore,
            @Nonnull GroupDefinition groupDefinition, boolean expandNestedGroups)
            throws StoreOperationException {
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

                if (expandNestedGroups) {
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
               if (expandNestedGroups) {
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
     * @throws StoreOperationException if error occurred while processing group data
     */
    private void checkUserAccessToGrouping(@Nonnull IGroupStore groupStore, long groupId)
            throws StoreOperationException {
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
     * @throws StoreOperationException if some error occurred while operating with stores
     */
    public boolean userHasAccessToGrouping(@Nonnull IGroupStore groupStore, long groupId)
            throws StoreOperationException {
        if (!userSessionContext.isUserScoped()) {
            return true;
        }
        // if the user scope groups contains the group id directly, we don't even need to expand the
        // group.
        final EntityAccessScope entityAccessScope = userSessionContext.getUserAccessScope();
        if (entityAccessScope.getScopeGroupIds().contains(groupId)) {
            return true;
        }
        // check membership
        return entityAccessScope.contains(
                getGroupMembers(groupStore, Collections.singleton(groupId), true));
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

    @Nonnull
    private DiscoveredGroup createDiscoveredGroup(@Nonnull IGroupStore groupStore,
            @Nonnull StitchingGroup src) {
        final GroupDefinition groupDefinition = src.buildGroupDefinition();
        final Set<MemberType> expectedMembers = findGroupExpectedTypes(groupStore, groupDefinition);
        return new DiscoveredGroup(src.getOid(), groupDefinition, src.getSourceId(),
                src.getTargetIds(), expectedMembers,
                determineMemberReverseLookupSupported(groupDefinition));
    }

    @Nonnull
    private Table<Long, String, Long> createGroupIdTable(@Nonnull StitchingResult stitchingResult) {
        final ImmutableTable.Builder<Long, String, Long> builder = ImmutableTable.builder();
        for (StitchingGroup stitchingGroup : stitchingResult.getGroupsToAddOrUpdate()) {
            for (long targetId : stitchingGroup.getTargetIds()) {
                builder.put(targetId, GroupProtoUtil.createIdentifyingKey(
                        stitchingGroup.getGroupDefinition().getType(),
                        stitchingGroup.getSourceId()), stitchingGroup.getOid());
            }
        }
        return builder.build();
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
            if (record.getDataAvailable()) {
                groupStitchingContext.setTargetGroups(targetId, record.getProbeType(),
                        record.getUploadedGroupsList());
                policiesByTarget.put(targetId, record.getDiscoveredPolicyInfosList());
                settingPoliciesByTarget.put(targetId, record.getDiscoveredSettingPoliciesList());
            } else {
                groupStitchingContext.addUndiscoveredTarget(targetId);
            }
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
            final StitchingResult stitchingResult =
                    groupStitchingManager.stitch(stores.getGroupStore(), groupStitchingContext);
            final List<DiscoveredGroup> groupsToAdd = stitchingResult.getGroupsToAddOrUpdate()
                    .stream()
                    .filter(StitchingGroup::isNewGroup)
                    .map(stitchingGroup -> createDiscoveredGroup(stores.getGroupStore(),
                            stitchingGroup))
                    .collect(Collectors.toList());
            final List<DiscoveredGroup> groupsToUpdate = stitchingResult.getGroupsToAddOrUpdate()
                    .stream()
                    .filter(group -> !group.isNewGroup())
                    .map(stitchingGroup -> createDiscoveredGroup(stores.getGroupStore(),
                            stitchingGroup))
                    .collect(Collectors.toList());
            logger.info("Got {} new groups, {} for update and {} to delete", groupsToAdd.size(),
                    groupsToUpdate.size(), stitchingResult.getGroupsToDelete().size());
            // First, we need to remove setting policies and placement policies for the groups
            // that are removed. After the groups are removed themselves, a link between groups
            // and policies will be lost
            stores.getPlacementPolicyStore()
                    .deletePoliciesForGroupBeingRemoved(stitchingResult.getGroupsToDelete());
            for (Long removedGroup : stitchingResult.getGroupsToDelete()) {
                stores.getSettingPolicyStore().onGroupDeleted(removedGroup);
            }
            stores.getGroupStore()
                    .updateDiscoveredGroups(groupsToAdd, groupsToUpdate,
                            stitchingResult.getGroupsToDelete());
            final Table<Long, String, Long> allGroupsMap = createGroupIdTable(stitchingResult);
            for (Entry<Long, List<DiscoveredPolicyInfo>> entry : policiesByTarget.entrySet()) {
                stores.getPlacementPolicyStore()
                        .updateTargetPolicies(entry.getKey(), entry.getValue(),
                                allGroupsMap.row(entry.getKey()));
            }
            for (Entry<Long, List<DiscoveredSettingPolicyInfo>> entry : settingPoliciesByTarget.entrySet()) {
                stores.getSettingPolicyStore()
                        .updateTargetSettingPolicies(entry.getKey(), entry.getValue(),
                                allGroupsMap.row(entry.getKey()));
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
