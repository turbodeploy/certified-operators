package com.vmturbo.group.service;

import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.util.StopWatch;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CountGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupAndImmediateMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupAndImmediateMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsForEntitiesResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
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
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc.TargetsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.group.common.Truncator;
import com.vmturbo.group.group.DiscoveredGroupHash;
import com.vmturbo.group.group.GroupEnvironment;
import com.vmturbo.group.group.GroupEnvironmentTypeResolver;
import com.vmturbo.group.group.GroupSeverityCalculator;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.group.IGroupStore.DiscoveredGroup;
import com.vmturbo.group.group.TemporaryGroupCache;
import com.vmturbo.group.group.TemporaryGroupCache.InvalidTempGroupException;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.policy.DiscoveredPlacementPolicyUpdater;
import com.vmturbo.group.service.TransactionProvider.Stores;
import com.vmturbo.group.setting.DiscoveredSettingPoliciesUpdater;
import com.vmturbo.group.stitching.GroupStitchingContext;
import com.vmturbo.group.stitching.GroupStitchingManager;
import com.vmturbo.group.stitching.StitchingGroup;
import com.vmturbo.group.stitching.StitchingResult;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.proactivesupport.DataMetricHistogram;

/**
 * Implementation of group component services.
 */
public class GroupRpcService extends GroupServiceImplBase {

    private static final DataMetricHistogram GROUPS_BY_ID_SIZE_COUNTER =
            DataMetricHistogram.builder()
                    .withName("group_rpc_get_groups_response_size")
                    .withHelp("Number of groups requested to be loaded from Group RPC service within one request.")
                    .withBuckets(1, 10, 100, 1000, 10000, 100000, 1000000)
                    .build()
                    .register();

    private long realtimeTopologyContextId = 777777;
    static final int MAX_NESTING_DEPTH = 100;

    private final Logger logger = LogManager.getLogger();

    private final TemporaryGroupCache tempGroupCache;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final UserSessionContext userSessionContext;

    private final GroupStitchingManager groupStitchingManager;
    private final IdentityProvider identityProvider;

    private final TargetsServiceBlockingStub targetSearchService;
    private final DiscoveredSettingPoliciesUpdater settingPolicyUpdater;
    private final DiscoveredPlacementPolicyUpdater placementPolicyUpdater;
    private final GrpcTransactionUtil grpcTransactionUtil;
    private final GroupPermit groupRequestPermits;

    private final GroupMemberCalculator memberCalculator;

    private final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver;

    private final GroupSeverityCalculator groupSeverityCalculator;

    private final long groupLoadTimeoutMs;

    /**
     * Constructs group gRPC service.
     *
     * @param tempGroupCache temporary groups cache to store temp groups
     * @param searchServiceRpc search gRPC service client to resolve dynamic groups
     * @param userSessionContext user session context
     * @param groupStitchingManager groups stitching manager
     * @param transactionProvider transaction provider
     * @param identityProvider identity provider to assign OIDs to user groups
     * @param targetSearchService target search service for dynamic groups
     * @param settingPolicyUpdater updater for the discovered setting policies
     * @param placementPolicyUpdater updater for the discovered placement policies
     * @param memberCalculator Member calculator.
     * @param groupLoadPermits size of group batch used to retrieve groups from the DB
     * @param groupLoadTimeoutSec timeout to await for permits to load groups from DAO. If
     *         this timeout expires, {@link #getGroupsByIds(IGroupStore, List, StreamObserver,
     *         StopWatch, boolean)} will return a error.
     * @param groupEnvironmentTypeResolver utility class for group environment type resolution
     * @param groupSeverityCalculator utility class for group severity calculation
     */
    public GroupRpcService(@Nonnull final TemporaryGroupCache tempGroupCache,
            @Nonnull final SearchServiceBlockingStub searchServiceRpc,
            @Nonnull final UserSessionContext userSessionContext,
            @Nonnull final GroupStitchingManager groupStitchingManager,
            @Nonnull TransactionProvider transactionProvider,
            @Nonnull IdentityProvider identityProvider,
            @Nonnull TargetsServiceBlockingStub targetSearchService,
            @Nonnull DiscoveredSettingPoliciesUpdater settingPolicyUpdater,
            @Nonnull DiscoveredPlacementPolicyUpdater placementPolicyUpdater,
            GroupMemberCalculator memberCalculator,
            int groupLoadPermits,
            long groupLoadTimeoutSec,
            @Nonnull GroupEnvironmentTypeResolver groupEnvironmentTypeResolver,
            @Nonnull GroupSeverityCalculator groupSeverityCalculator) {
        this.tempGroupCache = Objects.requireNonNull(tempGroupCache);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.groupStitchingManager = Objects.requireNonNull(groupStitchingManager);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.targetSearchService = Objects.requireNonNull(targetSearchService);
        this.settingPolicyUpdater = Objects.requireNonNull(settingPolicyUpdater);
        this.placementPolicyUpdater = Objects.requireNonNull(placementPolicyUpdater);
        this.grpcTransactionUtil = new GrpcTransactionUtil(transactionProvider, logger);
        if (groupLoadPermits <= 0) {
            throw new IllegalArgumentException(
                    "Group load permits size must be a positive value, found: " + groupLoadPermits);
        }
        this.groupRequestPermits = new GroupPermit(groupLoadPermits);
        if (groupLoadTimeoutSec <= 0) {
            throw new IllegalArgumentException(
                    "Group load timeout must be a positive value, found: " + groupLoadTimeoutSec);
        }
        this.groupLoadTimeoutMs = groupLoadTimeoutSec * 1000;
        this.memberCalculator = memberCalculator;
        this.groupEnvironmentTypeResolver = Objects.requireNonNull(groupEnvironmentTypeResolver);
        this.groupSeverityCalculator = Objects.requireNonNull(groupSeverityCalculator);
    }

    @Override
    public void countGroups(GetGroupsRequest request,
            StreamObserver<CountGroupsResponse> responseObserver) {
        if (!request.hasGroupFilter()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("No group filter is present.").asException());
            return;
        }
        grpcTransactionUtil.executeOperation(responseObserver, (stores) -> {
            final Collection<Long> listOfGroups = getGroupIds(stores.getGroupStore(), request);
            responseObserver.onNext(
                    CountGroupsResponse.newBuilder().setCount(listOfGroups.size()).build());
            responseObserver.onCompleted();
        });
    }

    /**
     * Returns groups based on the request, in a paginated response.
     *
     * @param request the request for groups, including pagination, filtering & ordering params.
     * @param responseObserver the observer to notify with the response.
     */
    @Override
    public void getPaginatedGroups(GetPaginatedGroupsRequest request,
            StreamObserver<GetPaginatedGroupsResponse> responseObserver) {
        grpcTransactionUtil.executeOperation(responseObserver, stores -> {
            getPaginatedGroups(stores.getGroupStore(), request, responseObserver);
        });
    }

    /**
     * Internal implementation for getPaginatedGroups. Queries the store provided to get the
     * paginated response, and notifies the observer.
     *
     * @param groupStore the group store to query.
     * @param request the request for groups, including pagination, filtering & ordering params.
     * @param observer the observer to notify with the response.
     */
    private void getPaginatedGroups(@Nonnull IGroupStore groupStore,
            GetPaginatedGroupsRequest request,
            StreamObserver<GetPaginatedGroupsResponse> observer) {
        try {
            observer.onNext(groupStore.getPaginatedGroups(request));
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            final String errorMessage = "Invalid request for groups. Error: " + e.getMessage();
            logger.error(errorMessage);
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription(errorMessage)
                    .asException());
        }
    }

    @Override
    public void getGroups(GetGroupsRequest request,
            StreamObserver<Grouping> responseObserver) {
        if (!request.hasGroupFilter()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("No group filter is present.").asException());
            return;
        }
        grpcTransactionUtil.executeOperation(responseObserver, stores -> {
            getListOfGroups(stores.getGroupStore(), request, responseObserver);
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

    private void getListOfGroups(@Nonnull IGroupStore groupStore, GetGroupsRequest request,
            StreamObserver<Grouping> observer)
            throws StoreOperationException, InterruptedException {
        final StopWatch stopWatch = new StopWatch("GetListOfGroups");

        // Look up any temp groups explicitly specified in the request.
        // This is fast, so we do it before anything.
        if (request.getGroupFilter().getIncludeTemporary()) {
            request.getGroupFilter().getIdList().forEach(id -> {
                tempGroupCache.getGrouping(id).ifPresent(observer::onNext);
            });
        }

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
        GROUPS_BY_ID_SIZE_COUNTER.observe((double)filteredIds.size());
        try {
            getGroupsByIds(groupStore, filteredIds, observer, stopWatch, resolveGroupBasedFilters);
        } catch (TimeoutException e) {
            throw new StoreOperationException(Status.RESOURCE_EXHAUSTED,
                    "Timed out while quering the DB for groups", e);
        }
        observer.onCompleted();
        logger.debug(stopWatch::prettyPrint);
    }

    private void getGroupsByIds(@Nonnull IGroupStore groupStore, @Nonnull List<Long> oids,
            @Nonnull StreamObserver<Grouping> observer, @Nonnull StopWatch stopWatch,
            boolean resolveGroupBasedFilters) throws InterruptedException, TimeoutException {
        logger.debug("Trying to load {} groups by OIDs", oids::size);
        int processed = 0;
        while (processed < oids.size()) {
            stopWatch.start("Waiting for permits");
            int permits = groupRequestPermits.acquire(oids.size() - processed, groupLoadTimeoutMs);
            try {
                stopWatch.stop();
                logger.debug("Querying for the next {} groups starting from {}", permits, processed);
                final List<Long> sublist = oids.subList(processed, processed + permits);
                stopWatch.start("get groups by ids: " + permits);
                final Collection<Grouping> filteredGroups = groupStore.getGroupsById(sublist);
                stopWatch.stop();
                stopWatch.start("replace group properties filter");
                final Collection<Grouping> groupsResult;
                if (resolveGroupBasedFilters) {
                    groupsResult = filteredGroups.stream().map(group -> {
                        try {
                            return replaceGroupPropertiesWithGroupMembershipFilter(groupStore, group);
                        } catch (org.springframework.dao.DataAccessException | DataAccessException e) {
                            logger.error("Failed to replace group properties with group membership filter for group {}.", group, e);
                            return null;
                        }
                    })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
                } else {
                    groupsResult = filteredGroups;
                }
                groupsResult.forEach(observer::onNext);
                stopWatch.stop();
                processed += permits;
            } finally {
                groupRequestPermits.release(permits);
            }
        }
    }

    private boolean userScopeFilter(long groupId, @Nonnull Set<Long> requestedIds,
            @Nonnull List<Long> scopes, @Nonnull IGroupStore groupStore) throws StoreOperationException {
        // if the user is scoped, set up a filter to restrict the results based on their scope.
        // if the request contains scopes limit, set up a filter to restrict the results based on it.
        // if the request is for "all" groups and the user is scoped: we will filter results and
        // only return accessible ones.
        // If the request was for a specific set of groups: we will use a filter that will throw an
        // access exception if any groups are deemed "out of scope".
        if (!userSessionContext.isUserScoped() && isMarketScoped(scopes)) {
            return true;
        }

        final Collection<Long> members = memberCalculator.getGroupMembers(groupStore, Collections.singleton(groupId), true);
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
        grpcTransactionUtil.executeOperation(responseObserver,
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
            stores.getPlacementPolicyStore()
                    .deletePoliciesForGroupBeingRemoved(Collections.singleton(groupId));
            stores.getGroupStore().deleteGroup(gid.getId());
            responseObserver.onNext(DeleteGroupResponse.newBuilder().setDeleted(true).build());
            responseObserver.onCompleted();
        }
    }

    private boolean isMarketScoped(@Nonnull List<Long> scopes) {
        return scopes.isEmpty() || (scopes.size() == 1 && scopes.get(0).equals(realtimeTopologyContextId));
    }

    private GetMembersResponse makeMembersResponse(final long groupId, Set<Long> members) {
        members.remove(groupId);
        final GetMembersResponse response = GetMembersResponse.newBuilder()
                .setGroupId(groupId)
                .addAllMemberId(members)
                .build();
        return response;
    }

    @Override
    public void getMembers(final GroupDTO.GetMembersRequest request,
            final StreamObserver<GroupDTO.GetMembersResponse> responseObserver) {
        grpcTransactionUtil.executeOperation(responseObserver,
                (stores) -> getMembersResponse(stores.getGroupStore(), request, responseObserver));
    }

    /**
     * Iterates groups in request and streams results to consumer.
     * @param groupStore store to query groups
     * @param request request config for members
     * @param getMembersResponseConsumer {@link Consumer} accepting {@link GetMembersResponse} results
     * @throws StoreOperationException If there is an error interacting with the {@link IGroupStore}
     * @throws StatusRuntimeException Invalid argument, groupId not found
     */
    private void getMembers(@Nonnull IGroupStore groupStore,
                            final GroupDTO.GetMembersRequest request,
                            final Consumer<GetMembersResponse> getMembersResponseConsumer)
                    throws StoreOperationException, StatusRuntimeException {

        if (request.getIdCount() == 0) {
            final String errMsg = "Group ID is missing for the getMembers request";
            logger.error(errMsg);
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(errMsg));
        }

        final Collection<Grouping> tmpGroups = getTemporaryGroups(request.getIdList());
        for (Grouping group: tmpGroups) {
            GetMembersResponse getMembersResponse = getMembersResponseForGroup(groupStore, request, group);
            getMembersResponseConsumer.accept(getMembersResponse);
        }

        // Real (non-temporary groups)
        final Set<Long> realGroupIds = new HashSet<>(request.getIdList());
        realGroupIds.removeAll(Collections2.transform(tmpGroups, Grouping::getId));

        // we only need to go here if there is also some ids for non-temporary groups
        if (!realGroupIds.isEmpty()) {
            validateIfNotAllGroupsPresentThrowError(groupStore, request.getExpectPresent(), realGroupIds);

            for (Long groupId : realGroupIds) {
                try {
                    GetMembersResponse getMembersResponse = getMembersResponseForGroup(groupStore, request, groupId);
                    getMembersResponseConsumer.accept(getMembersResponse);
                } catch (StoreOperationException | RuntimeException e) {
                    // We don't want a failure to retrieve the members of one group to result in a failure
                    // to retrieve members of all groups.
                    // In the future it might be worth it to try to detect database connection issues
                    // here, and NOT retry in that case.
                    logger.error("Failed to retrieve members for group " + groupId, e);
                }
            }
        }
    }

    /**
     * Gets member responses to streamObserver.
     * @param groupStore store to query groups
     * @param request request config for members
     * @param responseObserver observers stream of messages
     * @throws StoreOperationException If there is an error interacting with the {@link IGroupStore}
     */
    private void getMembersResponse(@Nonnull IGroupStore groupStore,
                                    final GroupDTO.GetMembersRequest request,
                                    final StreamObserver<GroupDTO.GetMembersResponse> responseObserver)
                    throws StoreOperationException {
        try {
            Consumer<GetMembersResponse> getMembersResponseConsumer = (getMembersResponse -> responseObserver.onNext(getMembersResponse));
            getMembers(groupStore, request, getMembersResponseConsumer);
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        }
    }

    /**
     * If requested in {@link GroupDTO.GetMembersRequest}, throws error if not all groups exist.
     * @param groupStore used to query groups
     * @param throwErrorOnFailedValidation whether error should be thrown or not
     * @param nonTemporaryGroupIds collection of real group ids, temporary groups should be excluded
     * @throws StatusRuntimeException Invalid argument, groupId not found
     */
    private void validateIfNotAllGroupsPresentThrowError(@Nonnull IGroupStore groupStore,
                                                            boolean throwErrorOnFailedValidation,
                                                            @Nonnull Set<Long> nonTemporaryGroupIds)
                    throws StatusRuntimeException {
        if (throwErrorOnFailedValidation) {
            final Set<Long> existingRealGroups = groupStore.getExistingGroupIds(nonTemporaryGroupIds);
            if (!nonTemporaryGroupIds.equals(existingRealGroups)) {
                nonTemporaryGroupIds.removeAll(existingRealGroups);
                final String errMsg = "Cannot find groups with ids " + nonTemporaryGroupIds;
                logger.error(errMsg);
                throw new StatusRuntimeException(Status.NOT_FOUND.withDescription(errMsg));
            }
        }
    }

    /**
     * Get {@link GetMembersResponse} for groups.
     * @param groupStore used to query groups
     * @param request  request with different options to apply on members discovered
     * @param group the group in focus
     * @return {@link GetMembersResponse}
     * @throws StoreOperationException If there is an error interacting with the {@link IGroupStore}
     */
    private GetMembersResponse getMembersResponseForGroup(@Nonnull IGroupStore groupStore,
                                            GroupDTO.GetMembersRequest request,
                                            Grouping group) throws StoreOperationException {
        final Set<Long> members = memberCalculator.getGroupMembers(groupStore, group.getDefinition(),
                                                                   request.getExpandNestedGroups());
        // verify the user has access to all of the group members before returning any of them.
        if (request.getEnforceUserScope() && userSessionContext.isUserScoped()) {
            if (!request.getExpandNestedGroups()) {
                // Need to use the expanded members for checking access, if we didn't already fetch them
                UserScopeUtils.checkAccess(userSessionContext,
                        memberCalculator.getGroupMembers(groupStore, group.getDefinition(), true));
            } else {
                UserScopeUtils.checkAccess(userSessionContext, members);
            }
        }
        // return members
        logger.trace("Returning group ({}) with {} members", group.getId(), members.size());
        return makeMembersResponse(group.getId(), members);
    }

    /**
     * Get {@link GetMembersResponse} for groups.
     * @param groupStore used to query groups
     * @param request  request with different options to apply on memebers discovered
     * @param groupId groupUuid of focus
     * @return {@link GetMembersResponse}
     * @throws StoreOperationException If there is an error interacting with the {@link IGroupStore}
     */
    private GetMembersResponse getMembersResponseForGroup(@Nonnull IGroupStore groupStore,
                                                          GroupDTO.GetMembersRequest request,
                                                          Long groupId) throws StoreOperationException {
        final Set<Long> members = memberCalculator.getGroupMembers(groupStore, Collections.singleton(
                        groupId),
                                                                   request.getExpandNestedGroups());
        // verify the user has access to all of the group members before returning any of them.
        if (request.getEnforceUserScope() && userSessionContext.isUserScoped()) {
            if (!request.getExpandNestedGroups()) {
                // Need to use the expanded members for checking access, if we didn't already fetch them
                UserScopeUtils.checkAccess(userSessionContext,
                                           memberCalculator.getGroupMembers(groupStore, Collections.singleton(groupId), true));
            } else {
                UserScopeUtils.checkAccess(userSessionContext, members);
            }
        }
        // return members
        logger.trace("Returning group ({}) with {} members", groupId, members.size());
        return makeMembersResponse(groupId, members);
    }

    /**
     * Returns only temporary groups from list of ids.
     * @param groupIds list of ids to focus
     * @return List of temporary {@link Grouping}
     */
    @Nonnull
    private List<Grouping> getTemporaryGroups(@Nonnull final List<Long> groupIds) {
        return groupIds
            .stream()
            .map(tempGroupCache::getGrouping)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
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
        grpcTransactionUtil.executeOperation(responseObserver,
                (stores) -> getGroupForEntity(stores.getGroupStore(), request, responseObserver));
    }

    private void getGroupForEntity(@Nonnull IGroupStore groupStore,
            @Nonnull GetGroupsForEntitiesRequest request,
            @Nonnull StreamObserver<GetGroupsForEntitiesResponse> responseObserver)
            throws StoreOperationException {
        final Map<Long, Set<Long>> staticGroupsPerEntity =
                memberCalculator.getEntityGroups(groupStore,
                    new HashSet<>(request.getEntityIdList()),
                    new HashSet<>(request.getGroupTypeList()));
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
        grpcTransactionUtil.executeOperation(responseObserver, (stores) -> {
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
        grpcTransactionUtil.executeOperation(responseObserver, stores -> {
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
        grpcTransactionUtil.executeOperationAndReturn(responseObserver,
                stores -> createGroup(stores.getGroupStore(), request));
    }

    private CreateGroupResponse createGroup(@Nonnull IGroupStore groupStore, @Nonnull CreateGroupRequest request)
            throws StoreOperationException {
        try {
            validateCreateGroupRequest(groupStore, request);
        } catch (InvalidGroupDefinitionException e) {
            logger.error("Group {} is not valid", request.getGroupDefinition(), e);
            throw new StoreOperationException(Status.INVALID_ARGUMENT, e.getMessage(), e);
        }

        logger.info("Creating group {}", request.getGroupDefinition().getDisplayName());

        final GroupDefinition groupDef = request.getGroupDefinition();

        Grouping createdGroup = null;

        final Set<MemberType> expectedTypes = findGroupExpectedTypes(groupStore, groupDef);

        if (groupDef.getIsTemporary()) {
            if (groupDef.hasOptimizationMetadata()
                            && !groupDef.getOptimizationMetadata().getIsGlobalScope()) {
                UserScopeUtils.checkAccess(userSessionContext,
                        memberCalculator.getGroupMembers(groupStore, groupDef, true));
            }

            try {
                createdGroup = tempGroupCache.create(groupDef, request.getOrigin(), expectedTypes);
            } catch (InvalidTempGroupException e) {
                final String errorMsg = String.format("Failed to create group: %s as it is invalid. exception: %s.",
                                groupDef, e.getLocalizedMessage());
                logger.error(errorMsg, e);
                throw new StoreOperationException(Status.ABORTED, errorMsg, e);
            }
        } else {
            final boolean supportsMemberReverseLookup =
                            determineMemberReverseLookupSupported(groupDef);

            if (userSessionContext.isUserScoped()) {
                // verify that the members of the new group would all be in scope
                UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(),
                                memberCalculator.getGroupMembers(groupStore, groupDef, true));
            }

            long groupOid = identityProvider.next();
            groupStore.createGroup(groupOid, request.getOrigin(), groupDef, expectedTypes,
                                    supportsMemberReverseLookup);
            // We split group creation to two different methods to benefit from the group membership
            // cache.
            // Supplementary info derive from group members, so we create them after the cache has
            // been updated.
            storeSingleGroupSupplementaryInfo(groupStore, groupOid);
            createdGroup = Grouping.newBuilder()
                .setId(groupOid)
                .setDefinition(groupDef)
                .addAllExpectedTypes(expectedTypes)
                .setSupportsMemberReverseLookup(supportsMemberReverseLookup)
                .build();
                postValidateNewGroup(groupStore, createdGroup);
        }

        return CreateGroupResponse.newBuilder()
                .setGroup(createdGroup)
                .build();
    }

    /**
     * Creates group supplementary info that derive from group members.
     * These data currently include emptiness, environment type & cloud type, severity.
     *
     * @param groupStore group store to use for queries
     * @param groupId the group to update
     * @throws StoreOperationException on db error
     */
    private void storeSingleGroupSupplementaryInfo(@Nonnull IGroupStore groupStore,
            final long groupId) throws StoreOperationException {
        Set<Long> groupEntities = memberCalculator.getGroupMembers(groupStore,
                Collections.singleton(groupId), true);
        final boolean isEmpty = groupEntities.size() == 0;
        List<PartialEntity> entitiesWithEnvironment =
                fetchEntitiesWithEnvironment(groupEntities);
        if (entitiesWithEnvironment == null) {
            return;
        }
        // calculate environment type based on members environment type
        GroupEnvironment groupEnvironment =
                groupEnvironmentTypeResolver.getEnvironmentAndCloudTypeForGroup(groupId,
                        entitiesWithEnvironment
                                .stream()
                                .map(PartialEntity::getWithOnlyEnvironmentTypeAndTargets)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toSet()),
                        ArrayListMultimap.create());
        // calculate severity based on members severity
        Severity groupSeverity = groupSeverityCalculator.calculateSeverity(groupEntities);
        // add group info to the database
        groupStore.createGroupSupplementaryInfo(groupId, isEmpty, groupEnvironment, groupSeverity);
    }

    private void validateCreateGroupRequest(@Nonnull final IGroupStore groupStore,
                                            @Nonnull final CreateGroupRequest request)
                    throws InvalidGroupDefinitionException {
        if (!request.hasGroupDefinition()) {
            throw new InvalidGroupDefinitionException("No group definition is present.");
        }

        if (!request.hasOrigin()) {
            throw new InvalidGroupDefinitionException("No origin definition is present.");
        }

        validateGroupDefinition(groupStore, request.getGroupDefinition());

    }

    @Override
    public void updateGroup(@Nonnull UpdateGroupRequest request,
            @Nonnull StreamObserver<UpdateGroupResponse> responseObserver) {
        grpcTransactionUtil.executeOperation(responseObserver,
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
            validateGroupDefinition(groupStore, groupDefinition);
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
                            memberCalculator.getGroupMembers(groupStore, groupDefinition, true));
        }

        final boolean supportsMemberReverseLookup =
                        determineMemberReverseLookupSupported(groupDefinition);

        final Set<MemberType> expectedTypes = findGroupExpectedTypes(groupStore, groupDefinition);
        final Grouping newGroup =
                groupStore.updateGroup(request.getId(), groupDefinition, expectedTypes,
                        supportsMemberReverseLookup);
        // We split updating some information related to group to a different method, to benefit
        // from the group membership cache.
        // Supplementary info derive from group members, so we update them after the cache has been
        // updated (during the previous call).
        updateSingleGroupSupplementaryInfo(groupStore, newGroup.getId());
        postValidateNewGroup(groupStore, newGroup);

        final UpdateGroupResponse res =
                UpdateGroupResponse.newBuilder().setUpdatedGroup(newGroup).build();
        responseObserver.onNext(res);
        responseObserver.onCompleted();
    }

    /**
     * Updates group supplementary info that derive from group members.
     * These data currently include emptiness, environment type & cloud type, severity.
     *
     * @param groupStore group store to use for queries
     * @param groupId the group to update
     * @throws StoreOperationException on db error
     */
    private void updateSingleGroupSupplementaryInfo(@Nonnull IGroupStore groupStore,
            final long groupId) throws StoreOperationException {
        Set<Long> groupEntities =
            memberCalculator.getGroupMembers(groupStore, Collections.singleton(groupId), true);
        List<PartialEntity> entitiesWithEnvironment =
                fetchEntitiesWithEnvironment(groupEntities);
        if (entitiesWithEnvironment == null) {
            return;
        }
        // calculate environment type based on members environment type
        GroupEnvironment groupEnvironment =
                groupEnvironmentTypeResolver.getEnvironmentAndCloudTypeForGroup(groupId,
                        entitiesWithEnvironment
                                .stream()
                                .map(PartialEntity::getWithOnlyEnvironmentTypeAndTargets)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toSet()),
                        ArrayListMultimap.create());
        // calculate severity based on members severity
        Severity groupSeverity = groupSeverityCalculator.calculateSeverity(groupEntities);
        // add record to the database
        groupStore.updateSingleGroupSupplementaryInfo(groupId, groupEntities.isEmpty(),
                groupEnvironment, groupSeverity);
    }

    /**
     * Given a set of entities, it returns a list of those entities with information about their
     * environment type and the targets that discovered them, by querying the repository.
     *
     * @param entities the entities to query for
     * @return A list of {@link PartialEntity} containing
     *         {@link PartialEntity.EntityWithOnlyEnvironmentTypeAndTargets} for the entities
     *         provided.
     */
    private List<PartialEntity> fetchEntitiesWithEnvironment(Set<Long> entities) {
        List<PartialEntity> entitiesWithEnvironment = new ArrayList<>();
        if (entities.isEmpty()) {
            return entitiesWithEnvironment;
        }
        SearchEntitiesRequest request = SearchEntitiesRequest.newBuilder()
                .addAllEntityOid(entities)
                .setReturnType(Type.WITH_ONLY_ENVIRONMENT_TYPE_AND_TARGETS)
                .build();
        // (When there's no real time topology in repository, we get a status runtime
        // exception.)
        try {
            searchServiceRpc.searchEntitiesStream(request).forEachRemaining(e ->
                    entitiesWithEnvironment.addAll(e.getEntitiesList()));
        } catch (StatusRuntimeException e) {
            logger.warn(
                    "Request for entities failed. Group supplementary info cannot be updated.");
            return null;
        }
        return entitiesWithEnvironment;
    }

    @Override
    public void getGroup(@Nonnull GroupID request,
            @Nonnull StreamObserver<GetGroupResponse> responseObserver) {
        grpcTransactionUtil.executeOperation(responseObserver,
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
                UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(), memberCalculator.getGroupMembers(group.get()));
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

    @Override
    public void getGroupAndImmediateMembers(@Nonnull GetGroupAndImmediateMembersRequest request,
                         @Nonnull StreamObserver<GetGroupAndImmediateMembersResponse> responseObserver) {
        grpcTransactionUtil.executeOperation(responseObserver,
                                             stores -> getGroupAndImmediateMembers(stores.getGroupStore(), request, responseObserver));
    }

    /**
     * Get a static or dynamic group with its immediate members.
     *
     * <p>Immediate members are 1 level removed, will not expand nested groups</p>
     * @param groupStore store to query groups
     * @param request request config for members
     * @param responseObserver the observer which notifies client of any result
     */
    private void getGroupAndImmediateMembers(@Nonnull IGroupStore groupStore, @Nonnull GetGroupAndImmediateMembersRequest request,
                          @Nonnull StreamObserver<GetGroupAndImmediateMembersResponse> responseObserver) {
        if (!request.hasGroupId()) {
            final String errMsg = "Invalid GroupID input for get a group: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }

        logger.debug("Attempting to retrieve group: {}", request);

        try {
            Optional<Grouping> group = getGroupById(groupStore, request.getGroupId());
            GetGroupAndImmediateMembersResponse.Builder builder = GetGroupAndImmediateMembersResponse.newBuilder();

            if (group.isPresent()) {
                //Get the group members
                builder.setGroup(group.get());
                if (group.get().getDefinition().hasStaticGroupMembers()) {
                    // Members that are groups exist on the groupDefinition so will not be included
                    builder.addAllImmediateMembers(group.get()
                        .getDefinition()
                        .getStaticGroupMembers()
                        .getMembersByTypeList().stream()
                        .flatMap(membersByType -> membersByType.getMembersList().stream())
                        .collect(Collectors.toList()));
                } else {
                    //dynamic groups
                    GetMembersRequest getMembersRequest = GetMembersRequest.newBuilder()
                                    .addId(request.getGroupId())
                                    .setExpectPresent(true)
                                    .build();
                    Consumer<GetMembersResponse> getMembersResponseConsumer =
                                    (getMembersResponse -> builder.addAllImmediateMembers(getMembersResponse.getMemberIdList()));

                    getMembers(groupStore, getMembersRequest, getMembersResponseConsumer);
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            final String errorMsg = String.format("Failed to retrieve group: %s due to data access error: %s",
                                                  request.hasGroupId(), e.getLocalizedMessage());
            logger.error(errorMsg, e);
            responseObserver.onError(Status.INTERNAL.withDescription(errorMsg).asRuntimeException());
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (StoreOperationException e) {
            final String errorMsg = "Failed to retrieve members";
            responseObserver.onError(Status.INTERNAL.withDescription(errorMsg).asRuntimeException());
        }
    }

    @Nonnull
    Optional<Grouping> getGroupById(@Nonnull IGroupStore groupStore, long groupId) {
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
                memberCalculator.getGroupMembers(groupStore, Collections.singleton(groupId), true));
    }

    @VisibleForTesting
    void validateGroupDefinition(@Nonnull final IGroupStore groupStore,
                                 @Nonnull GroupDefinition groupDefinition)
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
                // If using a group filter, make sure the filter is valid. The most notable type
                // of invalid group is a group with an invalid regex. This is tricky to validate,
                // because MySQL has different REQEX validation criteria than Java's native regex.
                // Instead, we check that running this group filter against the GroupStore produces
                // some results (even if empty).
                try {
                    groupStore.getGroupIds(groupDefinition.getGroupFilters());
                } catch (BadSqlGrammarException e) {
                    // This is a particular exception that gets thrown if the regexes in the group
                    // filter are invalid by MySQL standards.
                    //
                    // Other DataAccessExceptions (e.g. if there is an error connecting to the DB)
                    // will propagate upwards, and won't lead to an INVALID_ARGUMENT return code.
                    throw new InvalidGroupDefinitionException("Group filter invalid: " + e.getSQLException().getMessage());
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
     * Perform post validation operations on a new created group. If we find out the group should
     * not have been created than we roll back the transaction.
     *
     * @param groupStore group store to use
     * @param newGroup the group that is being created
     * @throws StoreOperationException if the group should not be created and the transaction
     * should be rolled back
     */
    private void postValidateNewGroup(@Nonnull final IGroupStore groupStore,
                                     @Nonnull Grouping newGroup) throws StoreOperationException {
        final GroupDefinition groupDefinition = newGroup.getDefinition();
        switch (groupDefinition.getSelectionCriteriaCase()) {
            // Check that the new group is not a member of a group and at the same time has that
            // group as one of its members. We perform this check here since it's easier to to
            // apply the regEx filters in sql.
            case GROUP_FILTERS:
                Set<Long> groupMembers = memberCalculator.getGroupMembers(groupStore, groupDefinition, false);
                Collection<Grouping> groups = groupStore.getGroupsById(groupMembers);
                for (Grouping group : groups) {
                    Set<Long> members = memberCalculator.getGroupMembers(groupStore, group.getDefinition(), false);
                    int depth = 0;
                    while (members.size() > 0) {
                        if (depth > MAX_NESTING_DEPTH) {
                            throw new StoreOperationException(Status.ABORTED, "Max depth"
                                + " exceeded, could not create the group");
                        }
                        if (members.contains(newGroup.getId())) {
                            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                                "Recursive group definition: this group is contained and "
                                    + "contains the same group with name "
                                    + group.getDefinition().getDisplayName());
                        }
                        members = groupStore.getMembers(members, false).getGroupIds();
                        depth += 1;
                    }
                }
        }
    }

    @Nonnull
    private DiscoveredGroup createDiscoveredGroup(@Nonnull IGroupStore groupStore,
            @Nonnull StitchingGroup src) {
        final GroupDefinition groupDefinition = src.buildGroupDefinition();
        final Set<MemberType> expectedMembers = findGroupExpectedTypes(groupStore, groupDefinition);
        final String truncatedSourceIdentifier =
            Truncator.truncateGroupSourceIdentifier(src.getSourceId(), true);
        return new DiscoveredGroup(src.getOid(), groupDefinition, truncatedSourceIdentifier,
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

        private final Map<Long, Collection<DiscoveredPolicyInfo>> policiesByTarget = new HashMap<>();
        private final Map<Long, Collection<DiscoveredSettingPolicyInfo>> settingPoliciesByTarget = new HashMap<>();
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
                logger.debug("Target {} reported following setting policies: {}", () -> targetId,
                        () -> record.getDiscoveredSettingPoliciesList()
                                .stream()
                                .map(DiscoveredSettingPolicyInfo::getName)
                                .collect(Collectors.joining(",", "[", "]")));
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
            grpcTransactionUtil.executeOperation(responseObserver, this::onCompleted);
        }

        private void onCompleted(@Nonnull Stores stores) throws StoreOperationException {
            // stitch all groups, e.g. merge same groups from different targets into one
            final StitchingResult stitchingResult =
                    groupStitchingManager.stitch(stores.getGroupStore(), groupStitchingContext);
            final List<DiscoveredGroup> groupsToAdd = new ArrayList<>();
            final List<DiscoveredGroup> groupsToUpdate = new ArrayList<>();
            final List<Long> unchangedGroups = new ArrayList<>();
            for (StitchingGroup group: stitchingResult.getGroupsToAddOrUpdate()) {
                final DiscoveredGroup discoveredGroup = createDiscoveredGroup(
                        stores.getGroupStore(), group);
                if (group.isNewGroup()) {
                    groupsToAdd.add(discoveredGroup);
                } else {
                    final byte[] existingHash = group.getExistingHash();
                    final byte[] actualHash = DiscoveredGroupHash.hash(discoveredGroup);
                    if (Arrays.equals(existingHash, actualHash)) {
                        unchangedGroups.add(group.getOid());
                    } else {
                        groupsToUpdate.add(discoveredGroup);
                    }
                }
            }
            logger.info("Got {} new groups, {} for update, {} unchanged and {} to delete",
                    groupsToAdd.size(), groupsToUpdate.size(), unchangedGroups.size(),
                    stitchingResult.getGroupsToDelete().size());
            if (!stitchingResult.getGroupsToDelete().isEmpty()) {
                logger.info("Following groups are getting deleted: ", stitchingResult.getGroupsToDelete());
            }
            logger.debug("The following {} groups will not be updated as they are not changed: {}",
                    unchangedGroups::size, unchangedGroups::toString);
            // First, we need to remove setting policies and placement policies for the groups
            // that are removed. After the groups are removed themselves, a link between groups
            // and policies will be lost
            stores.getPlacementPolicyStore()
                    .deletePoliciesForGroupBeingRemoved(stitchingResult.getGroupsToDelete());
            stores.getGroupStore()
                    .updateDiscoveredGroups(groupsToAdd, groupsToUpdate,
                            stitchingResult.getGroupsToDelete());
            final Table<Long, String, Long> allGroupsMap = createGroupIdTable(stitchingResult);

            placementPolicyUpdater.updateDiscoveredPolicies(stores.getPlacementPolicyStore(),
                    policiesByTarget, allGroupsMap, groupStitchingContext.getUndiscoveredTargets());
            settingPolicyUpdater.updateSettingPolicies(stores.getSettingPolicyStore(),
                    settingPoliciesByTarget, allGroupsMap, groupStitchingContext.getUndiscoveredTargets());
            responseObserver.onNext(StoreDiscoveredGroupsPoliciesSettingsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    /**
     * Special case of semaphore. It is able to acquire any available permits, when requested
     * permits is more the existing permits.
     */
    private static class GroupPermit {
        private int permits;
        private final Object lock = new Object();
        private final Logger logger = LogManager.getLogger();

        /**
         * Consctucts semaphore with the initial value of permits.
         * @param permits initial number of permits
         */
        GroupPermit(int permits) {
            if (permits <= 0) {
                throw new IllegalArgumentException("Number of permits must be positive");
            }
            logger.debug("Created a groups request semaphore with {} permits", permits);
            this.permits = permits;
        }

        /**
         * Acquire no more then a specified number of permits. If there is not enough permits
         * in the pool (requested is larger then existing) all the existing permits are returned.
         *
         * @param requested requested number of permits to acquire
         * @param timeoutMs timeout to await if no permits are available
         * @return number of actually acquired permits
         * @throws InterruptedException if current thread interrupted waiting for permits
         * @throws TimeoutException if time is out when awaiting for any permits
         */
        public int acquire(int requested, long timeoutMs)
                throws InterruptedException, TimeoutException {
            if (requested <= 0) {
                throw new IllegalArgumentException("Number of requested permits must be positive");
            }
            final long startTime = System.currentTimeMillis();
            synchronized (lock) {
                while (true) {
                    if (permits > 0) {
                        final int retVal = Math.min(permits, requested);
                        permits -= retVal;
                        logger.trace(
                                "Successfully acquired {} permits. {} permits are still available",
                                retVal, permits);
                        return retVal;
                    } else {
                        final long timeToWait = timeoutMs + startTime - System.currentTimeMillis();
                        logger.trace("No permits available. Waiting for {}ms...", timeToWait);
                        if (timeToWait < 0) {
                            throw new TimeoutException("Timed out while waiting for permits");
                        }
                        lock.wait(timeToWait);
                    }
                }
            }
        }

        /**
         * Releases the specified number of permits.
         *
         * @param permits number of permits to release
         */
        public void release(int permits) {
            synchronized (lock) {
                this.permits += permits;
                lock.notifyAll();
            }
            logger.trace("Successfully released {} permits. Total available permits became {}",
                    permits, this.permits);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + permits + "]";
        }
    }
}
