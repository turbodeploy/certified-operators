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
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateNestedGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateNestedGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterForEntityResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetTagsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.NestedGroupInfo.TypeCase;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsPoliciesSettingsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateNestedGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateNestedGroupResponse;
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
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableGroupUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.GroupNotFoundException;
import com.vmturbo.group.group.EntityToClusterMapping;
import com.vmturbo.group.group.GroupStore;
import com.vmturbo.group.group.GroupStore.GroupNotClusterException;
import com.vmturbo.group.group.TemporaryGroupCache;
import com.vmturbo.group.group.TemporaryGroupCache.InvalidTempGroupException;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.policy.PolicyStore.PolicyDeleteException;
import com.vmturbo.group.setting.SettingStore;

public class GroupRpcService extends GroupServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final GroupStore groupStore;

    private final TemporaryGroupCache tempGroupCache;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final EntityToClusterMapping entityToClusterMapping;

    private final DSLContext dslContext;

    private final PolicyStore policyStore;

    private final SettingStore settingStore;

    private final UserSessionContext userSessionContext;

    public GroupRpcService(@Nonnull final GroupStore groupStore,
                           @Nonnull final TemporaryGroupCache tempGroupCache,
                           @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                           @Nonnull final EntityToClusterMapping entityToClusterMapping,
                           @Nonnull final DSLContext dslContext,
                           @Nonnull final PolicyStore policyStore,
                           @Nonnull final SettingStore settingStore,
                           @Nonnull final UserSessionContext userSessionContext) {
        this.groupStore = Objects.requireNonNull(groupStore);
        this.tempGroupCache = Objects.requireNonNull(tempGroupCache);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
        this.entityToClusterMapping = Objects.requireNonNull(entityToClusterMapping);
        this.dslContext = Objects.requireNonNull(dslContext);
        this.policyStore = Objects.requireNonNull(policyStore);
        this.settingStore = Objects.requireNonNull(settingStore);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
    }

    @Override
    public void createGroup(GroupInfo groupInfo, StreamObserver<CreateGroupResponse> responseObserver) {
        logger.info("Creating a group: {}", groupInfo);

        try {
            if (userSessionContext.isUserScoped()) {
                // verify that the members of the new group would all be in scope
                UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(), getNormalGroupMembers(groupInfo));
            }
            final Group createdGroupOpt = groupStore.newUserGroup(groupInfo);
            responseObserver.onNext(CreateGroupResponse.newBuilder()
                    .setGroup(createdGroupOpt)
                    .build());
            responseObserver.onCompleted();
        } catch (DataAccessException | DuplicateNameException e) {
            logger.error("Failed to create group: {}", groupInfo, e);
            responseObserver.onError(Status.ABORTED.withDescription(e.getLocalizedMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void createNestedGroup(final CreateNestedGroupRequest request,
                                  final StreamObserver<CreateNestedGroupResponse> responseObserver) {
        if (!request.hasGroupInfo()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("No group info present.").asException());
            return;
        }

        final NestedGroupInfo groupInfo = request.getGroupInfo();
        if (userSessionContext.isUserScoped()) {
            // verify that all members of the nested groups will fit in the scope.
            UserScopeUtils.checkAccess(userSessionContext,
                getNestedGroupMembers(request.getGroupInfo(), true));
        }

        // If the group has a static selection criteria, verify that all the underlying groups
        // exist.
        if (groupInfo.getSelectionCriteriaCase() == SelectionCriteriaCase.STATIC_GROUP_MEMBERS) {
            final Map<Long, Optional<Group>> results =
                groupStore.getGroups(groupInfo.getStaticGroupMembers().getStaticMemberOidsList());
            for (Long memberId : groupInfo.getStaticGroupMembers().getStaticMemberOidsList()) {
                if (!results.getOrDefault(memberId, Optional.empty()).isPresent()) {
                    responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Group " + memberId + " does not exist.")
                        .asException());
                    return;
                }
            }
        }

        try {
            final Group group = groupStore.newUserNestedGroup(groupInfo);
            responseObserver.onNext(CreateNestedGroupResponse.newBuilder()
                .setGroup(group)
                .build());
            responseObserver.onCompleted();
        } catch (DataAccessException | DuplicateNameException e) {
            logger.error("Failed to create group: {}", groupInfo, e);
            responseObserver.onError(Status.ABORTED.withDescription(e.getLocalizedMessage())
                .asException());
        }
    }

    @Override
    public void createTempGroup(final CreateTempGroupRequest request,
                                  final StreamObserver<CreateTempGroupResponse> responseObserver) {
        if (!request.hasGroupInfo()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("No group info present.").asException());
            return;
        }

        // check scope access if not a global temp group
        TempGroupInfo groupInfo = request.getGroupInfo();
        if (!groupInfo.getIsGlobalScopeGroup() && groupInfo.hasMembers()) {
            UserScopeUtils.checkAccess(userSessionContext, groupInfo.getMembers().getStaticMemberOidsList());
        }

        try {
            final Group group = tempGroupCache.create(groupInfo);
            responseObserver.onNext(CreateTempGroupResponse.newBuilder()
                    .setGroup(group)
                    .build());
            responseObserver.onCompleted();
        } catch (InvalidTempGroupException e) {
            logger.error("Failed to create temporary group", e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void getGroup(GroupID gid, StreamObserver<GetGroupResponse> responseObserver) {
        if (!gid.hasId()) {
            final String errMsg = "Invalid GroupID input for get a group: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
            return;
        }

        logger.debug("Attempting to retrieve group: {}", gid);

        try {
            Optional<Group> group = getGroupWithId(gid.getId());
            // Patrick - removing this check, as it's preventing retrieval of data for plans. We will
            // re-enable this with OM-44360
            /*
            if (userSessionContext.isUserScoped() && group.isPresent()) {
                // verify that the members of the new group would all be in scope
                UserScopeUtils.checkAccess(userSessionContext.getUserAccessScope(), getNormalGroupMembers(group.get(), true));
            }
            */
            GetGroupResponse.Builder builder = GetGroupResponse.newBuilder();
            group.ifPresent(builder::setGroup);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            logger.error("Failed to retrieve group: {}", gid, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getLocalizedMessage()).asException());
        }
    }

    private Optional<Group> getGroupWithId(Long groupId) {
        // Try the temporary group cache first because it's much faster.
        Optional<Group> group = tempGroupCache.get(groupId);
        if (!group.isPresent()) {
            group = groupStore.get(groupId);
        }
        return group;
    }

    /**
     * Check if the user has access to the group identified by Id. A user has access to the group by
     * default, but if the user is a "scoped" user, they will only have access to the group if all
     * members of the group are in the user's scope.
     *
     * This method will trigger a {@link UserAccessScopeException} if the group exists and the user
     * does not have access to it, otherwise it will return quietly.
     *
     * @param groupId the group id to check
     *
     */
    private void checkUserAccessToGroup(Long groupId) {
        if (! userHasAccessToGroup(groupId)) {
            throw new UserAccessScopeException("User does not have access to group "+ groupId);
        }
    }

    /**
     * Check if the user has access to the group identified by Id. A user has access to the group by
     * default, but if the user is a "scoped" user, they will only have access to the group if all
     * members of the group are in the user's scope, or if the group itself is explicitly in the user's
     * scope groups. (in the case of a group that no longer exists)
     *
     * TODO: move this to a helper class that could be shared by PolicyRpcService and GroupRpcService.
     * This would mean moving getGroupById() and getNormalGroupMembers() as well though.
     *
     * @param groupId the group id to check access for
     * @return true, if the user definitely has access to the group. false, if not.
     */
    public boolean userHasAccessToGroup(long groupId) {
        if (! userSessionContext.isUserScoped()) {
            return true;
        }
        // if the user scope groups contains the group id directly, we don't even need to expand the
        // group.
        EntityAccessScope entityAccessScope = userSessionContext.getUserAccessScope();
        if (entityAccessScope.getScopeGroupIds().contains(groupId)) {
            return true;
        }
        Optional<Group> optionalGroup = getGroupWithId(groupId);
        if (optionalGroup.isPresent()) {
            // check membership
            return entityAccessScope.contains(getNormalGroupMembers(optionalGroup.get(), true));
        } else {
            // the group does not exist any more - we'll return false to be safe, although it is
            // possible that the user had access when the group did exist.
            return false;
        }
    }

    @Override
    public void getGroups(GetGroupsRequest request, StreamObserver<Group> responseObserver) {
        logger.trace("Get all user groups");

        boolean resolveClusterFilters = request.hasResolveClusterSearchFilters() &&
            request.getResolveClusterSearchFilters();

        final Set<Long> requestedIds = new HashSet<>(request.getIdList());
        // if the user is scoped, set up a filter to restrict the results based on their scope.
        // if the request is for "all" groups: we will filter results and only return accessible ones.
        // If the request was for a specific set of groups: we will use a filter that will throw an
        // access exception if any groups are deemed "out of scope".
        Predicate<Group> userScopeFilter = userSessionContext.isUserScoped()
                ? requestedIds.isEmpty()
                    ? group -> userSessionContext.getUserAccessScope().contains(getNormalGroupMembers(group, true))
                    : group -> UserScopeUtils.checkAccess(userSessionContext, getNormalGroupMembers(group, true))
                : group -> true;
        try {
            final boolean requestTempGroups = request.getTypeFilterList().contains(Type.TEMP_GROUP);
            // Only return temp groups if explicitly requested.
            final Stream<Group> tempGroupStream = requestTempGroups ?
                    tempGroupCache.getAll().stream() : Stream.empty();
            // Return non-temp groups, unless the user ONLY requested temp groups.
            // TODO: if specific group ids were requested, consider getting a stream of only those
            // groups from the group dstore rather than all, if the number of groups gets very large.
            final Stream<Group> otherGroupStream =
                requestTempGroups && request.getTypeFilterCount() == 1 ?
                    Stream.empty() : groupStore.getAll().stream();
            List<Group> responseGroups = Stream.concat(tempGroupStream, otherGroupStream)

                    .filter(group -> requestedIds.isEmpty() || requestedIds.contains(group.getId()))
                    .filter(group -> !request.hasOriginFilter() ||
                            group.getOrigin().equals(request.getOriginFilter()))
                    .filter(group -> request.getTypeFilterList().isEmpty() ||
                            request.getTypeFilterList().contains(group.getType()))
                    .filter(group ->
                            matchFilters(request.getPropertyFilters().getPropertyFiltersList(), group))
                    .filter(group -> !request.hasClusterFilter() ||
                            GroupProtoUtil.clusterFilterMatcher(group, request.getClusterFilter()))
                    .map(group -> resolveClusterFilters ? resolveClusterFilters(group) : group)
                    .filter(userScopeFilter)
                    .collect(Collectors.toList());

            responseGroups.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            logger.error("Failed to query group store for group definitions.", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getLocalizedMessage())
                    .asException());
        }
    }

    @Override
    public void updateNestedGroup(UpdateNestedGroupRequest request,
                                  StreamObserver<UpdateNestedGroupResponse> responseObserver) {
        if (!request.hasGroupId() || !request.hasNewGroupInfo()) {
            final String errMsg = "Invalid GroupID input for group update: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
            return;
        }

        logger.info("Updating a group: {}", request);

        if (userSessionContext.isUserScoped()) {
            // verify the user has access to the group they are trying to modify
            checkUserAccessToGroup(request.getGroupId());
            // verify the modified version would fit in scope too
            UserScopeUtils.checkAccess(userSessionContext, getNestedGroupMembers(request.getNewGroupInfo(), true));
        }

        try {
            Group newGroup = groupStore.updateUserNestedGroup(request.getGroupId(), request.getNewGroupInfo());
            final UpdateNestedGroupResponse res = UpdateNestedGroupResponse.newBuilder()
                .setUpdatedGroup(newGroup)
                .build();
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        } catch (ImmutableGroupUpdateException e) {
            logger.error("Failed to update group {} due to error: {}",
                request.getGroupId(), e.getLocalizedMessage());
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getLocalizedMessage()).asException());
        } catch (GroupNotFoundException e) {
            logger.error("Failed to update group {} because it doesn't exist.",
                request.getGroupId(), e.getLocalizedMessage());
            responseObserver.onError(Status.NOT_FOUND
                .withDescription(e.getLocalizedMessage()).asException());
        } catch (DataAccessException | DuplicateNameException e) {
            logger.error("Failed to update group " + request.getGroupId(), e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getLocalizedMessage()).asException());
        }
    }

    @Override
    public void updateGroup(UpdateGroupRequest request,
                            StreamObserver<UpdateGroupResponse> responseObserver) {
        if (!request.hasId() || !request.hasNewInfo()) {
            final String errMsg = "Invalid GroupID input for group update: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
            return;
        }

        logger.info("Updating a group: {}", request);

        if (userSessionContext.isUserScoped()) {
            // verify the user has access to the group they are trying to modify
            checkUserAccessToGroup(request.getId());
            // verify the modified version would fit in scope too
            UserScopeUtils.checkAccess(userSessionContext, getNormalGroupMembers(request.getNewInfo()));
        }

        try {
            Group newGroup = groupStore.updateUserGroup(request.getId(), request.getNewInfo());
            final UpdateGroupResponse res = UpdateGroupResponse.newBuilder()
                    .setUpdatedGroup(newGroup)
                    .build();
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        } catch (ImmutableGroupUpdateException e) {
            logger.error("Failed to update group {} due to error: {}",
                    request.getId(), e.getLocalizedMessage());
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getLocalizedMessage()).asException());
        } catch (GroupNotFoundException e) {
            logger.error("Failed to update group {} because it doesn't exist.",
                    request.getId(), e.getLocalizedMessage());
            responseObserver.onError(Status.NOT_FOUND
                .withDescription(e.getLocalizedMessage()).asException());
        } catch (DataAccessException | DuplicateNameException e) {
            logger.error("Failed to update group " + request.getId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getLocalizedMessage()).asException());
        }
    }

    /**
     * Update the cluster headroom template ID for a cluster.
     *
     * @param request The request object contains the Group ID and the template ID.
     * @param responseObserver response observer
     */
    @Override
    public void updateClusterHeadroomTemplate(final UpdateClusterHeadroomTemplateRequest request,
                                              final StreamObserver<UpdateClusterHeadroomTemplateResponse> responseObserver) {
        if (!request.hasGroupId() || !request.hasClusterHeadroomTemplateId()) {
            final String errMsg = "Group ID or cluster headroom template ID is missing.";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
            return;
        }

        logger.info("Updating cluster headroom template ID for group {}", request.getGroupId());

        // ensure the user has access to the template group
        checkUserAccessToGroup(request.getGroupId());

        try {
            Group updatedGroup = groupStore.updateClusterHeadroomTemplate(request.getGroupId(),
                    request.getClusterHeadroomTemplateId());
            final UpdateClusterHeadroomTemplateResponse response =
                    UpdateClusterHeadroomTemplateResponse.newBuilder()
                            .setUpdatedGroup(updatedGroup)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (GroupNotFoundException e) {
            logger.error("Failed to update group {} because it doesn't exist.",
                    request.getGroupId(), e.getLocalizedMessage());
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getLocalizedMessage()).asException());
        } catch (GroupNotClusterException e) {
            logger.error("Failed to update cluster headroom template ID for group {} because " +
                    "this group is not a cluster. ");
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getLocalizedMessage()).asException());
        } catch (DataAccessException e) {
            logger.error("Failed to update group " + request.getGroupId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getLocalizedMessage()).asException());
        }

        super.updateClusterHeadroomTemplate(request, responseObserver);
    }

    @Override
    public void deleteGroup(GroupID gid, StreamObserver<DeleteGroupResponse> responseObserver) {
        if (!gid.hasId()) {
            final String errMsg = "Invalid GroupID input for delete a group: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.ABORTED.withDescription(errMsg).asRuntimeException());
            return;
        }

        final long groupId = gid.getId();

        checkUserAccessToGroup(groupId);

        logger.info("Deleting a group: {}", groupId);
        final Optional<Group> group = tempGroupCache.delete(groupId);
        if (group.isPresent()) {
            // If the group was a temporary group, it shouldn't have been in any policies, so
            // we don't need to do any other work.
            responseObserver.onNext(DeleteGroupResponse.newBuilder().setDeleted(true).build());
            responseObserver.onCompleted();
        } else {
            try {
                groupStore.deleteUserGroup(gid.getId());
                responseObserver.onNext(DeleteGroupResponse.newBuilder().setDeleted(true).build());
                responseObserver.onCompleted();
            } catch (ImmutableGroupUpdateException e) {
                logger.error("Failed to update group {} due to error: {}",
                        gid.getId(), e.getLocalizedMessage());
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription(e.getLocalizedMessage()).asException());
            } catch (GroupNotFoundException e) {
                logger.error("Failed to update group {} because it doesn't exist.",
                        gid.getId(), e.getLocalizedMessage());
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription(e.getLocalizedMessage()).asException());
            } catch (DataAccessException e) {
                logger.error("Failed to delete group " + gid, e);
                responseObserver.onError(Status.INTERNAL.withDescription(e.getLocalizedMessage())
                        .asException());
            } catch (PolicyDeleteException e) {
                logger.error("Failed to delete attached policies for " + gid, e);
                responseObserver.onError(Status.INTERNAL.withDescription(e.getLocalizedMessage())
                        .asException());
            }
        }
    }

    @Override
    public void getMembers(final GroupDTO.GetMembersRequest request,
                           final StreamObserver<GroupDTO.GetMembersResponse> responseObserver) {
        if (!request.hasId()) {
            final String errMsg = "Group ID is missing for the getMembers request";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
            return;
        }

        final long groupId = request.getId();
        Optional<Group> optGroupInfo;
        try {
            // Check temp group cache first, because it's faster.
            optGroupInfo = tempGroupCache.get(groupId);
            if (!optGroupInfo.isPresent()) {
                optGroupInfo = groupStore.get(groupId);
            }
        } catch (DataAccessException e) {
            logger.error("Failed to get group: " + groupId, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getLocalizedMessage()).asRuntimeException());
            return;
        }

        if (optGroupInfo.isPresent()) {
            final Group group = optGroupInfo.get();
            final GetMembersResponse resp;
            List<Long> members = getNormalGroupMembers(group, request.getExpandNestedGroups());
            // verify the user has access to all of the group members before returning any of them.
            if (request.getEnforceUserScope() && userSessionContext.isUserScoped()) {
                if (group.getType() == Type.NESTED_GROUP) {
                    // Need to get the expanded members.
                    UserScopeUtils.checkAccess(userSessionContext, getNormalGroupMembers(group, true));
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
        } else if (!request.getExpectPresent()){
            logger.debug("Did not find group with id {} ; this may be expected behavior", groupId);
            responseObserver.onNext(GetMembersResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } else {
            final String errMsg = "Cannot find a group with id " + groupId;
            logger.error(errMsg);
            responseObserver.onError(Status.NOT_FOUND.withDescription(errMsg).asRuntimeException());
        }
    }

    /**
     * Given a {@link Group} object, return it's list of members.
     *
     * @param group the {@link Group} to check
     * @param expandNested If true, expand nested groups recursively.
     * @return a list of member oids.
     */
    private List<Long> getNormalGroupMembers(Group group, final boolean expandNested) {
        switch (group.getType()) {
            case CLUSTER:
                return group.getCluster().getMembers().getStaticMemberOidsList();
            case GROUP:
                return getNormalGroupMembers(group.getGroup());
            case TEMP_GROUP:
                return group.getTempGroup().getMembers().getStaticMemberOidsList();
            case NESTED_GROUP:
                return getNestedGroupMembers(group.getNestedGroup(), expandNested);
            default:
                throw new IllegalStateException("Invalid group returned.");
        }
    }

    /**
     * Get the members of a nested group.
     *
     * @param nestedGroupInfo Description of the nested group.
     * @param getLeafMembers If true, return the leaf members (i.e. the entities). If false,
     *                       return the immediate members (i.e. the groups).
     * @return The list of requested members.
     */
    @Nonnull
    private List<Long> getNestedGroupMembers(@Nonnull final NestedGroupInfo nestedGroupInfo,
                                                        final boolean getLeafMembers) {
        switch (nestedGroupInfo.getSelectionCriteriaCase()) {
            case STATIC_GROUP_MEMBERS:
                final Map<Long, Optional<Group>> groupsById =
                    groupStore.getGroups(nestedGroupInfo.getStaticGroupMembers().getStaticMemberOidsList());
                if (getLeafMembers) {
                    return expandNestedGroups(nestedGroupInfo, groupsById.values().stream()
                                    .filter(Optional::isPresent)
                                    .map(Optional::get));
                } else {
                    // Just return the members.
                    return groupsById.entrySet().stream()
                        .filter(entry -> entry.getValue().isPresent())
                        .map(Entry::getKey)
                        .collect(Collectors.toList());
                }
            case PROPERTY_FILTER_LIST:
                final Stream<Group> groupStream;
                if (nestedGroupInfo.getTypeCase() == TypeCase.CLUSTER) {
                    groupStream = groupStore.getAll().stream()
                        .filter(group -> group.getType() == Group.Type.CLUSTER)
                        .filter(cluster -> cluster.getCluster().getClusterType() == nestedGroupInfo.getCluster())
                        .filter(g ->
                                matchFilters(nestedGroupInfo.getPropertyFilterList().getPropertyFiltersList(), g));
                } else {
                    groupStream = Stream.empty();
                }
                if (getLeafMembers) {
                    // get all leaf members of these groups
                    return expandNestedGroups(nestedGroupInfo, groupStream);
                } else {
                    // return the member group ids
                    return groupStream
                            .map(Group::getId)
                            .collect(Collectors.toList());
                }
            default:
                throw new IllegalArgumentException("Invalid nested group selection criteria: " +
                    nestedGroupInfo.getSelectionCriteriaCase());
        }
    }

    /**
     * Helper method for expanding groups contained within nested groups, applying some validation to
     * support the Nested Group rules. (mainly that, currently, only clusters may be nested).
     * @param nestedGroupInfo The Nested Group being expanded
     * @param groupMembersStream A stream of groups directly contained by the nested groups.
     * @return a list of the leaf members of all of the groups in the stream.
     */
    private List<Long> expandNestedGroups(@Nonnull final NestedGroupInfo nestedGroupInfo,
                                          Stream<Group> groupMembersStream) {
        return groupMembersStream
            .filter(group -> {
                    if (group.getType() != Type.CLUSTER) {
                        // Right now we only support nested groups of clusters.
                        logger.warn("Nested group {} has non-cluster member group {} " +
                                        "(id: {}, type: {}). Ignoring it when expanding members.",
                                nestedGroupInfo.getName(), GroupProtoUtil.getGroupName(group),
                                group.getId(), group.getType());
                        return false;
                    } else {
                        return true;
                    }
                })
            // Right now this call is non-recursive, because we don't support nested groups of
            // nested groups. If we do support it in the future we should handle cycles and
            // potential stack overflows - probably with an iterative instead of recursive
            // approach.
            .flatMap(group -> getNormalGroupMembers(group, true).stream())
            .collect(Collectors.toList());
    }

    /**
     * Given a {@link GroupInfo} object, return the list of members in the group. This includes
     * resolution of dynamic groups.
     *
     * @param groupInfo
     * @return
     */
    private List<Long> getNormalGroupMembers(GroupInfo groupInfo) {
        if (groupInfo.hasStaticGroupMembers()) {
            return groupInfo.getStaticGroupMembers().getStaticMemberOidsList();
        } else {
            // resolve a dynamic group
            final List<SearchParameters> searchParameters
                    = groupInfo.getSearchParametersCollection().getSearchParametersList();

            // Convert any ClusterMemberFilters to static set member checks based
            // on current group membership info
            Search.SearchEntityOidsRequest.Builder searchRequestBuilder =
                    Search.SearchEntityOidsRequest.newBuilder();
            for (SearchParameters params : searchParameters) {
                searchRequestBuilder.addSearchParameters(resolveClusterFilters(params));
            }
            final Search.SearchEntityOidsRequest searchRequest = searchRequestBuilder.build();
            final Search.SearchEntityOidsResponse searchResponse = searchServiceRpc.searchEntityOids(searchRequest);
            return searchResponse.getEntitiesList();
        }
    }


    /**
     *  {@inheritDoc}
     * @param request
     * @param responseObserver
     */
    @Override
    public void getClusterForEntity(final GroupDTO.GetClusterForEntityRequest request,
                           final StreamObserver<GroupDTO.GetClusterForEntityResponse> responseObserver) {

        if (!request.hasEntityId()) {
            final String errMsg = "EntityID is missing for the getClusterForEntity request";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
            return;
        }

        GetClusterForEntityResponse.Builder response = GetClusterForEntityResponse.newBuilder();
        long clusterId = entityToClusterMapping.getClusterForEntity(request.getEntityId());
        if (clusterId > 0) {
            Optional<Group> group = groupStore.get(clusterId);
            if (group.isPresent() && group.get().hasCluster()){
                response.setCluster(group.get());
            }
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public StreamObserver<DiscoveredGroupsPoliciesSettings> storeDiscoveredGroupsPoliciesSettings(
            final StreamObserver<StoreDiscoveredGroupsPoliciesSettingsResponse> responseObserver) {

        return new StreamObserver<DiscoveredGroupsPoliciesSettings>() {

            final Map<String, Long> allGroupsMap = new HashMap<>();
            // Mapping from TargetId -> List of ClusterInfos
            // We assume that there are no duplicate clusterInfos. We could have used a set.
            // But hashing a complex object like clusterInfos will be expensive.
            final Map<Long, List<ClusterInfo>> targetIdToClusterInfosMap = new HashMap<>();

            @Override
            public void onNext(final DiscoveredGroupsPoliciesSettings record) {
                try {
                    if (!record.hasTargetId()) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Request must have a target ID.").asException());
                        return;
                    }
                    final long targetId = record.getTargetId();
                    final Map<String, Long> groupsMap = new HashMap<>();
                    // Update everything in a single transaction.
                    dslContext.transaction(configuration -> {
                        final DSLContext transactionContext = DSL.using(configuration);
                        groupsMap.putAll(groupStore.updateTargetGroups(transactionContext,
                                targetId,
                                record.getDiscoveredGroupList(),
                                record.getDiscoveredClusterList()));
                        policyStore.updateTargetPolicies(transactionContext,
                                targetId,
                                record.getDiscoveredPolicyInfosList(), groupsMap);
                        settingStore.updateTargetSettingPolicies(transactionContext,
                                targetId,
                                record.getDiscoveredSettingPoliciesList(), groupsMap);

                    });
                    // successfully updated the DB. Now add it to the records that needs to
                    // be added to the index.
                    allGroupsMap.putAll(groupsMap);
                    targetIdToClusterInfosMap.computeIfAbsent(targetId,
                            k -> new ArrayList<>()).addAll(record.getDiscoveredClusterList());

                } catch (DataAccessException e) {
                    logger.error("Failed to store discovered collections due to a database query error.", e);
                    responseObserver.onError(Status.INTERNAL
                            .withDescription(e.getLocalizedMessage()).asException());
                }
            }

            @Override
            public void onError(final Throwable t) {
                logger.error("Error uploading discovered non-entities for target {}", t);
            }

            @Override
            public void onCompleted() {
                // Update the entityId -> clusterId Index
                // The index clears its state during every update.
                // That's why we add to the index here for all successfully updated records.
                entityToClusterMapping.updateEntityClusterMapping(createClusterIdToClusterInfoMapping());
                responseObserver.onNext(StoreDiscoveredGroupsPoliciesSettingsResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }

            private Map<Long, ClusterInfo> createClusterIdToClusterInfoMapping() {
                Map<Long, ClusterInfo> clusterIdToClusterInfoMap = new HashMap<>();
                targetIdToClusterInfosMap.forEach((targetId, clusterInfos) -> {
                            clusterInfos.forEach(clusterInfo -> {
                                String clusterName =
                                        GroupProtoUtil.discoveredIdFromName(clusterInfo, targetId);
                                long clusterId = allGroupsMap.getOrDefault(clusterName, 0L);
                                if (clusterId == 0L) {
                                    // Skip this cluster as we couldn't find the clusterId.
                                    logger.warn("Can't get clusterId for cluster {}, targetId:{}",
                                            clusterInfo, targetId);
                                    return;
                                }
                                clusterIdToClusterInfoMap.put(clusterId, clusterInfo);
                            });
                        });
                return clusterIdToClusterInfoMap;
            }
        };
    }

    @Override
    public void getTags(GetTagsRequest request, StreamObserver<GetTagsResponse> responseObserver) {
        try {
            responseObserver.onNext(
                GetTagsResponse.newBuilder()
                    .setTags(groupStore.getTags())
                    .build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            logger.error("Data access exception while fetching group tags", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * Given a group, transform any dynamic clusterMembershipFilters it contains into StringFilters
     * that express the cluster membership filter statically.
     *
     * @param group the group to resolve cluster filters for
     * @return A new group containing the changes if there were any cluster membership filters to
     * transform. If not, the original group is returned.
     */
    private Group resolveClusterFilters(Group group) throws DataAccessException {
        final GroupInfo groupInfo = group.getGroup();
        if (groupInfo.hasStaticGroupMembers()) {
            return group; // not a dynamic group -- return original group
        }

        final List<SearchParameters> searchParameters
                = groupInfo.getSearchParametersCollection().getSearchParametersList();
        // check if there are any cluster membership filters in the search params
        if (!searchParameters.stream()
                .anyMatch(params -> params.getSearchFilterList().stream()
                        .anyMatch(SearchFilter::hasClusterMembershipFilter))) {
            return group; // no cluster filters inside -- return original group
        }
        // we have cluster membership filters to resolve -- rebuild the group with the resolved
        // filters
        logger.debug("Resolving cluster filters for {}", group.getGroup().getName());
        Group.Builder groupBuilder = Group.newBuilder(group);
        SearchParametersCollection.Builder searchParamsBuilder = groupBuilder.getGroupBuilder()
                .getSearchParametersCollectionBuilder()
                .clearSearchParameters();
        for (SearchParameters params : searchParameters) {
            searchParamsBuilder.addSearchParameters(resolveClusterFilters(params));
        }
        return groupBuilder.build();
    }

    /**
     * Provided an input SearchParameters object, resolve any cluster membership filters contained
     * inside and return a new SearchParameters object with the resolved filters. If there are no
     * cluster membership filters inside, return the original object.
     *
     * @param searchParameters A SearchParameters object that may contain cluster filters.
     * @return A SearchParameters object that has had any cluster filters in it resolved. Will be the
     * original object if there were no group filters inside.
     */
    SearchParameters resolveClusterFilters(SearchParameters searchParameters)
            throws DataAccessException {
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
            searchParamBuilder.addSearchFilter(convertClusterMemberFilter(sf));
        }

        return searchParamBuilder.build();
    }

    /**
     * Convert a cluster member filter to a static property filter. If the input filter does not
     * contain a cluster member filter, the input filter will be returned, unchanged.
     *
     * @param inputFilter The ClusterMemberFilter to convert.
     * @return A new SearchFilter with any ClusterMemberFilters converted to property filters. If
     * there weren't any ClusterMemberFilters to convert, the original filter is returned.
     */
    private SearchFilter convertClusterMemberFilter(SearchFilter inputFilter)
            throws DataAccessException {
        if (! inputFilter.hasClusterMembershipFilter()) {
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
            groupStore.getAll().stream()
                .filter(group -> matchFilter(clusterSpecifierFilter, group))
                .filter(Group::hasCluster) // only clusters plz
                .map(Group::getCluster)
                .flatMap(clusterInfo -> clusterInfo.getMembers().getStaticMemberOidsList().stream())
                .collect(Collectors.toSet());
        // build the replacement filter - a regex against /^oid1$|^oid2$|.../
        StringJoiner sj = new StringJoiner("$|^","^","$");
        matchingClusterMembers.forEach(oid -> sj.add(oid.toString()));

        SearchFilter searchFilter = SearchFilter.newBuilder()
                .setPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName("oid")
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex(sj.toString())))
                .build();
        return searchFilter;
    }


    private boolean matchFilters(@Nonnull List<PropertyFilter> filters, @Nonnull Group group) {
        return filters.stream().allMatch(filter -> matchFilter(filter, group));
    }

    private boolean matchFilter(@Nonnull PropertyFilter filter, @Nonnull Group group) {
        if (filter.getPropertyName().equals(SearchableProperties.DISPLAY_NAME) && filter.hasStringFilter()) {
            // filter according to property name
            return GroupProtoUtil.nameFilterMatches(
                       GroupProtoUtil.getGroupDisplayName(group), filter.getStringFilter());
        } else if (filter.getPropertyName().equals(StringConstants.TAGS_ATTR) && filter.hasMapFilter()) {
            // filter according to tags

            // get tags from group object
            final Tags tags;
            switch (group.getInfoCase()) {
                case GROUP:
                    tags = group.getGroup().getTags();
                    break;
                case CLUSTER:
                    tags = group.getCluster().getTags();
                    break;
                default:
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
}
