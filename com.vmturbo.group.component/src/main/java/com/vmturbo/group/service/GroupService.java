package com.vmturbo.group.service;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javaslang.collection.Stream;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.group.persistent.DatabaseException;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.GroupStore.DuplicateGroupException;
import com.vmturbo.group.persistent.GroupStore.GroupNotFoundException;
import com.vmturbo.group.persistent.GroupStore.ImmutableUpdateException;
import com.vmturbo.group.persistent.PolicyStore;
import com.vmturbo.group.persistent.PolicyStore.PolicyDeleteException;

public class GroupService extends GroupServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final GroupStore groupStore;

    private final PolicyStore policyStore;

    private final SearchServiceBlockingStub searchServiceRpc;

    public GroupService(final GroupStore groupStore,
                        final PolicyStore policyStore,
                        final SearchServiceBlockingStub searchServiceRpc) {
        this.groupStore = Objects.requireNonNull(groupStore);
        this.policyStore = Objects.requireNonNull(policyStore);
        this.searchServiceRpc = Objects.requireNonNull(searchServiceRpc);
    }

    @Override
    public void createGroup(GroupInfo groupInfo, StreamObserver<CreateGroupResponse> responseObserver) {
        logger.info("Creating a group: {}", groupInfo);

        try {
            final Group createdGroupOpt = groupStore.newUserGroup(groupInfo);
            responseObserver.onNext(CreateGroupResponse.newBuilder()
                    .setGroup(createdGroupOpt)
                    .build());
            responseObserver.onCompleted();
        } catch (DatabaseException | DuplicateGroupException e) {
            logger.error("Failed to create group: " + groupInfo, e);
            responseObserver.onError(Status.ABORTED.withDescription(e.getLocalizedMessage())
                    .asRuntimeException());
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

        logger.info("Getting a group: {}", gid);

        try {
            final Optional<Group> group = groupStore.get(gid.getId());
            GetGroupResponse.Builder builder = GetGroupResponse.newBuilder();
            group.ifPresent(builder::setGroup);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (DatabaseException e) {
            logger.error("Failed to retrieve group: " + gid, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getLocalizedMessage()).asException());
        }
    }

    @Override
    public void getGroups(GetGroupsRequest request, StreamObserver<Group> responseObserver) {
        logger.info("Get all user groups");

        final Set<Long> requestedIds = new HashSet<>(request.getIdList());
        try {
            groupStore.getAll().stream()
                    .filter(group -> requestedIds.isEmpty() || requestedIds.contains(group.getId()))
                    .filter(group -> !request.hasOriginFilter() ||
                            group.getOrigin().equals(request.getOriginFilter()))
                    .filter(group -> !request.hasTypeFilter() ||
                            group.getType().equals(request.getTypeFilter()))
                    .filter(group -> !request.hasNameFilter() ||
                            GroupProtoUtil.nameFilterMatches(GroupProtoUtil.getGroupName(group),
                                    request.getNameFilter()))
                    .filter(group -> !request.hasClusterFilter() ||
                            GroupProtoUtil.clusterFilterMatcher(group, request.getClusterFilter()))
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DatabaseException e) {
            logger.error("Failed to query group store for group definitions.", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getLocalizedMessage())
                    .asException());
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

        try {
            Group newGroup = groupStore.updateUserGroup(request.getId(), request.getNewInfo());
            final UpdateGroupResponse res = UpdateGroupResponse.newBuilder()
                    .setUpdatedGroup(newGroup)
                    .build();
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        } catch (ImmutableUpdateException e) {
            logger.error("Failed to update group {} due to error: {}",
                    request.getId(), e.getLocalizedMessage());
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getLocalizedMessage()).asException());
        } catch (GroupNotFoundException e) {
            logger.error("Failed to update group {} because it doesn't exist.",
                    request.getId(), e.getLocalizedMessage());
            responseObserver.onError(Status.NOT_FOUND
                .withDescription(e.getLocalizedMessage()).asException());
        } catch (DatabaseException | DuplicateGroupException e) {
            logger.error("Failed to update group " + request.getId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getLocalizedMessage()).asException());
        }
    }

    @Override
    public void deleteGroup(GroupID gid, StreamObserver<DeleteGroupResponse> responseObserver) {
        if (!gid.hasId()) {
            final String errMsg = "Invalid GroupID input for delete a group: No group ID specified";
            logger.error(errMsg);
            responseObserver.onError(Status.ABORTED.withDescription(errMsg).asRuntimeException());
            return;
        }

        logger.info("Deleting a group: {}", gid);
        try {
            // TODO: Policy and group delete should happen in a single transaction, but because we are
            // migrating from ArangoDB to MySQL, it isn't worth time to figure out transactional
            // operations in ArangoDB.
            List<Long> policies = getPolicyByGroup(gid.getId());
            logger.info("Find group " + gid.getId() + " attached policies: " + policies);

            policyStore.deletePolicies(policies);
            logger.info("Deleted policy " + policies + " successfully");

            groupStore.deleteUserGroup(gid.getId());
            responseObserver.onNext(DeleteGroupResponse.newBuilder().setDeleted(true).build());
            responseObserver.onCompleted();
        } catch (ImmutableUpdateException e) {
            logger.error("Failed to update group {} due to error: {}",
                    gid.getId(), e.getLocalizedMessage());
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getLocalizedMessage()).asException());
        } catch (GroupNotFoundException e) {
            logger.error("Failed to update group {} because it doesn't exist.",
                    gid.getId(), e.getLocalizedMessage());
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getLocalizedMessage()).asException());
        } catch (DatabaseException e) {
            logger.error("Failed to delete group " + gid, e);
            responseObserver.onError(Status.ABORTED.withDescription(e.getLocalizedMessage())
                    .asException());
        }
        catch (PolicyDeleteException e) {
            logger.error("Failed to delete attached policies for " + gid, e);
            responseObserver.onError(Status.ABORTED.withDescription(e.getLocalizedMessage())
                .asException());
        }
    }

    private GetMembersResponse getStaticMembers(final long groupId, StaticGroupMembers staticGroupMembers) {
        final List<Long> memberIds = staticGroupMembers.getStaticMemberOidsList();
        logger.debug("Static group ({}) and its first 10 members {}",
                groupId,
                Stream.ofAll(memberIds).take(10).toJavaList());
        return GroupDTO.GetMembersResponse.newBuilder()
                .addAllMemberId(memberIds)
                .build();
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
        final Optional<Group> optGroupInfo;
        try {
            optGroupInfo = groupStore.get(groupId);
        } catch (DatabaseException e) {
            logger.error("Failed to get group: " + groupId, e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getLocalizedMessage()).asRuntimeException());
            return;
        }

        if (optGroupInfo.isPresent()) {
            final Group group = optGroupInfo.get();
            final GetMembersResponse resp;
            switch (group.getType()) {
                case CLUSTER:
                    resp = getStaticMembers(group.getId(), group.getCluster().getMembers());
                    break;
                case GROUP:
                    final GroupInfo groupInfo = group.getGroup();
                    if (groupInfo.hasStaticGroupMembers()) {
                        resp = getStaticMembers(group.getId(), groupInfo.getStaticGroupMembers());
                    } else {
                        try {
                            final List<SearchParameters> searchParameters = groupInfo.getSearchParametersCollection().getSearchParametersList();
                            final Search.SearchRequest searchRequest = Search.SearchRequest.newBuilder()
                                    .addAllSearchParameters(searchParameters).build();
                            final Search.SearchResponse searchResponse = searchServiceRpc.searchEntityOids(searchRequest);
                            final List<Long> searchResults = searchResponse.getEntitiesList();
                            logger.debug("Dynamic group ({}) and its first 10 members {}",
                                    groupId,
                                    Stream.ofAll(searchResults).take(10).toJavaList());

                            resp = GroupDTO.GetMembersResponse.newBuilder()
                                    .addAllMemberId(searchResults)
                                    .build();
                        } catch (RuntimeException e) {
                            final String errMsg = "Exception encountered while resolving group " + groupId;
                            logger.error(errMsg, e);
                            responseObserver.onError(Status.ABORTED.withCause(e)
                                    .withDescription(e.getMessage()).asRuntimeException());
                            return;
                        }
                    }
                    break;
                default:
                    throw new IllegalStateException("Invalid group returned.");
            }
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } else {
            final String errMsg = "Cannot find a group with id " + groupId;
            logger.error(errMsg);
            responseObserver.onError(Status.NOT_FOUND.withDescription(errMsg).asRuntimeException());
        }
    }

    /**
     * In order to get policies corresponding to group, in ArangoDb, we have to get all policies
     * and filter them by group Id. After migrating to relational database, it should be more efficient.
     *
     * @param groupId The Id of a group.
     * @return A list of related polices Id.
     */
    private List<Long> getPolicyByGroup(long groupId) {
        Collection<InputPolicy> inputPolicies = policyStore.getAll();
        return inputPolicies.stream()
            .filter(policy -> matchWithGroupId(policy, groupId))
            .map(InputPolicy::getId)
            .collect(Collectors.toList());
    }

    /**
     * Check policy consumer or provider group whether contains groupId or not.
     *
     * @param policy Policy object need to check.
     * @param groupId The Id of a group need to find.
     * @return A boolean to represent whether find matched groupId in policy consumer or provider group.
     */
    private boolean matchWithGroupId(@Nonnull InputPolicy policy, long groupId) {
        switch (policy.getPolicyDetailCase()) {
            case AT_MOST_N:
                return (policy.getAtMostN().getConsumerGroup() == groupId) ||
                    (policy.getAtMostN().getProviderGroup() == groupId);

            case AT_MOST_NBOUND:
                return (policy.getAtMostNbound().getConsumerGroup() == groupId) ||
                    (policy.getAtMostNbound().getProviderGroup() == groupId);

            case BIND_TO_COMPLEMENTARY_GROUP:
                return (policy.getBindToComplementaryGroup().getConsumerGroup() == groupId) ||
                    (policy.getBindToComplementaryGroup().getProviderGroup() == groupId);

            case BIND_TO_GROUP:
                return (policy.getBindToGroup().getConsumerGroup() == groupId) ||
                    (policy.getBindToGroup().getProviderGroup() == groupId);

            case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                return (policy.getBindToGroupAndGeoRedundancy().getConsumerGroup() == groupId) ||
                    (policy.getBindToGroupAndGeoRedundancy().getProviderGroup() == groupId);

            case BIND_TO_GROUP_AND_LICENSE:
                return (policy.getBindToGroupAndLicense().getConsumerGroup() == groupId) ||
                    (policy.getBindToGroupAndLicense().getProviderGroup() == groupId);

            case MERGE:
                return policy.getMerge().getMergeGroupsList().stream()
                    .anyMatch(id -> id.equals(groupId));

            case MUST_RUN_TOGETHER:
                return (policy.getMustRunTogether().getConsumerGroup() == groupId) ||
                    (policy.getMustRunTogether().getProviderGroup() == groupId);

            default:
                throw new RuntimeException("Failed to parse policy case: " + policy.getPolicyDetailCase());
        }
    }
}
