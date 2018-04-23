package com.vmturbo.group.service;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.DeleteGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.NameFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.group.persistent.DatabaseException;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.GroupStore.DuplicateGroupException;
import com.vmturbo.group.persistent.GroupStore.GroupNotClusterException;
import com.vmturbo.group.persistent.GroupStore.GroupNotFoundException;
import com.vmturbo.group.persistent.GroupStore.ImmutableUpdateException;
import com.vmturbo.group.persistent.PolicyStore;
import com.vmturbo.group.persistent.PolicyStore.PolicyDeleteException;
import com.vmturbo.group.persistent.TemporaryGroupCache;
import com.vmturbo.group.persistent.TemporaryGroupCache.InvalidTempGroupException;

public class GroupService extends GroupServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final GroupStore groupStore;

    private final TemporaryGroupCache tempGroupCache;

    private final PolicyStore policyStore;

    private final SearchServiceBlockingStub searchServiceRpc;

    public GroupService(final GroupStore groupStore,
                        final PolicyStore policyStore,
                        final TemporaryGroupCache tempGroupCache,
                        final SearchServiceBlockingStub searchServiceRpc) {
        this.groupStore = Objects.requireNonNull(groupStore);
        this.policyStore = Objects.requireNonNull(policyStore);
        this.tempGroupCache = Objects.requireNonNull(tempGroupCache);
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
            logger.error("Failed to create group: {}", groupInfo, e);
            responseObserver.onError(Status.ABORTED.withDescription(e.getLocalizedMessage())
                    .asRuntimeException());
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

        try {
            final Group group = tempGroupCache.create(request.getGroupInfo());
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
            // Try the temporary group cache first because it's much faster.
            Optional<Group> group = tempGroupCache.get(gid.getId());
            if (!group.isPresent()) {
                group = groupStore.get(gid.getId());
            }
            GetGroupResponse.Builder builder = GetGroupResponse.newBuilder();
            group.ifPresent(builder::setGroup);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (DatabaseException e) {
            logger.error("Failed to retrieve group: {}", gid, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getLocalizedMessage()).asException());
        }
    }

    @Override
    public void getGroups(GetGroupsRequest request, StreamObserver<Group> responseObserver) {
        logger.trace("Get all user groups");

        boolean resolveClusterFilters = request.hasResolveClusterSearchFilters() &&
            request.getResolveClusterSearchFilters();

        final Set<Long> requestedIds = new HashSet<>(request.getIdList());
        try {
            final Stream<Group> groupStream =
                request.hasTypeFilter() && request.getTypeFilter() == Type.TEMP_GROUP ?
                    tempGroupCache.getAll().stream() : groupStore.getAll().stream();
            groupStream.filter(group -> requestedIds.isEmpty() || requestedIds.contains(group.getId()))
                    .filter(group -> !request.hasOriginFilter() ||
                            group.getOrigin().equals(request.getOriginFilter()))
                    .filter(group -> !request.hasTypeFilter() ||
                            group.getType().equals(request.getTypeFilter()))
                    .filter(group -> !request.hasNameFilter() ||
                            GroupProtoUtil.nameFilterMatches(GroupProtoUtil.getGroupName(group),
                                    request.getNameFilter()))
                    .filter(group -> !request.hasClusterFilter() ||
                            GroupProtoUtil.clusterFilterMatcher(group, request.getClusterFilter()))
                    .map(group -> {
                        try {
                            return resolveClusterFilters ? resolveClusterFilters(group) : group;
                        } catch (DatabaseException de) {
                            throw new RuntimeException(de);
                        }
                    })
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DatabaseException|RuntimeException e) {
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
        } catch (DatabaseException e) {
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

        logger.info("Deleting a group: {}", groupId);
        final Optional<Group> group = tempGroupCache.delete(groupId);
        if (group.isPresent()) {
            // If the group was a temporary group, it shouldn't have been in any policies, so
            // we don't need to do any other work.
            responseObserver.onNext(DeleteGroupResponse.newBuilder().setDeleted(true).build());
            responseObserver.onCompleted();
        } else {
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
    }

    private GetMembersResponse getStaticMembers(final long groupId, StaticGroupMembers staticGroupMembers) {
        final List<Long> memberIds = staticGroupMembers.getStaticMemberOidsList();
        logger.debug("Static group ({}) and its first 10 members {}",
                groupId,
                Stream.of(memberIds).limit(10).collect(Collectors.toList()));
        return GetMembersResponse.newBuilder()
            .setMembers(Members.newBuilder().addAllIds(memberIds))
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
        Optional<Group> optGroupInfo;
        try {
            // Check temp group cache first, because it's faster.
            optGroupInfo = tempGroupCache.get(groupId);
            if (!optGroupInfo.isPresent()) {
                optGroupInfo = groupStore.get(groupId);
            }
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
                            final List<SearchParameters> searchParameters
                                = groupInfo.getSearchParametersCollection().getSearchParametersList();

                            // Convert any ClusterMemberFilters to static set member checks based
                            // on current group membership info
                            Search.SearchRequest.Builder searchRequestBuilder = Search.SearchRequest.newBuilder();
                            try {
                                for (SearchParameters params : searchParameters) {
                                    searchRequestBuilder.addSearchParameters(resolveClusterFilters(params));
                                }
                            } catch (DatabaseException de) {
                                logger.error("Failed to resolve cluster filters: ", de);
                                responseObserver.onError(Status.INTERNAL
                                    .withDescription(de.getLocalizedMessage()).asRuntimeException());
                                return;

                            }
                            final Search.SearchRequest searchRequest = searchRequestBuilder.build();
                            final Search.SearchResponse searchResponse = searchServiceRpc.searchEntityOids(searchRequest);
                            final List<Long> searchResults = searchResponse.getEntitiesList();
                            logger.debug("Dynamic group ({}) and its first 10 members {}",
                                groupId,
                                Stream.of(searchResults).limit(10).collect(Collectors.toList()));

                            resp = GetMembersResponse.newBuilder()
                                .setMembers(Members.newBuilder().addAllIds(searchResults))
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
                case TEMP_GROUP:
                    resp = getStaticMembers(group.getId(), group.getTempGroup().getMembers());
                    break;
                default:
                    throw new IllegalStateException("Invalid group returned.");
            }
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
     * Given a group, transform any dynamic clusterMembershipFilters it contains into StringFilters
     * that express the cluster membership filter statically.
     *
     * @param group the group to resolve cluster filters for
     * @return A new group containing the changes if there were any cluster membership filters to
     * transform. If not, the original group is returned.
     */
    private Group resolveClusterFilters(Group group) throws DatabaseException {
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
            throws DatabaseException {
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
            throws DatabaseException {
        if (! inputFilter.hasClusterMembershipFilter()) {
            return inputFilter;
        }
        // this has a cluster membership filter -- resolve plz
        // We are only supporting cluster lookups in this filter. Theoretically we could call
        // back to getMembers() to get generic group resolution, which would be more flexible,
        // but has the huge caveat of allowing circular references to happen. We'll stick to
        // just handling clusters here and open it up later, when/if needed.
        StringFilter clusterSpecifierFilter = inputFilter.getClusterMembershipFilter().getClusterSpecifier().getStringFilter();
        NameFilter nf = NameFilter.newBuilder()
                .setNameRegex(clusterSpecifierFilter.getStringPropertyRegex())
                .setNegateMatch(!clusterSpecifierFilter.getMatch())
                .build();
        logger.debug("Resolving ClusterMemberFilter {}", clusterSpecifierFilter.getStringPropertyRegex());
        Set<Long> matchingClusterMembers = groupStore.getAll().stream()
                .filter(group -> GroupProtoUtil.nameFilterMatches(GroupProtoUtil.getGroupName(group), nf))
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
                return (policy.getMustRunTogether().getGroup() == groupId);

            case MUST_NOT_RUN_TOGETHER:
                return (policy.getMustNotRunTogether().getGroup() == groupId);

            default:
                throw new RuntimeException("Failed to parse policy case: " + policy.getPolicyDetailCase());
        }
    }
}
