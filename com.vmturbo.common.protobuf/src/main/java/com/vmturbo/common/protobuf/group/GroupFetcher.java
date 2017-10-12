package com.vmturbo.common.protobuf.group;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClustersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;

/**
 * Fetch groups and clusters for use with policies, which store IDs.
 */
public class GroupFetcher {

    private final GroupServiceStub groupRpcService;
    private final ClusterServiceStub clusterRpcService;

    private final Duration timeout;

    public GroupFetcher(@Nonnull final Channel groupChannel,
                        @Nonnull final Duration timeoutDuration) {
        Objects.requireNonNull(groupChannel);

        groupRpcService = GroupServiceGrpc.newStub(groupChannel);
        clusterRpcService = ClusterServiceGrpc.newStub(groupChannel);
        timeout = timeoutDuration;
    }

    /**
     *
     * @param sought set of {@link PolicyGroupingID}s to search for
     * @return map of {@link PolicyGroupingID} to {@link PolicyGrouping}
     * @throws GroupFetchingException if the group fetch operation fails for some reason
     */
    public Map<PolicyGroupingID, PolicyGrouping> getGroupings(Set<PolicyGroupingID> sought)
            throws GroupFetchingException {

        Map<PolicyGroupingID, PolicyGrouping> result = new HashMap<>();
        Set<Long> clusterIds = sought.stream().filter(PolicyGroupingID::hasClusterId)
                .map(PolicyGroupingID::getClusterId).collect(Collectors.toSet());
        Set<Long> groupIds = sought.stream().filter(PolicyGroupingID::hasGroupId)
                .map(PolicyGroupingID::getGroupId).collect(Collectors.toSet());
        if(!groupIds.isEmpty()){
            try {
                GroupsFetchOperation groupsFetchOperation = new GroupsFetchOperation(
                        groupIds, groupRpcService, timeout);
                Set<Group> fetched = groupsFetchOperation.fetch();
                fetched.forEach(f -> result
                        .put(PolicyGroupingID.newBuilder().setGroupId(f.getId()).build(),
                                PolicyGrouping.newBuilder().setGroup(f).build()));
            } catch (TimeoutException | ExecutionException | InterruptedException e) {
                throw new GroupFetchingException("Error fetching groupings: " + e.getMessage());
            }

        }
        if(!clusterIds.isEmpty()){
            try {
                ClustersFetchOperation clustersFetchOperation = new ClustersFetchOperation(
                        clusterIds, clusterRpcService, timeout);
                Set<Cluster> fetched = clustersFetchOperation.fetch();
                fetched.forEach(f -> result
                        .put(PolicyGroupingID.newBuilder().setClusterId(f.getId()).build(),
                                PolicyGrouping.newBuilder().setCluster(f).build()));
            } catch (TimeoutException | ExecutionException | InterruptedException e) {
                throw new GroupFetchingException("Error fetching groupings: " + e.getMessage());
            }
        }
        return result;
    }

    /**
     * Fetch operation querying a group RPC service.
     */
    private static class GroupsFetchOperation extends
            CollectionsFetchOperation<GetGroupsRequest, Group, GroupServiceStub> {

        GroupsFetchOperation(@Nonnull Set<Long> groupIds, @Nonnull GroupServiceStub svc,
                             @Nonnull Duration timeout) {
            super(groupIds, svc, timeout);
            response = Group.newBuilder().build();
        }

        @Override
        void makeRpcCall(){
            rpcService.getGroups(request, this);
        }

        @Override
        GetGroupsRequest composeRequest() {
            return GetGroupsRequest.newBuilder().addAllId(ids).build();
        }
    }

    /**
     * Fetch operation querying a cluster RPC service.
     */
    private static class ClustersFetchOperation extends
            CollectionsFetchOperation<GetClustersRequest, Cluster, ClusterServiceStub> {

        ClustersFetchOperation(@Nonnull Set<Long> clusterIds, @Nonnull ClusterServiceStub svc,
                             @Nonnull Duration timeout) {
            super(clusterIds, svc, timeout);
            response = Cluster.newBuilder().build();
        }

        @Override
        void makeRpcCall(){
            rpcService.getClusters(request, this);
        }

        @Override
        GetClustersRequest composeRequest() {
            return GetClustersRequest.newBuilder().addAllId(ids).build();
        }
    }

    /**
     * Fetch operation querying a collection RPC service.
     * @param <CollectionsRequestType> The type of request to be sent to the RPC service
     * @param <CollectionsResponseType> The type of response returned in a stream from the RPC service
     * @param <CollectionsServiceType> The type of RPC service
     */
    private static abstract class CollectionsFetchOperation<CollectionsRequestType,
            CollectionsResponseType, CollectionsServiceType> implements
            StreamObserver<CollectionsResponseType> {

        protected final Logger logger = LogManager.getLogger();

        protected Set<Long> ids;
        protected CollectionsResponseType response;
        protected CompletableFuture<CollectionsResponseType> responseFuture;
        protected Set<CollectionsResponseType> responseSet;
        protected CollectionsRequestType request;
        protected CollectionsServiceType rpcService;
        protected Duration timeout;

        CollectionsFetchOperation(@Nonnull Set<Long> collectionIds,
                                  @Nonnull CollectionsServiceType service,
                                  @Nonnull Duration timeoutDuration){
            rpcService = Objects.requireNonNull(service);
            timeout = Objects.requireNonNull(timeoutDuration);
            ids = Objects.requireNonNull(collectionIds);
            responseFuture = new CompletableFuture<>();
            responseSet = new HashSet<>();

        }

        abstract CollectionsRequestType composeRequest();

        abstract void makeRpcCall();

        Set<CollectionsResponseType> fetch() throws InterruptedException, ExecutionException,
                TimeoutException {
            request = composeRequest();
            makeRpcCall();
            CollectionsResponseType c = responseFuture.get(timeout.getSeconds(), TimeUnit.SECONDS);
            return responseSet;
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("Error fetching collection: " + request, throwable);
            responseFuture.completeExceptionally(throwable);
        }

        @Override
        public void onNext(final CollectionsResponseType result) {
            responseSet.add(result);
            response = result;
        }

        @Override
        public void onCompleted(){
            responseFuture.complete(response);
        }
    }
}