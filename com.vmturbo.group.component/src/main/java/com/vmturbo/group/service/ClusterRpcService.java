package com.vmturbo.group.service;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.GroupDTOUtil;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClustersRequest;
import com.vmturbo.group.persistent.ClusterStore;
import com.vmturbo.group.persistent.DatabaseException;

/**
 * {@inheritDoc}
 */
public class ClusterRpcService extends ClusterServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ClusterStore clusterStore;

    public ClusterRpcService(@Nonnull final ClusterStore clusterStore) {
        this.clusterStore = Objects.requireNonNull(clusterStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getCluster(GetClusterRequest request,
                           StreamObserver<GetClusterResponse> responseObserver) {
        if (!request.hasClusterId()) {
            logger.error("Missing ID in request.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Cluster request must have an ID").asException());
            return;
        }

        try {
            final Optional<Cluster> cluster = clusterStore.get(request.getClusterId());
            final GetClusterResponse.Builder respBuilder = GetClusterResponse.newBuilder();
            cluster.ifPresent(respBuilder::setCluster);
            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
        } catch (DatabaseException e) {
            logger.error("Failed to query cluster store.", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getLocalizedMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getClusters(GetClustersRequest request,
                            StreamObserver<Cluster> responseObserver) {
        final Set<Long> requestedIds = new HashSet<>(request.getIdList());
        try {
            // TODO (roman, Aug 2, 2017): The ID filters should be part of the cluster store.
            // The name filter could (maybe) be part of the cluster store too.
            // It's not critical while the number of clusters is low, since each cluster is
            // a pretty small object.
            clusterStore.getAll().stream()
                    .filter(cluster -> requestedIds.isEmpty() || requestedIds.contains(cluster.getId()))
                    .filter(cluster -> !request.hasTypeFilter() ||
                            cluster.getInfo().getClusterType().equals(request.getTypeFilter()))
                    .filter(cluster -> !request.hasNameFilter() || GroupDTOUtil.nameFilterMatches(cluster.getInfo().getName(), request.getNameFilter()))
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DatabaseException e) {
            logger.error("Failed to query cluster store for cluster definitions.", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getLocalizedMessage())
                    .asException());
        }
    }
}
