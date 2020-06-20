package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import static com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import static com.vmturbo.common.protobuf.group.GroupServiceGrpc.newBlockingStub;
import static com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import static com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;

import java.util.Iterator;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionServiceGrpc;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionServiceGrpc.TopologyDataDefinitionServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;

/**
 * A class responsible for executing gRpc requests to Group and Repository services.
 */
public class RequestExecutor {

    private final TopologyDataDefinitionServiceBlockingStub topologyDataDefService;
    private final RepositoryServiceBlockingStub repositoryService;
    private final SearchServiceBlockingStub searchService;
    private final GroupServiceBlockingStub groupService;

    /**
     * Constructor.
     *
     * @param connection - provider of gRpc channels.
     */
    public RequestExecutor(@Nonnull Connection connection) {
        this.topologyDataDefService = TopologyDataDefinitionServiceGrpc.newBlockingStub(connection.getGroupChannel());
        this.repositoryService = RepositoryServiceGrpc.newBlockingStub(connection.getRepositoryChannel());
        this.groupService = newBlockingStub(connection.getGroupChannel());
        this.searchService = SearchServiceGrpc.newBlockingStub(connection.getRepositoryChannel());
    }

    @Nonnull
    SearchEntitiesResponse searchEntities(@Nonnull SearchEntitiesRequest request) {
        return searchService.searchEntities(request);
    }

    @Nonnull
    Iterator<GetTopologyDataDefinitionResponse> getAllTopologyDataDefinitions(
            @Nonnull GetTopologyDataDefinitionsRequest request) {
        return topologyDataDefService.getAllTopologyDataDefinitions(request);
    }

    @Nonnull
    Iterator<PartialEntityBatch> retrieveTopologyEntities(
            @Nonnull RetrieveTopologyEntitiesRequest request) {
        return repositoryService.retrieveTopologyEntities(request);
    }

    @Nonnull
    Iterator<GetMembersResponse> getGroupMembers(@Nonnull GetMembersRequest request) {
        return groupService.getMembers(request);
    }

    @Nonnull
    @VisibleForTesting
    TopologyDataDefinitionServiceBlockingStub getTopologyDataDefService() {
        return topologyDataDefService;
    }

    @Nonnull
    @VisibleForTesting
    RepositoryServiceBlockingStub getRepositoryService() {
        return repositoryService;
    }

    @Nonnull
    @VisibleForTesting
    SearchServiceBlockingStub getSearchService() {
        return searchService;
    }

    @Nonnull
    @VisibleForTesting
    GroupServiceBlockingStub getGroupService() {
        return groupService;
    }

}
