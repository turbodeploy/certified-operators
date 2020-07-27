package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import static com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import static com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import static com.vmturbo.common.protobuf.group.GroupServiceGrpc.newBlockingStub;
import static com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import static com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;

import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
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

    private final Logger logger = LogManager.getLogger();

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
        this(TopologyDataDefinitionServiceGrpc.newBlockingStub(connection.getGroupChannel()),
                RepositoryServiceGrpc.newBlockingStub(connection.getRepositoryChannel()),
                newBlockingStub(connection.getGroupChannel()),
                SearchServiceGrpc.newBlockingStub(connection.getRepositoryChannel()));
    }

    /**
     * Constructor that receives the blocking stubs. Used for testing.
     *
     * @param topologyDataDefService - a topology data definition service.
     * @param repositoryService - a repository service.
     * @param groupService - a group service.
     * @param searchService - a search service.
     */
    @VisibleForTesting
    protected RequestExecutor(@Nonnull final TopologyDataDefinitionServiceBlockingStub topologyDataDefService,
                           @Nonnull final RepositoryServiceBlockingStub repositoryService,
                           @Nonnull final GroupServiceBlockingStub groupService,
                           @Nonnull final SearchServiceBlockingStub searchService) {
        this.topologyDataDefService = topologyDataDefService;
        this.repositoryService = repositoryService;
        this.groupService = groupService;
        this.searchService = searchService;
    }

    @Nonnull
    SearchEntitiesResponse searchEntities(@Nonnull SearchEntitiesRequest request) {
        SearchEntitiesResponse.Builder aggregatedResponseBuilder = SearchEntitiesResponse.newBuilder();
        boolean paginationDone = false;
        String nextCursor = null;

        while (!paginationDone) {
            SearchEntitiesRequest.Builder paginationRequest = SearchEntitiesRequest.newBuilder();
            paginationRequest.mergeFrom(request);
            if (nextCursor != null) {
                paginationRequest.setPaginationParams(PaginationParameters.newBuilder()
                        .setCursor(nextCursor)
                        .build());
            }

            SearchEntitiesResponse paginationResponse
                    = searchService.searchEntities(paginationRequest.build());

            // aggregate entities for current response
            if (paginationResponse.getEntitiesCount() > 0) {
                aggregatedResponseBuilder.addAllEntities(paginationResponse.getEntitiesList());
            } else {
                logger.error("searchEntities response didn't return any entities");
                break;
            }

            // check if pagination is needed
            if (paginationResponse.hasPaginationResponse()
                    && paginationResponse.getPaginationResponse().hasNextCursor()) {
                nextCursor = paginationResponse.getPaginationResponse().getNextCursor();
            } else {
                paginationDone = true;
            }
        }
        return aggregatedResponseBuilder.build();
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
    Set<Long> getGroupIds(@Nonnull GetGroupsRequest request) {
        Spliterator<Grouping> spliterator = Spliterators
                .spliteratorUnknownSize(groupService.getGroups(request), 0);
        return StreamSupport.stream(spliterator, false)
                .map(Grouping::getId).collect(Collectors.toSet());
    }

    Set<Long> getOwnersOfGroups(@Nonnull GetOwnersRequest request) {
        return Sets.newHashSet(groupService.getOwnersOfGroups(request).getOwnerIdList());
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
