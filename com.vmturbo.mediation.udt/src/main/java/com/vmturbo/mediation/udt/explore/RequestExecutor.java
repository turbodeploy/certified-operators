package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import static com.vmturbo.common.protobuf.group.GroupDTO.GetOwnersRequest;
import static com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import static com.vmturbo.common.protobuf.group.GroupServiceGrpc.newBlockingStub;
import static com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import static com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import static com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StopWatch;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionServiceGrpc;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionServiceGrpc.TopologyDataDefinitionServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.SearchTagValuesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchTagValuesResponse;
import com.vmturbo.common.protobuf.search.Search.TaggedEntities;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * A class responsible for executing gRpc requests to Group and Repository services.
 */
public class RequestExecutor {

    private final Logger logger = LogManager.getLogger();

    private final TopologyDataDefinitionServiceBlockingStub topologyDataDefService;
    private final RepositoryServiceBlockingStub repositoryService;
    private final GroupServiceBlockingStub groupService;
    private final SearchServiceBlockingStub searchService;
    private final TopologyProcessor topologyProcessor;

    /**
     * Constructor.
     *
     * @param connection - provider of gRpc channels.
     */
    public RequestExecutor(@Nonnull Connection connection) {
        this.topologyDataDefService =
                TopologyDataDefinitionServiceGrpc.newBlockingStub(connection.getGroupChannel());
        this.repositoryService =
                RepositoryServiceGrpc.newBlockingStub(connection.getRepositoryChannel());
        this.groupService = newBlockingStub(connection.getGroupChannel());
        this.searchService = SearchServiceGrpc.newBlockingStub(connection.getRepositoryChannel());
        this.topologyProcessor = connection.getTopologyProcessorApi();
    }

    @Nonnull
    Map<String, TaggedEntities> getTagValues(@Nonnull SearchTagValuesRequest request) {
        final StopWatch stopWatch = new StopWatch("UDT request: search tag values");
        stopWatch.start();
        final SearchTagValuesResponse resp = searchService.searchTagValues(request);
        final Map<String, TaggedEntities> map = resp.getEntitiesByTagValueMap();
        stopWatch.stop();
        logger.debug(stopWatch.prettyPrint());
        return map;
    }

    @Nonnull
    SearchEntitiesResponse searchEntities(@Nonnull SearchEntitiesRequest request) {
        final StopWatch stopWatch = new StopWatch("UDT request: search entities");
        stopWatch.start();
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
        stopWatch.stop();
        logger.debug(stopWatch.prettyPrint());
        return aggregatedResponseBuilder.build();
    }

    @Nonnull
    Iterator<GetTopologyDataDefinitionResponse> getAllTopologyDataDefinitions(
            @Nonnull GetTopologyDataDefinitionsRequest request) {
        final StopWatch stopWatch = new StopWatch("UDT request: get all TDD");
        stopWatch.start();
        final Iterator<GetTopologyDataDefinitionResponse> response =
                topologyDataDefService.getAllTopologyDataDefinitions(request);
        stopWatch.stop();
        logger.debug(stopWatch.prettyPrint());
        return response;
    }

    @Nonnull
    Iterator<GetMembersResponse> getGroupMembers(@Nonnull GetMembersRequest request) {
        final StopWatch stopWatch = new StopWatch("UDT request: get group members");
        stopWatch.start();
        final Iterator<GetMembersResponse> response = groupService.getMembers(request);
        stopWatch.stop();
        logger.debug(stopWatch.prettyPrint());
        return response;
    }

    @Nonnull
    Set<Long> getGroupIds(@Nonnull GetGroupsRequest request) {
        final StopWatch stopWatch = new StopWatch("UDT request: get group IDs");
        stopWatch.start();
        final Spliterator<Grouping> spliterator = Spliterators
                .spliteratorUnknownSize(groupService.getGroups(request), 0);
        stopWatch.stop();
        logger.debug(stopWatch.prettyPrint());
        return StreamSupport.stream(spliterator, false)
                .map(Grouping::getId).collect(Collectors.toSet());
    }

    @Nonnull
    Set<Long> getOwnersOfGroups(@Nonnull GetOwnersRequest request) {
        final StopWatch stopWatch = new StopWatch("UDT request: get owner of group");
        stopWatch.start();
        final Set<Long> result = Sets.newHashSet(groupService.getOwnersOfGroups(request).getOwnerIdList());
        stopWatch.stop();
        logger.debug(stopWatch.prettyPrint());
        return result;
    }

    @Nullable
    ProbeInfo findProbe(SDKProbeType probeType) throws CommunicationException {
        final StopWatch stopWatch = new StopWatch("UDT request: find probe");
        stopWatch.start();
        final ProbeInfo probeInfo = topologyProcessor.getAllProbes()
                .stream().filter(probe -> probe.getType().equals(probeType.getProbeType()))
                .findFirst().orElse(null);
        stopWatch.stop();
        logger.debug(stopWatch.prettyPrint());
        return probeInfo;

    }

    @Nonnull
    List<TargetInfo> findTarget(long probeId) throws CommunicationException {
        final StopWatch stopWatch = new StopWatch("UDT request: find target");
        stopWatch.start();
        final List<TargetInfo> targetInfo = topologyProcessor.getAllTargets().stream()
                .filter(t -> t.getProbeId() == probeId).collect(Collectors.toList());
        stopWatch.stop();
        logger.debug(stopWatch.prettyPrint());
        return targetInfo;
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
