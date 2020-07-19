package com.vmturbo.mediation.udt.explore;

import java.util.Arrays;
import java.util.Collections;

import io.grpc.ManagedChannel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionServiceGrpc.TopologyDataDefinitionServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;

/**
 * Test class for {@link RequestExecutor}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TopologyDataDefinitionServiceBlockingStub.class, RepositoryServiceBlockingStub.class,
        GroupServiceBlockingStub.class, SearchServiceBlockingStub.class})
public class RequestExecutorTest {

    private Connection connection;
    private ManagedChannel groupChannel;
    private ManagedChannel repoChannel;

    /**
     * Initialize request executor.
     */
    @Before
    public void setup() {
        connection = Mockito.mock(Connection.class);
        groupChannel = Mockito.mock(ManagedChannel.class);
        repoChannel = Mockito.mock(ManagedChannel.class);
        Mockito.when(connection.getGroupChannel()).thenReturn(groupChannel);
        Mockito.when(connection.getRepositoryChannel()).thenReturn(repoChannel);
    }

    /**
     * The method tests that 'RequestExecutor' correctly calls Connection`s channels.
     */
    @Test
    public void testCreateServices() {
        new RequestExecutor(connection);
        Mockito.verify(connection, Mockito.times(2)).getGroupChannel();
        Mockito.verify(connection, Mockito.times(2)).getRepositoryChannel();
    }

    /**
     * The method tests that services uses correct gRPC channels.
     */
    @Test
    public void testChannelsUsage() {
        RequestExecutor requestExecutor = new RequestExecutor(connection);
        Assert.assertEquals(groupChannel, requestExecutor.getGroupService().getChannel());
        Assert.assertEquals(groupChannel, requestExecutor.getTopologyDataDefService().getChannel());
        Assert.assertEquals(repoChannel, requestExecutor.getSearchService().getChannel());
        Assert.assertEquals(repoChannel, requestExecutor.getRepositoryService().getChannel());
    }

    /**
     * Test pagination when getting the entities.
     */
    @Test
    public void testSearchEntities() {
        final String nextCursor = "3";

        RequestExecutor requestExecutor = new RequestExecutor(
                PowerMockito.mock(TopologyDataDefinitionServiceBlockingStub.class),
                PowerMockito.mock(RepositoryServiceBlockingStub.class),
                PowerMockito.mock(GroupServiceBlockingStub.class),
                PowerMockito.mock(SearchServiceBlockingStub.class));

        // first request - no pagination parameter
        SearchEntitiesRequest request1 = SearchEntitiesRequest.newBuilder()
                .setSearch(Search.SearchQuery.newBuilder()
                    .addAllSearchParameters(Collections.singletonList(SearchParameters.newBuilder().build()))
                    .build())
                .build();
        // first response - has pagination cursor
        SearchEntitiesResponse response1 = SearchEntitiesResponse.newBuilder()
                .setPaginationResponse(PaginationResponse.newBuilder()
                        .setNextCursor(nextCursor)
                        .build())
                .addAllEntities(Arrays.asList(
                        createPartialEntity(1L),
                        createPartialEntity(2L),
                        createPartialEntity(3L)))
                .build();
        // second request - send cursor in pagination parameter
        SearchEntitiesRequest request2 = SearchEntitiesRequest.newBuilder()
                .mergeFrom(request1)
                .setPaginationParams(PaginationParameters.newBuilder()
                        .setCursor(nextCursor)
                        .build())
                .build();
        // second response - pagination finished
        SearchEntitiesResponse response2 = SearchEntitiesResponse.newBuilder()
                .addAllEntities(Arrays.asList(
                        createPartialEntity(4L),
                        createPartialEntity(5L)))
                .build();

        SearchServiceBlockingStub searchService = requestExecutor.getSearchService();
        Mockito.doReturn(response1).when(searchService).searchEntities(request1);
        Mockito.doReturn(response2).when(searchService).searchEntities(request2);

        SearchEntitiesResponse response = requestExecutor.searchEntities(request1);

        Mockito.verify(searchService, Mockito.times(2))
                .searchEntities(Mockito.any());
        Assert.assertEquals(5, response.getEntitiesCount());
    }

    /**
     * Create a partial minimal entity.
     *
     * @param oid the id of the entity.
     * @return a partial entity with the given ID.
     */
    private PartialEntity createPartialEntity(long oid) {
        return PartialEntity.newBuilder()
                .setMinimal(MinimalEntity.newBuilder()
                        .setOid(oid)
                        .build())
                .build();
    }
}
