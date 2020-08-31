package com.vmturbo.mediation.udt.explore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionMoles.TopologyDataDefinitionServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Test class for {@link RequestExecutor}.
 */
public class RequestExecutorTest {

    private Connection connection;
    private GrpcTestServer groupServer;
    private GrpcTestServer repositoryServer;
    private GrpcTestServer tpServer;
    private TopologyDataDefinitionServiceMole tddMole;
    private RepositoryServiceMole rsMole;
    private GroupServiceMole groupMole;
    private SearchServiceMole searchMole;

    /**
     * Initialize request executor.
     *
     * @throws IOException on exception starting gRPC in-memory servers
     */
    @Before
    public void setup() throws IOException {
        tddMole = new TopologyDataDefinitionServiceMole();
        rsMole = new RepositoryServiceMole();
        groupMole = new GroupServiceMole();
        searchMole = Mockito.spy(new SearchServiceMole());
        groupServer = GrpcTestServer.newServer(tddMole, groupMole);
        groupServer.start();
        repositoryServer = GrpcTestServer.newServer(rsMole, searchMole);
        repositoryServer.start();
        tpServer = GrpcTestServer.newServer();
        tpServer.start();

        connection = Mockito.spy(
                new Connection(groupServer.getChannel(), repositoryServer.getChannel(),
                        tpServer.getChannel()));
    }

    /**
     * Cleans up tests.
     */
    @After
    public void cleanup() {
        groupServer.close();
        repositoryServer.close();
        tpServer.close();
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
        Assert.assertEquals(groupServer.getChannel(), requestExecutor.getGroupService().getChannel());
        Assert.assertEquals(groupServer.getChannel(), requestExecutor.getTopologyDataDefService().getChannel());
        Assert.assertEquals(repositoryServer.getChannel(), requestExecutor.getSearchService().getChannel());
        Assert.assertEquals(repositoryServer.getChannel(), requestExecutor.getRepositoryService().getChannel());
    }

    /**
     * Test pagination when getting the entities.
     */
    @Test
    public void testSearchEntities() {
        final String nextCursor = "3";

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

        Mockito.when(searchMole.searchEntities(request1)).thenReturn(response1);
        Mockito.when(searchMole.searchEntities(request2)).thenReturn(response2);
        final RequestExecutor requestExecutor = new RequestExecutor(connection);

        SearchEntitiesResponse response = requestExecutor.searchEntities(request1);

        Mockito.verify(searchMole, Mockito.times(2))
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
