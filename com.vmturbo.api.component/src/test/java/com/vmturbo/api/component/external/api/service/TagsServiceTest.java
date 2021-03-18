package com.vmturbo.api.component.external.api.service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.TagsPaginationMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchTagsRequest;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Tests the tags service.
 */
public class TagsServiceTest {
    private final SearchServiceMole searchService = Mockito.spy(new SearchServiceMole());
    private final GroupExpander groupExpander = Mockito.mock(GroupExpander.class);
    private final RepositoryApi repositoryApi = Mockito.mock(RepositoryApi.class);
    private final PaginationMapper paginationMapper = Mockito.mock(PaginationMapper.class);
    private final TagsPaginationMapper tagPaginationMapper = Mockito.mock(TagsPaginationMapper.class);

    /**
     * Testing gRPC server.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(searchService);

    @Captor
    private ArgumentCaptor<SearchTagsRequest> searchTagsRequestArgumentCaptor;

    private TagsService tagsService;

    private static final long ID_1 = 1L;
    private static final long ID_2 = 2L;
    private static final long ID_3 = 3L;
    private static final long ID_4 = 4L;
    private static final long ID_5 = 5L;
    private static final List<String> SOME_SCOPE =
        ImmutableList.of(Long.toString(ID_1), Long.toString(ID_2), Long.toString(ID_3));
    private static final Set<Long> EXPANDED_SCOPE = ImmutableSet.of(ID_1, ID_4, ID_5, ID_3);

    /**
     * Set up tests.
     *
     * @throws OperationFailedException To satisfy compiler.
     */
    @Before
    public void setUp() throws OperationFailedException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(groupExpander.expandUuids(SOME_SCOPE.stream().collect(Collectors.toSet())))
                .thenReturn(EXPANDED_SCOPE);
        tagsService = new TagsService(SearchServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                                      repositoryApi,
                                      groupExpander, paginationMapper, tagPaginationMapper);
    }

    /**
     * Test {@link TagsService#getTags} with all parameters.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetTagsWithAllParameters() throws Exception {
        final SearchTagsRequest requestMade =
                callGetTags(SOME_SCOPE, ApiEntityType.VIRTUAL_MACHINE.apiStr(), EnvironmentType.ONPREM);
        Assert.assertEquals(EXPANDED_SCOPE,
                            requestMade.getEntitiesList().stream().collect(Collectors.toSet()));
        Assert.assertEquals(ApiEntityType.VIRTUAL_MACHINE.typeNumber(), requestMade.getEntityType());
        Assert.assertEquals(EnvironmentTypeEnum.EnvironmentType.ON_PREM, requestMade.getEnvironmentType());
    }

    /**
     * Test {@link TagsService#getTags} with empty scope and null entity type.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetTagsWithSomeParameters1() throws Exception {
        final SearchTagsRequest requestMade = callGetTags(ImmutableList.of(), null, EnvironmentType.CLOUD);
        Assert.assertTrue(requestMade.getEntitiesList().isEmpty());
        Assert.assertFalse(requestMade.hasEntityType());
        Assert.assertEquals(EnvironmentTypeEnum.EnvironmentType.CLOUD, requestMade.getEnvironmentType());
    }

    /**
     * Test {@link TagsService#getTags} with null scope, null entity type, and hybrid environment.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetTagsWithSomeParameters2() throws Exception {
        final SearchTagsRequest requestMade = callGetTags(null, null, EnvironmentType.HYBRID);
        Assert.assertTrue(requestMade.getEntitiesList().isEmpty());
        Assert.assertFalse(requestMade.hasEntityType());
        Assert.assertEquals(EnvironmentTypeEnum.EnvironmentType.HYBRID, requestMade.getEnvironmentType());
    }

    /**
     * Test {@link TagsService#getTags} with unknown environment.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetTagsWithUnknownEnvironment() throws Exception {
        final SearchTagsRequest requestMade = callGetTags(null, null, EnvironmentType.UNKNOWN);
        Assert.assertTrue(requestMade.getEntitiesList().isEmpty());
        Assert.assertFalse(requestMade.hasEntityType());
        Assert.assertEquals(EnvironmentTypeEnum.EnvironmentType.UNKNOWN_ENV, requestMade.getEnvironmentType());
    }

    /**
     * Test {@link TagsService#getTags} with no parameters.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetTagsWithoutParameters() throws Exception {
        final SearchTagsRequest requestMade = callGetTags(null, null, null);
        Assert.assertTrue(requestMade.getEntitiesList().isEmpty());
        Assert.assertFalse(requestMade.hasEntityType());
        Assert.assertFalse(requestMade.hasEnvironmentType());
    }

    /**
     * Test what happens when inner call fails.
     *
     * @throws Exception must throw {@link OperationFailedException}
     */
    @Test(expected = OperationFailedException.class)
    public void testFailedGetTags() throws Exception {
        Mockito.doThrow(new RuntimeException()).when(searchService).searchTags(Mockito.any());
        callGetTags(null, null, null);
    }

    /**
     * Tests {@link TagsService#getEntitiesByTagKey}.
     *
     * @throws Exception if an error occurs.
     */
    @Test
    public void testGetPaginatedEntitiesByTagKey() throws Exception {

        final String tagKey = "tagKey";
        final ResponseEntity responseEntity = new ResponseEntity<List<ServiceEntityApiDTO>>(HttpStatus.OK);

        final SearchRequest searchRequest = Mockito.mock(SearchRequest.class);
        final String cursor = "cursor";
        final int limit = 10;
        final Boolean ascending = true;
        final String orderBy = StringConstants.NAME;
        SearchPaginationRequest searchPaginationRequest = new SearchPaginationRequest(cursor, limit, ascending, orderBy);
        PaginationParameters paginationParameters = paginationMapper.toProtoParams(searchPaginationRequest);

        Mockito.when(repositoryApi.newSearchRequest(Mockito.eq(
                SearchParameters.newBuilder()
                        .setStartingFilter(PropertyFilter.newBuilder()
                                .setPropertyName(StringConstants.TAGS_ATTR)
                                .setMapFilter(MapFilter.newBuilder().setKey(tagKey)))
                        .build())))
                .thenReturn(searchRequest);

        Mockito.when(searchRequest.getPaginatedSEList(Mockito.eq(paginationParameters)))
                .thenReturn(responseEntity);

        Assert.assertEquals(responseEntity, tagsService.getEntitiesByTagKey(tagKey, searchPaginationRequest));
    }

    /**
     * Tests {@link TagsService#getEntitiesByTagKey}.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetEntitiesByTagKey() throws Exception {
        final String tagKey = "tagKey";
        final SearchRequest searchRequest = Mockito.mock(SearchRequest.class);
        final ServiceEntityApiDTO mockSE = new ServiceEntityApiDTO();
        final ResponseEntity<List<ServiceEntityApiDTO>> mockResult = new ResponseEntity<List<ServiceEntityApiDTO>>(Collections.singletonList(mockSE), new HttpHeaders(), HttpStatus.OK);
        Mockito.when(searchRequest.getSEList()).thenReturn(mockResult.getBody());
        Mockito.when(repositoryApi.newSearchRequest(Mockito.eq(
                SearchParameters.newBuilder()
                    .setStartingFilter(PropertyFilter.newBuilder()
                                            .setPropertyName(StringConstants.TAGS_ATTR)
                                            .setMapFilter(MapFilter.newBuilder().setKey(tagKey)))
                    .build())))
            .thenReturn(searchRequest);

        SearchPaginationRequest searchPaginationRequest = null;
        Assert.assertEquals(mockResult, tagsService.getEntitiesByTagKey(tagKey, searchPaginationRequest));
    }

    private SearchTagsRequest callGetTags(@Nullable final List<String> scope,
                                          @Nullable final String entityType,
                                          @Nullable final EnvironmentType envType)
            throws OperationFailedException {
        tagsService.getTags(scope, entityType, envType, null);
        Mockito.verify(searchService).searchTags(searchTagsRequestArgumentCaptor.capture());
        return searchTagsRequestArgumentCaptor.getValue();
    }
}
