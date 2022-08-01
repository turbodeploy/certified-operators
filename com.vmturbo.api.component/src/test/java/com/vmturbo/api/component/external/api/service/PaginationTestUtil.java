package com.vmturbo.api.component.external.api.service;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.PaginatedSearchRequest;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.api.pagination.ActionPaginationRequest.ActionPaginationResponse;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IEntitiesService;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.api.serviceinterfaces.IMarketsService;
import com.vmturbo.api.serviceinterfaces.IReservedInstancesService;
import com.vmturbo.api.serviceinterfaces.ISearchService;
import com.vmturbo.api.serviceinterfaces.IStatsService;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.platform.sdk.common.util.Pair;


/**
 * Utilities for running unit tests on methods that accept some kind of
 * {@link com.vmturbo.api.pagination.PaginationRequest} and return a
 * {@link com.vmturbo.api.pagination.PaginationResponse}.
 */
public class PaginationTestUtil {

    @Nonnull
    public static Pair<SearchQuery, SearchPaginationRequest> getPaginatedSearchResults(
        ISearchService searchService,
        RepositoryApi repositoryApi,
        SearchPaginationRequest searchPaginationRequest,
        String query,
        List<String> types,
        List<String> scopes,
        String state,
        List<String> groupTypes,
        EnvironmentType envType,
        List<String> probeTypes,
        EntityDetailType entityDetailType) throws Exception {

        final PaginatedSearchRequest mockPaginatedSearchRequest = Mockito.mock(PaginatedSearchRequest.class);
        doReturn(null).when(mockPaginatedSearchRequest).getResponse();
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedSearchRequest);

        final ArgumentCaptor<SearchQuery> searchQueryArgumentCaptor = ArgumentCaptor.forClass(SearchQuery.class);
        final ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestArgumentCaptor = ArgumentCaptor.forClass(SearchPaginationRequest.class);


        searchService.getSearchResults(
            query,
            types,
            scopes,
            state,
            groupTypes,
            envType,
            entityDetailType,
            searchPaginationRequest,
            null,
            probeTypes,
            null,
            null);

        Mockito.verify(repositoryApi, Mockito.atLeastOnce()).newPaginatedSearch(searchQueryArgumentCaptor.capture(), any(), searchPaginationRequestArgumentCaptor.capture());
        return new Pair<>(searchQueryArgumentCaptor.getValue(), searchPaginationRequestArgumentCaptor.getValue());
    }

    @Nonnull
    public static Collection<BaseApiDTO> getNonPaginatedSearchResults(ISearchService searchService,
                                                                      String query,
                                                                      List<String> types,
                                                                      List<String> scopes,
                                                                      String state,
                                                                      List<String> groupTypes,
                                                                      EnvironmentType envType,
                                                                      List<String> probeTypes,
                                                                      EntityDetailType entityDetailType)
            throws Exception {
        final ArgumentCaptor<List<BaseApiDTO>> resultCaptor = ArgumentCaptor.forClass((Class)List.class);

        final SearchPaginationRequest paginationRequest = spy(new SearchPaginationRequest(null, null, true, null));

        doReturn(Mockito.mock(SearchPaginationResponse.class)).when(paginationRequest).allResultsResponse(any());

        searchService.getSearchResults(
                query,
                types,
                scopes,
                state,
                groupTypes,
                envType,
                entityDetailType,
                paginationRequest,
                null,
                probeTypes, null, null);
        Mockito.verify(paginationRequest).allResultsResponse(resultCaptor.capture());
        return resultCaptor.getValue();
    }

    @Nonnull
    public static List<ActionApiDTO> getActionsByGroupUuid(IGroupsService groupService,
                                                           String uuid,
                                                           ActionApiInputDTO inputDto) throws Exception {
        final ArgumentCaptor<List<ActionApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        final ActionPaginationRequest request = Mockito.mock(ActionPaginationRequest.class);
        Mockito.when(request.allResultsResponse(any()))
                .thenReturn(Mockito.mock(ActionPaginationResponse.class));
        groupService.getActionsByGroupUuid(uuid, inputDto, request);
        Mockito.verify(request).allResultsResponse(resultCaptor.capture());
        return resultCaptor.getValue();
    }

    @Nonnull
    public static List<ActionApiDTO> getActionsByMarketUuid(IMarketsService marketsService,
                                                            String uuid,
                                                            ActionApiInputDTO inputDto) throws Exception {
        final ArgumentCaptor<List<ActionApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        final ActionPaginationRequest request = Mockito.mock(ActionPaginationRequest.class);
        Mockito.when(request.allResultsResponse(any()))
                .thenReturn(Mockito.mock(ActionPaginationResponse.class));
        marketsService.getActionsByMarketUuid(uuid, inputDto, request);
        Mockito.verify(request).allResultsResponse(resultCaptor.capture());
        return resultCaptor.getValue();
    }

    @Nonnull
    public static List<ActionApiDTO> getActionsByEntityUuid(IEntitiesService entitiesService,
                                                            String uuid,
                                                            ActionApiInputDTO inputDto) throws Exception {
        final ArgumentCaptor<List<ActionApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        final ActionPaginationRequest request = Mockito.mock(ActionPaginationRequest.class);
        Mockito.when(request.allResultsResponse(any()))
                .thenReturn(Mockito.mock(ActionPaginationResponse.class));
        entitiesService.getActionsByEntityUuid(uuid, inputDto, request);
        Mockito.verify(request).allResultsResponse(resultCaptor.capture());
        return resultCaptor.getValue();
    }

    @Nonnull
    public static List<EntityStatsApiDTO> getStatsByUuidsQuery(IStatsService statsService,
                                                               StatScopesApiInputDTO inputDto) throws Exception {
        final ArgumentCaptor<List<EntityStatsApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        final EntityStatsPaginationRequest request = Mockito.mock(EntityStatsPaginationRequest.class);
        Mockito.when(request.allResultsResponse(any()))
                .thenReturn(Mockito.mock(EntityStatsPaginationResponse.class));
        statsService.getStatsByUuidsQuery(inputDto, request);
        Mockito.verify(request).allResultsResponse(resultCaptor.capture());
        return resultCaptor.getValue();
    }

    @Nonnull
    public static List<EntityStatsApiDTO> getReservedInstancesStats(IReservedInstancesService reservedInstancesService,
                                                                    StatScopesApiInputDTO inputDto) throws Exception {
        final ArgumentCaptor<List<EntityStatsApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        final EntityStatsPaginationRequest request = Mockito.mock(EntityStatsPaginationRequest.class);
        Mockito.when(request.allResultsResponse(any()))
                .thenReturn(Mockito.mock(EntityStatsPaginationResponse.class));
        reservedInstancesService.getReservedInstancesStats(inputDto, request);
        Mockito.verify(request).allResultsResponse(resultCaptor.capture());
        return resultCaptor.getValue();
    }
}
