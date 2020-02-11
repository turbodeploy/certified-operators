package com.vmturbo.api.component.external.api.service;

import static org.mockito.Matchers.any;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.api.enums.EntityDetailType;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
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

/**
 * Utilities for running unit tests on methods that accept some kind of
 * {@link com.vmturbo.api.pagination.PaginationRequest} and return a
 * {@link com.vmturbo.api.pagination.PaginationResponse}.
 */
public class PaginationTestUtil {

    @Nonnull
    public static Collection<BaseApiDTO> getSearchResults(ISearchService searchService,
                                                          String query,
                                                          List<String> types,
                                                          List<String> scopes,
                                                          String state,
                                                          String groupType,
                                                          EnvironmentType envType,
                                                          List<String> probeTypes,
                                                          EntityDetailType entityDetailType) throws Exception {
        final ArgumentCaptor<List<BaseApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        final SearchPaginationRequest paginationRequest = Mockito.mock(SearchPaginationRequest.class);
        Mockito.when(paginationRequest.allResultsResponse(any()))
                .thenReturn(Mockito.mock(SearchPaginationResponse.class));
        searchService.getSearchResults(
                query,
                types,
                scopes,
                state,
                Collections.singletonList(groupType),
                envType,
                entityDetailType,
                paginationRequest,
                null,
                probeTypes, true);
        Mockito.verify(paginationRequest).allResultsResponse(resultCaptor.capture());
        return resultCaptor.getValue();
    }

    @Nonnull
    public static List<BaseApiDTO> getMembersBasedOnFilter(ISearchService searchService,
                                                           String query,
                                                           GroupApiDTO inputDto) throws Exception {
        final ArgumentCaptor<List<BaseApiDTO>> resultCaptor =
                ArgumentCaptor.forClass((Class)List.class);
        final SearchPaginationRequest paginationRequest = Mockito.mock(SearchPaginationRequest.class);
        Mockito.when(paginationRequest.allResultsResponse(any()))
                .thenReturn(Mockito.mock(SearchPaginationResponse.class));
        searchService.getMembersBasedOnFilter(query, inputDto, paginationRequest, null);
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
