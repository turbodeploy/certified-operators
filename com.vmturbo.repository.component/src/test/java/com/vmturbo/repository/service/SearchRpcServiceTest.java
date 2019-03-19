package com.vmturbo.repository.service;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.EntityCountResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.search.AQLRepr;
import com.vmturbo.repository.search.SearchDTOConverter;
import com.vmturbo.repository.search.SearchHandler;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;


@RunWith(MockitoJUnitRunner.class)
public class SearchRpcServiceTest {

    private SearchRpcService searchRpcService;

    @Mock
    private SupplyChainService supplyChainService;

    @Mock
    private TopologyLifecycleManager topologyManager;

    @Mock
    private SearchHandler searchHandler;

    @Mock
    private UserSessionContext userSessionContext;

    private final SearchParameters searchParameterEntityType = SearchParameters.newBuilder()
            .setStartingFilter(entityTypeFilter("VirtualMachine"))
            .build();

    private final SearchParameters searchParameterEntityName = SearchParameters.newBuilder()
            .setStartingFilter(entityNameFilter("foo"))
            .build();

    private final SearchEntityOidsRequest searchEntityOidsRequest = SearchEntityOidsRequest.newBuilder()
            .addSearchParameters(searchParameterEntityType)
            .build();

    private final CountEntitiesRequest countEntitiesRequest = CountEntitiesRequest.newBuilder()
            .addSearchParameters(searchParameterEntityType)
            .build();

    private final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            .setLimit(20)
            .setCursor("0")
            .setAscending(true)
            .setOrderBy(OrderBy.newBuilder().setSearch(SearchOrderBy.ENTITY_NAME))
            .build();

    private final PaginationParameters paginationParametersOnlySort =
            PaginationParameters.newBuilder(paginationParameters)
                    .clearLimit()
                    .build();


    private final SearchEntitiesRequest simpleRequestWithPagination = SearchEntitiesRequest.newBuilder()
            .addSearchParameters(searchParameterEntityType)
            .setPaginationParams(paginationParameters)
            .build();

    private final SearchEntityOidsRequest requestWithEntityOids = SearchEntityOidsRequest.newBuilder()
            .addSearchParameters(searchParameterEntityType)
            .addEntityOid(1L)
            .addEntityOid(2L)
            .build();

    private final SearchEntitiesRequest searchEntitiesRequest = SearchEntitiesRequest.newBuilder()
            .addSearchParameters(searchParameterEntityType)
            .setPaginationParams(paginationParameters)
            .addEntityOid(1L)
            .addEntityOid(2L)
            .build();

    private final SearchEntityOidsRequest searchEntityOidsWithMultiParameters = SearchEntityOidsRequest.newBuilder()
            .addSearchParameters(searchParameterEntityType)
            .addSearchParameters(searchParameterEntityName)
            .build();

    private final SearchEntitiesRequest searchEntitiesWithMultiParameters = SearchEntitiesRequest.newBuilder()
            .addSearchParameters(searchParameterEntityType)
            .addSearchParameters(searchParameterEntityName)
            .setPaginationParams(paginationParameters)
            .build();

    // Hold converted results from a list of SearchParameters. Each searchParameters will be converted
    // to a list of AQLRepr, each list of AQLRepr will be send to database for query
    private List<List<AQLRepr>> singleReprs = new ArrayList<>();
    private List<List<AQLRepr>> multiReprs = new ArrayList<>();

    private final String db = "db-1";

    @Before
    public void setUp() throws Throwable {
        searchRpcService = new SearchRpcService(supplyChainService,
                                          topologyManager,
                                          searchHandler, 100, 500,
                                            userSessionContext);

        given(topologyManager.getRealtimeDatabase()).willReturn(
                Optional.of(TopologyDatabase.from(db)));

        for (SearchParameters searchParameters : searchEntityOidsRequest.getSearchParametersList()) {
            singleReprs.add(SearchDTOConverter.toAqlRepr(searchParameters));
        }

        for (SearchParameters searchParameters : searchEntityOidsWithMultiParameters.getSearchParametersList()) {
            multiReprs.add(SearchDTOConverter.toAqlRepr(searchParameters));
        }
    }

    private Search.PropertyFilter entityTypeFilter(final String entityType) {
        return Search.PropertyFilter.newBuilder()
                .setPropertyName("entityType")
                .setStringFilter(Search.PropertyFilter.StringFilter.newBuilder()
                        .setStringPropertyRegex(entityType)
                        .build())
                .build();
    }

    private Search.PropertyFilter entityNameFilter(final String entityName) {
        return Search.PropertyFilter.newBuilder()
                .setPropertyName("displayName")
                .setStringFilter(Search.PropertyFilter.StringFilter.newBuilder()
                        .setStringPropertyRegex(entityName))
                .build();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntityOidsWithException() {
        final StreamObserver<SearchEntityOidsResponse> mockObserver = Mockito.mock(StreamObserver.class);

        given(searchHandler.searchEntityOids(singleReprs.get(0), db, Optional.empty(), Collections.emptyList()))
                .willReturn(Either.left(new Exception()));

        searchRpcService.searchEntityOids(searchEntityOidsRequest, mockObserver);

        verify(mockObserver).onError(any(Exception.class));
        verify(mockObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntityOidsNoTopology() {
        final StreamObserver<SearchEntityOidsResponse> mockObserver = Mockito.mock(StreamObserver.class);

        given(topologyManager.getRealtimeDatabase()).willReturn(Optional.empty());

        searchRpcService.searchEntityOids(searchEntityOidsRequest, mockObserver);

        // The result should be empty as no topology available for search.
        verify(mockObserver).onNext(
                SearchEntityOidsResponse.newBuilder().addAllEntities(Collections.emptyList()).build());
        verify(mockObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntityOids() {
        final StreamObserver<SearchEntityOidsResponse> mockObserver = Mockito.mock(StreamObserver.class);

        final List<Long> oids = Arrays.asList(1L, 2L);
        given(searchHandler.searchEntityOids(singleReprs.get(0), db, Optional.empty(),
                Collections.emptyList())).willReturn(Either.right(Arrays.asList("1", "2")));

        searchRpcService.searchEntityOids(searchEntityOidsRequest, mockObserver);

        verify(mockObserver).onNext(SearchEntityOidsResponse.newBuilder().addAllEntities(oids).build());
        verify(mockObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntityOidsWithMultiParameters() {
        final StreamObserver<SearchEntityOidsResponse> mockObserver = Mockito.mock(StreamObserver.class);

        given(searchHandler.searchEntityOids(multiReprs.get(0), db, Optional.empty(),
                Collections.emptyList())).willReturn(Either.right(Arrays.asList("1", "2")));
        given(searchHandler.searchEntityOids(multiReprs.get(1), db, Optional.empty(),
                Collections.emptyList())).willReturn(Either.right(Arrays.asList("2", "3")));

        searchRpcService.searchEntityOids(searchEntityOidsWithMultiParameters, mockObserver);

        verify(mockObserver).onNext(SearchEntityOidsResponse.newBuilder()
                .addAllEntities(Lists.newArrayList(2L)).build());
        verify(mockObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntityOidsWithCandidates() {
        final StreamObserver<SearchEntityOidsResponse> mockObserver = Mockito.mock(StreamObserver.class);
        final List<String> entityOids = Lists.newArrayList("1", "2");
        final List<Long> oids = Arrays.asList(1L, 2L);
        given(searchHandler.searchEntityOids(singleReprs.get(0), db, Optional.empty(), entityOids)).willReturn(
                Either.right(Arrays.asList("1", "2")));

        searchRpcService.searchEntityOids(requestWithEntityOids, mockObserver);

        verify(mockObserver).onNext(SearchEntityOidsResponse.newBuilder().addAllEntities(oids).build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testSearchEntityOidsWithScopedUser() {
        // a scoped user should see a filtered set of results from the search endpoint.
        List<Long> accessibleEntities = Arrays.asList(1L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        Mockito.when(userSessionContext.isUserScoped()).thenReturn(true);
        Mockito.when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        final List<Long> oids = Arrays.asList(1L, 2L);
        given(searchHandler.searchEntityOids(singleReprs.get(0), db, Optional.empty(),
                Collections.emptyList())).willReturn(Either.right(Arrays.asList("1", "2")));

        final StreamObserver<SearchEntityOidsResponse> mockObserver = Mockito.mock(StreamObserver.class);
        searchRpcService.searchEntityOids(searchEntityOidsRequest, mockObserver);

        // the scoped user should only see their accessible subset.
        verify(mockObserver).onNext(SearchEntityOidsResponse.newBuilder()
                .addEntities(1L)
                .build());
    }

    public void testCountEntities() {
        final StreamObserver<EntityCountResponse> mockObserver = Mockito.mock(StreamObserver.class);

        given(searchHandler.searchEntityOids(singleReprs.get(0), db, Optional.empty(), Collections.emptyList()))
                .willReturn(Either.right(Arrays.asList("1", "2")));

        searchRpcService.countEntities(countEntitiesRequest, mockObserver);

        verify(mockObserver).onNext(EntityCountResponse.newBuilder().setEntityCount(2).build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testCountEntitiesWithScopedUser() {
        // a scoped user should see a filtered set of results from the search endpoint.
        List<Long> accessibleEntities = Arrays.asList(1L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        Mockito.when(userSessionContext.isUserScoped()).thenReturn(true);
        Mockito.when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        final List<Long> oids = Arrays.asList(1L, 2L);
        given(searchHandler.searchEntityOids(singleReprs.get(0), db, Optional.empty(),
                Collections.emptyList())).willReturn(Either.right(Arrays.asList("1", "2")));

        final StreamObserver<EntityCountResponse> mockObserver = Mockito.mock(StreamObserver.class);
        searchRpcService.countEntities(countEntitiesRequest, mockObserver);

        // the scoped user should only see their accessible subset.
        verify(mockObserver).onNext(EntityCountResponse.newBuilder().setEntityCount(1).build());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntitiesWithException() {
        final StreamObserver<SearchEntitiesResponse> mockObserver = Mockito.mock(StreamObserver.class);

        given(searchHandler.searchEntities(singleReprs.get(0), db, Optional.empty(),
                Collections.emptyList())).willReturn(Either.left(new Exception()));

        searchRpcService.searchEntities(simpleRequestWithPagination, mockObserver);

        verify(mockObserver).onError(any(Exception.class));
        verify(mockObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntitiesNoTopology() {
        final StreamObserver<SearchEntitiesResponse> mockObserver = Mockito.mock(StreamObserver.class);

        given(topologyManager.getRealtimeDatabase()).willReturn(Optional.empty());

        searchRpcService.searchEntities(simpleRequestWithPagination, mockObserver);

        // There shouldn't be any entity sent as no topology available for search.
        verify(mockObserver, never()).onNext(any());
        verify(mockObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntities() {
        final StreamObserver<SearchEntitiesResponse> mockObserver = Mockito.mock(StreamObserver.class);

        final Entity vmEntity = SearchDTOConverter.toSearchEntity(vmRepoDto);
        final PaginationParameters plusOnePagination =
                PaginationParameters.newBuilder(paginationParameters)
                        .setLimit(paginationParameters.getLimit() + 1)
                        .build();
        given(searchHandler.searchEntities(singleReprs.get(0), db, Optional.of(plusOnePagination),
                Lists.newArrayList("1", "2"))).willReturn(
                Either.right(Arrays.asList(vmRepoDto)));
        final SearchEntitiesResponse.Builder responseBuilder = SearchEntitiesResponse.newBuilder()
                .addEntities(vmEntity);
        searchRpcService.searchEntities(searchEntitiesRequest, mockObserver);

        verify(mockObserver, never()).onError(any());
        verify(mockObserver).onNext(responseBuilder
                .setPaginationResponse(PaginationResponse.newBuilder()).build());
        verify(mockObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntitiesWithPagination() {
        final StreamObserver<SearchEntitiesResponse> mockObserver = Mockito.mock(StreamObserver.class);

        final Entity vmEntity = SearchDTOConverter.toSearchEntity(vmRepoDto);
        final PaginationParameters plusOnePagination =
                PaginationParameters.newBuilder(simpleRequestWithPagination.getPaginationParams())
                        .setLimit(simpleRequestWithPagination.getPaginationParams().getLimit() + 1)
                        .build();
        given(searchHandler.searchEntities(singleReprs.get(0), db,
                Optional.of(plusOnePagination),
                Collections.emptyList())).willReturn(
                Either.right(Arrays.asList(vmRepoDto)));
        final SearchEntitiesResponse.Builder responseBuilder = SearchEntitiesResponse.newBuilder()
                .addEntities(vmEntity);
        searchRpcService.searchEntities(simpleRequestWithPagination, mockObserver);

        verify(mockObserver, never()).onError(any());
        verify(mockObserver).onNext(
                responseBuilder.setPaginationResponse(PaginationResponse.newBuilder()).build());
        verify(mockObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntitiesWithMultiParameters() {
        final StreamObserver<SearchEntitiesResponse> mockObserver = Mockito.mock(StreamObserver.class);
        final Optional<TopologyID> topologyID =
                Optional.of(new TopologyID(1L,2L, TopologyType.SOURCE));
        given(searchHandler.searchEntityOids(multiReprs.get(0), db, Optional.of(paginationParametersOnlySort),
                Collections.emptyList())).willReturn(Either.right(Arrays.asList("123", "124")));
        given(searchHandler.searchEntityOids(multiReprs.get(1), db, Optional.of(paginationParametersOnlySort),
                Collections.emptyList())).willReturn(Either.right(Arrays.asList("123", "456")));

        final List<ServiceEntityRepoDTO> serviceEntityRepoDTOs =
                com.google.common.collect.Lists.newArrayList(vmRepoDto);
        final Entity vmEntity = SearchDTOConverter.toSearchEntity(vmRepoDto);
        given(topologyManager.getRealtimeTopologyId()).willReturn(topologyID);
        given(searchHandler.getEntitiesByOids(Sets.newHashSet(123L), topologyID))
                .willReturn(Either.right(serviceEntityRepoDTOs));
        searchRpcService.searchEntities(searchEntitiesWithMultiParameters, mockObserver);
        verify(mockObserver).onNext(SearchEntitiesResponse.newBuilder()
                .setPaginationResponse(PaginationResponse.newBuilder().build())
                .addAllEntities(Lists.newArrayList(vmEntity)).build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testSearchEntitiesWithScopedUser() {
        // a scoped user should see a filtered set of results from the search endpoint.
        List<Long> accessibleEntities = Arrays.asList(123L);
        EntityAccessScope userScope = new EntityAccessScope(null, null, new ArrayOidSet(accessibleEntities), null);
        Mockito.when(userSessionContext.isUserScoped()).thenReturn(true);
        Mockito.when(userSessionContext.getUserAccessScope()).thenReturn(userScope);

        final SearchEntitiesRequest searchEntitiesRequest = SearchEntitiesRequest.newBuilder()
                .addSearchParameters(searchParameterEntityType)
                .setPaginationParams(paginationParameters)
                .build();

        final PaginationParameters plusOnePagination =
                PaginationParameters.newBuilder(paginationParameters)
                        .setLimit(paginationParameters.getLimit() + 1)
                        .build();
        given(searchHandler.searchEntities(singleReprs.get(0), db, Optional.of(plusOnePagination),
                Collections.emptyList())).willReturn(
                Either.right(Arrays.asList(vmRepoDto, vmRepoDtoTwo)));

        final StreamObserver<SearchEntitiesResponse> mockObserver = Mockito.mock(StreamObserver.class);
        searchRpcService.searchEntities(searchEntitiesRequest, mockObserver);

        // the scoped should should only see the "123" vm, even though both 123 and 124 are available.
        final Entity vmEntity = SearchDTOConverter.toSearchEntity(vmRepoDto);
        final SearchEntitiesResponse.Builder responseBuilder = SearchEntitiesResponse.newBuilder()
                .addEntities(vmEntity);

        verify(mockObserver).onNext(responseBuilder
                .setPaginationResponse(PaginationResponse.newBuilder()).build());
    }


    private static final ServiceEntityRepoDTO vmRepoDto;

    static {
        vmRepoDto = new ServiceEntityRepoDTO();
        vmRepoDto.setDisplayName("vm-1");
        vmRepoDto.setEntityType("VirtualMachine");
        vmRepoDto.setState("ACTIVE");
        vmRepoDto.setOid("123");
    }

    private static final ServiceEntityRepoDTO vmRepoDtoTwo;

    static {
        vmRepoDtoTwo = new ServiceEntityRepoDTO();
        vmRepoDtoTwo.setDisplayName("vm-2");
        vmRepoDtoTwo.setEntityType("VirtualMachine");
        vmRepoDtoTwo.setState("ACTIVE");
        vmRepoDtoTwo.setOid("124");
    }

    private static final ServiceEntityRepoDTO pmRepoDto;

    static {
        pmRepoDto = new ServiceEntityRepoDTO();
        pmRepoDto.setDisplayName("pm-1");
        pmRepoDto.setEntityType("PhysicalMachine");
        pmRepoDto.setState("SUSPENDED");
        pmRepoDto.setOid("456");
    }
}
