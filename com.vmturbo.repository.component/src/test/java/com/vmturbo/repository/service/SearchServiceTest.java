package com.vmturbo.repository.service;

import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.Entity;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchRequest;
import com.vmturbo.common.protobuf.search.Search.SearchResponse;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.search.AQLRepr;
import com.vmturbo.repository.search.SearchDTOConverter;
import com.vmturbo.repository.search.SearchHandler;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyIDManager;

import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;


@RunWith(MockitoJUnitRunner.class)
public class SearchServiceTest {

    private SearchService searchService;

    @Mock
    private SupplyChainService supplyChainService;

    @Mock
    private TopologyIDManager topologyIDManager;

    @Mock
    private SearchHandler searchHandler;

    private final SearchRequest simpleRequest = SearchRequest.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder().setStartingFilter(
                    entityTypeFilter("VirtualMachine")).build())
            .build();

    // Hold converted results from a list of SearchParameters. Each searchParameters will be converted
    // to a list of AQLRepr, each list of AQLRepr will be send to database for query
    private List<List<AQLRepr>> reprs = new ArrayList<>();

    private final String db = "db-1";

    @Before
    public void setUp() throws Throwable {
        searchService = new SearchService(supplyChainService,
                                          topologyIDManager,
                                          searchHandler);

        given(topologyIDManager.currentRealTimeDatabase()).willReturn(
                Optional.of(TopologyDatabase.from(db)));

        for (SearchParameters searchParameters : simpleRequest.getSearchParametersList()) {
            reprs.add(SearchDTOConverter.toAqlRepr(searchParameters));
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

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntityOidsWithException() {
        final StreamObserver<SearchResponse> mockObserver = Mockito.mock(StreamObserver.class);

        given(searchHandler.searchEntityOids(reprs.get(0), db)).willReturn(Either.left(new Exception()));

        searchService.searchEntityOids(simpleRequest, mockObserver);

        verify(mockObserver).onError(any(Exception.class));
        verify(mockObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntityOidsNoTopology() {
        final StreamObserver<SearchResponse> mockObserver = Mockito.mock(StreamObserver.class);

        given(topologyIDManager.currentRealTimeDatabase()).willReturn(Optional.empty());

        searchService.searchEntityOids(simpleRequest, mockObserver);

        // The result should be empty as no topology available for search.
        verify(mockObserver).onNext(
                SearchResponse.newBuilder().addAllEntities(Collections.emptyList()).build());
        verify(mockObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntityOids() {
        final StreamObserver<SearchResponse> mockObserver = Mockito.mock(StreamObserver.class);

        final List<Long> oids = Arrays.asList(1L, 2L);
        given(searchHandler.searchEntityOids(reprs.get(0), db)).willReturn(
                Either.right(Arrays.asList("1", "2")));

        searchService.searchEntityOids(simpleRequest, mockObserver);

        verify(mockObserver).onNext(SearchResponse.newBuilder().addAllEntities(oids).build());
        verify(mockObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntitiesWithException() {
        final StreamObserver<Entity> mockObserver = Mockito.mock(StreamObserver.class);

        given(searchHandler.searchEntities(reprs.get(0), db)).willReturn(Either.left(new Exception()));

        searchService.searchEntities(simpleRequest, mockObserver);

        verify(mockObserver).onError(any(Exception.class));
        verify(mockObserver, never()).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntitiesNoTopology() {
        final StreamObserver<Entity> mockObserver = Mockito.mock(StreamObserver.class);

        given(topologyIDManager.currentRealTimeDatabase()).willReturn(Optional.empty());

        searchService.searchEntities(simpleRequest, mockObserver);

        // There shouldn't be any entity sent as no topology available for search.
        verify(mockObserver, never()).onNext(any());
        verify(mockObserver).onCompleted();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSearchEntities() {
        final StreamObserver<Entity> mockObserver = Mockito.mock(StreamObserver.class);

        final Entity vmEntity = SearchDTOConverter.toSearchEntity(vmRepoDto);
        final Entity pmEntity = SearchDTOConverter.toSearchEntity(pmRepoDto);

        given(searchHandler.searchEntities(reprs.get(0), db)).willReturn(
                Either.right(Arrays.asList(vmRepoDto, pmRepoDto)));

        searchService.searchEntities(simpleRequest, mockObserver);

        verify(mockObserver, never()).onError(any());
        verify(mockObserver, times(2)).onNext(any(Entity.class));
        verify(mockObserver).onNext(vmEntity);
        verify(mockObserver).onNext(pmEntity);
        verify(mockObserver).onCompleted();
    }

    private static final ServiceEntityRepoDTO vmRepoDto;

    static {
        vmRepoDto = new ServiceEntityRepoDTO();
        vmRepoDto.setDisplayName("vm-1");
        vmRepoDto.setEntityType("VirtualMachine");
        vmRepoDto.setState("ACTIVE");
        vmRepoDto.setOid("123");
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