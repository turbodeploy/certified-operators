package com.vmturbo.api.component.communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.ManagedChannel;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.PaginatedSearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.RepositoryRequestResult;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.EntityDetailsMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.PriceIndexPopulator;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.businessaccount.BusinessAccountMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.EntityCountResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class RepositoryApiTest {

    private static final SearchParameters SEARCH_PARAMS =
            SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(1)).build();

    private final long realtimeContextId = 777777L;

    private final SeverityPopulator severityPopulator = mock(SeverityPopulator.class);

    private final PriceIndexPopulator priceIndexPopulatorMock = mock(PriceIndexPopulator.class);

    private final SearchServiceMole searchBackend = Mockito.spy(new SearchServiceMole());

    private final RepositoryServiceMole repoBackend = Mockito.spy(new RepositoryServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(searchBackend, repoBackend);

    private final ServiceEntityMapper serviceEntityMapper = mock(ServiceEntityMapper.class);

    private RepositoryApi repositoryApi;
    private BusinessAccountMapper businessAccountMapper;
    private PaginationMapper paginationMapper = mock(PaginationMapper.class);
    private EntityDetailsMapper entityDetailsMapper = mock(EntityDetailsMapper.class);

    @Before
    public void setup() {
        this.businessAccountMapper = mock(BusinessAccountMapper.class);
        final ManagedChannel grpcChannel = grpcTestServer.getChannel();
        repositoryApi = new RepositoryApi(severityPopulator,
                RepositoryServiceGrpc.newBlockingStub(grpcChannel),
                RepositoryServiceGrpc.newStub(grpcChannel),
                SearchServiceGrpc.newBlockingStub(grpcChannel),
                SearchServiceGrpc.newStub(grpcChannel), serviceEntityMapper, businessAccountMapper,
                paginationMapper, entityDetailsMapper, priceIndexPopulatorMock,
                realtimeContextId);
    }

    private MinimalEntity minimal(final long id) {
        return MinimalEntity.newBuilder()
                .setOid(id)
                .setDisplayName("foo")
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();
    }

    private ApiPartialEntity entity(final long id) {
        return ApiPartialEntity.newBuilder()
                .setOid(id)
                .setDisplayName("foo")
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();
    }

    private TopologyEntityDTO full(final long id) {
        return TopologyEntityDTO.newBuilder()
                .setOid(id)
                .setDisplayName("foo")
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();
    }

    @Test
    public void testGetMinimalEntity() {
        final MinimalEntity ret = minimal(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setMinimal(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entityRequest(7L).getMinimalEntity().get(),
                Matchers.is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.MINIMAL));
    }

    @Test
    public void testGetEntity() {
        // arrange
        final ApiPartialEntity ret = entity(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());

        // act
        final ApiPartialEntity response = repositoryApi.entityRequest(7L).getEntity().get();

        // assert
        MatcherAssert.assertThat(response, Matchers.is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));
    }

    @Test
    public void testGetEntityProjected() {
        // arrange
        final ApiPartialEntity ret = entity(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());

        // act
        final ApiPartialEntity response =
                repositoryApi.entityRequest(7L).projectedTopology().getEntity().get();

        // assert
        MatcherAssert.assertThat(response, Matchers.is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getTopologyType(), Matchers.is(TopologyType.PROJECTED));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));
    }

    @Test
    public void testGetEntityPlanTargetsProjected() {
        // arrange
        final ApiPartialEntity ret = entity(7L);
        final long planContextId = realtimeContextId + 1;

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());

        // act
        final ApiPartialEntity response = repositoryApi.entityRequest(7L)
                .projectedTopology()
                .contextId(planContextId)
                .getEntity()
                .get();

        // assert
        MatcherAssert.assertThat(response, Matchers.is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(planContextId));
        // Should still look in the projected topology!
        MatcherAssert.assertThat(req.getTopologyType(), Matchers.is(TopologyType.PROJECTED));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));
    }

    @Test
    public void testGetEntityPlanTargetsSource() {
        // arrange
        final ApiPartialEntity ret = entity(7L);
        final long planContextId = realtimeContextId + 1;

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());

        // act
        final ApiPartialEntity response =
                repositoryApi.entityRequest(7L).contextId(planContextId).getEntity().get();

        // assert
        MatcherAssert.assertThat(response, Matchers.is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(planContextId));
        // Should still look in the projected topology!
        MatcherAssert.assertThat(req.getTopologyType(), Matchers.is(TopologyType.SOURCE));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));
    }

    @Test
    public void testGetFullEntity() {
        final TopologyEntityDTO ret = full(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entityRequest(7L).getFullEntity().get(),
                Matchers.is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.FULL));
    }

    @Test
    public void testGetSE() throws Exception {
        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        Mockito.when(
                serviceEntityMapper.toServiceEntityApiDTOMap(org.mockito.Matchers.anyCollection()))
                .thenReturn(Collections.singletonMap(ret.getOid(), se));

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entityRequest(7L).getSE().get(), Matchers.is(se));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getTopologyType(), Matchers.is(TopologyType.SOURCE));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));
        Mockito.verify(serviceEntityMapper)
                .toServiceEntityApiDTOMap(Collections.singletonList(ret));
    }

    @Test
    public void testGetSEUseAspectMapper() throws Exception {
        final EntityAspectMapper aspectMapper = mock(EntityAspectMapper.class);

        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final TopologyEntityDTO ret = full(7L);
        Mockito.when(
                serviceEntityMapper.entitiesWithAspects(Mockito.anyListOf(TopologyEntityDTO.class),
                        Mockito.eq(aspectMapper),
                        Mockito.eq(Collections.singletonList(AspectName.CLOUD.getApiName()))))
                .thenReturn(Collections.singletonMap(ret.getOid(), se));

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entityRequest(7L)
                .useAspectMapper(aspectMapper,
                        Collections.singletonList(AspectName.CLOUD.getApiName()))
                .getSE()
                .get(), Matchers.is(se));
        MatcherAssert.assertThat(se.getAspects(), Matchers.is(Matchers.nullValue()));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getTopologyType(), Matchers.is(TopologyType.SOURCE));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.FULL));

        final ArgumentCaptor<Collection> severityPopulatorSEs =
                ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(severityPopulator)
                .populate(Mockito.eq(realtimeContextId), severityPopulatorSEs.capture());
        Assert.assertTrue(severityPopulatorSEs.getValue().contains(se));
    }

    @Test
    public void testGetUseContext() throws Exception {
        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        Mockito.when(
                serviceEntityMapper.toServiceEntityApiDTOMap(org.mockito.Matchers.anyCollection()))
                .thenReturn(Collections.singletonMap(ret.getOid(), se));

        final long contextId = 123;

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entityRequest(7L).contextId(contextId).getSE().get(),
                Matchers.is(se));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(contextId));

        Mockito.verify(serviceEntityMapper)
                .toServiceEntityApiDTOMap(Collections.singletonList(ret));
        Mockito.verify(severityPopulator).populate(contextId, Collections.singleton(se));
    }

    @Test
    public void testMultiGetMinimalEntity() {
        final MinimalEntity ret = minimal(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setMinimal(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
                .getMinimalEntities()
                .collect(Collectors.toList()), Matchers.contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.MINIMAL));
    }

    @Test
    public void testMultiGetMinimalEntityForContext() {
        final MinimalEntity ret = minimal(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setMinimal(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
                .getMinimalEntities()
                .collect(Collectors.toList()), Matchers.contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(777777L));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.MINIMAL));
    }

    @Test
    public void testMultiGetEntity() {
        final ApiPartialEntity ret = entity(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
                .getEntities()
                .collect(Collectors.toList()), Matchers.contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));
    }

    @Test
    public void testMultiGetEntityProjected() {
        final ApiPartialEntity ret = entity(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
                .projectedTopology()
                .getEntities()
                .collect(Collectors.toList()), Matchers.contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getTopologyType(), Matchers.is(TopologyType.PROJECTED));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));
    }

    @Test
    public void testMultiGetFullEntity() {
        final TopologyEntityDTO ret = full(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
                .getFullEntities()
                .collect(Collectors.toList()), Matchers.contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.FULL));
    }

    @Test
    public void testMultiGetNoAllowGetAll() {
        final TopologyEntityDTO ret = full(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entitiesRequest(Collections.emptySet())
                        // Missing allow empty to mean "get all".
                        .getFullEntities().collect(Collectors.toList()),
                Matchers.is(Collections.emptyList()));

        Mockito.verify(repoBackend, Mockito.never())
                .retrieveTopologyEntities(org.mockito.Matchers.any());
    }

    @Test
    public void testMultiGetAllowGetAll() {
        final TopologyEntityDTO ret = full(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entitiesRequest(Collections.emptySet())
                        // Allow empty to mean "get all".
                        .allowGetAll().getFullEntities().collect(Collectors.toList()),
                Matchers.contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.is(Collections.emptyList()));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.FULL));
    }

    @Test
    public void testMultiGetSEList() throws Exception {
        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        Mockito.when(serviceEntityMapper.toServiceEntityApiDTOMap(
                org.mockito.Matchers.anyCollectionOf(ApiPartialEntity.class)))
                .thenReturn(Collections.singletonMap(ret.getOid(), se));

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(
                repositoryApi.entitiesRequest(Collections.singleton(7L)).getSEList(),
                Matchers.contains(se));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getTopologyType(), Matchers.is(TopologyType.SOURCE));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));

        Mockito.verify(serviceEntityMapper)
                .toServiceEntityApiDTOMap(Collections.singletonList(ret));
        Mockito.verify(severityPopulator).populate(realtimeContextId, Collections.singleton(se));
    }

    @Test
    public void testMultiGetSEMap() throws Exception {
        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        Mockito.when(serviceEntityMapper.toServiceEntityApiDTOMap(Collections.singletonList(ret)))
                .thenReturn(Collections.singletonMap(ret.getOid(), se));

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(
                repositoryApi.entitiesRequest(Collections.singleton(7L)).getSEMap(),
                Matchers.is(ImmutableMap.of(7L, se)));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getTopologyType(), Matchers.is(TopologyType.SOURCE));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));

        Mockito.verify(serviceEntityMapper)
                .toServiceEntityApiDTOMap(Collections.singletonList(ret));
        Mockito.verify(severityPopulator).populate(realtimeContextId, Collections.singleton(se));
    }

    @Test
    public void testMultiGetSEUseAspectMapper() throws Exception {
        final EntityAspectMapper aspectMapper = mock(EntityAspectMapper.class);

        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final TopologyEntityDTO ret = full(7L);
        Mockito.when(
                serviceEntityMapper.entitiesWithAspects(Mockito.anyListOf(TopologyEntityDTO.class),
                        Mockito.eq(aspectMapper), Mockito.eq(null)))
                .thenReturn(Collections.singletonMap(ret.getOid(), se));

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(ret))
                .build())).when(repoBackend).retrieveTopologyEntities(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
                .useAspectMapper(aspectMapper)
                .getSEList(), Matchers.contains(se));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
                ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        Mockito.verify(repoBackend).retrieveTopologyEntities(captor.capture());
        final RetrieveTopologyEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getEntityOidsList(), Matchers.contains(7L));
        MatcherAssert.assertThat(req.getTopologyContextId(), Matchers.is(realtimeContextId));
        MatcherAssert.assertThat(req.getTopologyType(), Matchers.is(TopologyType.SOURCE));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.FULL));

        final ArgumentCaptor<Collection> severityPopulatorSEs =
                ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(severityPopulator)
                .populate(Mockito.eq(realtimeContextId), severityPopulatorSEs.capture());
        Assert.assertTrue(severityPopulatorSEs.getValue().contains(se));
    }

    @Test
    public void testSearchMinimalEntity() {
        final MinimalEntity ret = minimal(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setMinimal(ret))
                .build())).when(searchBackend).searchEntitiesStream(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
                .getMinimalEntities()
                .collect(Collectors.toList()), Matchers.contains(ret));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
                ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        Mockito.verify(searchBackend).searchEntitiesStream(captor.capture());
        final SearchEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getSearch().getSearchParametersList(),
                Matchers.contains(SEARCH_PARAMS));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.MINIMAL));
    }

    @Test
    public void testSearchEntity() {
        final ApiPartialEntity ret = entity(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(searchBackend).searchEntitiesStream(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
                .getEntities()
                .collect(Collectors.toList()), Matchers.contains(ret));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
                ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        Mockito.verify(searchBackend).searchEntitiesStream(captor.capture());
        final SearchEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getSearch().getSearchParametersList(),
                Matchers.contains(SEARCH_PARAMS));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));
    }

    @Test
    public void testSearchFullEntity() {
        final TopologyEntityDTO ret = full(7L);

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(ret))
                .build())).when(searchBackend).searchEntitiesStream(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
                .getFullEntities()
                .collect(Collectors.toList()), Matchers.contains(ret));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
                ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        Mockito.verify(searchBackend).searchEntitiesStream(captor.capture());
        final SearchEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getSearch().getSearchParametersList(),
                Matchers.contains(SEARCH_PARAMS));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.FULL));
    }

    @Test
    public void testSearchCount() {
        Mockito.doReturn(EntityCountResponse.newBuilder().setEntityCount(5).build())
                .when(searchBackend)
                .countEntities(org.mockito.Matchers.any());

        MatcherAssert.assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS).count(),
                Matchers.is(5L));

        final ArgumentCaptor<CountEntitiesRequest> captor =
                ArgumentCaptor.forClass(CountEntitiesRequest.class);
        Mockito.verify(searchBackend).countEntities(captor.capture());
        final CountEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getSearch().getSearchParametersList(),
                Matchers.contains(SEARCH_PARAMS));
    }

    @Test
    public void testSearchOids() {
        Mockito.doReturn(SearchEntityOidsResponse.newBuilder().addEntities(7L).build())
                .when(searchBackend)
                .searchEntityOids(org.mockito.Matchers.any());

        MatcherAssert.assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS).getOids(),
                Matchers.contains(7L));

        final ArgumentCaptor<SearchEntityOidsRequest> captor =
                ArgumentCaptor.forClass(SearchEntityOidsRequest.class);
        Mockito.verify(searchBackend).searchEntityOids(captor.capture());
        final SearchEntityOidsRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getSearch().getSearchParametersList(),
                Matchers.contains(SEARCH_PARAMS));
    }

    @Test
    public void testSearchSEList() throws Exception {
        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        Mockito.when(
                serviceEntityMapper.toServiceEntityApiDTOMap(org.mockito.Matchers.anyCollection()))
                .thenReturn(Collections.singletonMap(ret.getOid(), se));

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(searchBackend).searchEntitiesStream(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS).getSEList(),
                Matchers.contains(se));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
                ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        Mockito.verify(searchBackend).searchEntitiesStream(captor.capture());
        final SearchEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getSearch().getSearchParametersList(),
                Matchers.contains(SEARCH_PARAMS));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));

        Mockito.verify(serviceEntityMapper)
                .toServiceEntityApiDTOMap(Collections.singletonList(ret));
        Mockito.verify(severityPopulator).populate(realtimeContextId, Collections.singleton(se));
    }

    @Test
    public void testSearchSEUseAspectMapper() throws Exception {
        final EntityAspectMapper aspectMapper = mock(EntityAspectMapper.class);
        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final TopologyEntityDTO ret = full(7L);
        Mockito.when(
                serviceEntityMapper.entitiesWithAspects(Mockito.anyListOf(TopologyEntityDTO.class),
                        Mockito.eq(aspectMapper), Mockito.eq(null)))
                .thenReturn(Collections.singletonMap(ret.getOid(), se));

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(ret))
                .build())).when(searchBackend).searchEntitiesStream(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
                .useAspectMapper(aspectMapper)
                .getSEList(), Matchers.contains(se));

        // Check to make sure we used the aspect mapper.
        final ArgumentCaptor<Collection> severityPopulatorSEs =
                ArgumentCaptor.forClass(Collection.class);
        Mockito.verify(severityPopulator)
                .populate(Mockito.eq(realtimeContextId), severityPopulatorSEs.capture());
        Assert.assertTrue(severityPopulatorSEs.getValue().contains(se));
    }

    /**
     * Test that the PriceIndexPopulator#populateRealTimeEntities method is called when flag is set to True.
     *
     * @throws Exception if any error occurs
     */
    @Test
    public void testSearchSEListUsesPriceIndexPopulator() throws Exception {
        // GIVEN
        final EntityAspectMapper aspectMapperMock = mock(EntityAspectMapper.class);
        final ServiceEntityApiDTO serviceEntity1 = new ServiceEntityApiDTO();
        serviceEntity1.setUuid("7");

        final TopologyEntityDTO topologyEntityDTO = full(7L);
        Mockito.when(
                serviceEntityMapper.entitiesWithAspects(Mockito.anyListOf(TopologyEntityDTO.class),
                        Mockito.eq(aspectMapperMock), Mockito.eq(null)))
                .thenReturn(Collections.singletonMap(topologyEntityDTO.getOid(), serviceEntity1));

        final List<PartialEntityBatch> partialEntityBatch = Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder().setFullEntity(topologyEntityDTO))
                        .build());
        Mockito.doReturn(partialEntityBatch).when(searchBackend).searchEntitiesStream(org.mockito.Matchers.any());

        // WHEN
        final List<ServiceEntityApiDTO> serviceEntitiesList = repositoryApi.newSearchRequest(SEARCH_PARAMS)
                .usePriceIndexPopulator(true)
                .useAspectMapper(aspectMapperMock)
                .getSEList();

        // EXPECT
        MatcherAssert.assertThat(serviceEntitiesList, Matchers.contains(serviceEntity1));

        // Check to make sure we used the priceIndexPopulator.
        final ArgumentCaptor<List> priceIndexCapturedSEs =
                ArgumentCaptor.forClass(List.class);
        Mockito.verify(priceIndexPopulatorMock)
                .populateRealTimeEntities(priceIndexCapturedSEs.capture());
        Assert.assertTrue(priceIndexCapturedSEs.getValue().contains(serviceEntity1));
    }

    /**
     * Test that the PriceIndexPopulator#populateRealTimeEntities method is NOT called by default.
     *
     * @throws Exception if any error occurs
     */
    @Test
    public void testSearchSEListNotUsingPriceIndexPopulator() throws Exception {
        // GIVEN
        final EntityAspectMapper aspectMapperMock = mock(EntityAspectMapper.class);
        final ServiceEntityApiDTO serviceEntity1 = new ServiceEntityApiDTO();
        serviceEntity1.setUuid("7");

        final TopologyEntityDTO topologyEntityDTO = full(7L);
        Mockito.when(
                serviceEntityMapper.entitiesWithAspects(Mockito.anyListOf(TopologyEntityDTO.class),
                        Mockito.eq(aspectMapperMock), Mockito.eq(null)))
                .thenReturn(Collections.singletonMap(topologyEntityDTO.getOid(), serviceEntity1));

        final List<PartialEntityBatch> partialEntityBatch = Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setFullEntity(topologyEntityDTO))
                .build());
        Mockito.doReturn(partialEntityBatch).when(searchBackend).searchEntitiesStream(org.mockito.Matchers.any());

        // WHEN
        final List<ServiceEntityApiDTO> serviceEntitiesList = repositoryApi.newSearchRequest(SEARCH_PARAMS)
                .useAspectMapper(aspectMapperMock)
                .getSEList();

        // EXPECT
        MatcherAssert.assertThat(serviceEntitiesList, Matchers.contains(serviceEntity1));

        // Check to make sure we do not use the priceIndexPopulator by default
        Mockito.verifyZeroInteractions(priceIndexPopulatorMock);
    }

    @Test
    public void testSearchSEMap() throws Exception {
        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        Mockito.when(serviceEntityMapper.toServiceEntityApiDTOMap(Collections.singletonList(ret)))
                .thenReturn(Collections.singletonMap(7L, se));

        Mockito.doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder().setApi(ret))
                .build())).when(searchBackend).searchEntitiesStream(org.mockito.Matchers.any());
        MatcherAssert.assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS).getSEMap(),
                Matchers.is(ImmutableMap.of(7L, se)));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
                ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        Mockito.verify(searchBackend).searchEntitiesStream(captor.capture());
        final SearchEntitiesRequest req = captor.getValue();
        MatcherAssert.assertThat(req.getSearch().getSearchParametersList(),
                Matchers.contains(SEARCH_PARAMS));
        MatcherAssert.assertThat(req.getReturnType(), Matchers.is(Type.API));

        Mockito.verify(serviceEntityMapper)
                .toServiceEntityApiDTOMap(Collections.singletonList(ret));
        Mockito.verify(severityPopulator).populate(realtimeContextId, Collections.singleton(se));
    }

    /**
     * Tests returning only business accounts by OIDs.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testGetByIdsOnlyBusinessAccounts() throws Exception {
        final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build();
        Mockito.when(repoBackend.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyType(TopologyType.SOURCE)
                        .setReturnType(Type.FULL)
                        .addEntityOids(1L)
                        .setTopologyContextId(realtimeContextId)
                        .addEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .build()))
                .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder().setFullEntity(businessAccount))
                        .build()));
        final BusinessUnitApiDTO buDto = new BusinessUnitApiDTO();
        buDto.setUuid("1");
        Mockito.when(
                businessAccountMapper.convert(Collections.singletonList(businessAccount), false))
                .thenReturn(Collections.singletonList(buDto));
        final RepositoryRequestResult repositoryResult =
                repositoryApi.getByIds(Collections.singleton(1L),
                        Collections.singleton(EntityType.BUSINESS_ACCOUNT), false);
        Mockito.verify(repoBackend).retrieveTopologyEntities(Mockito.any());
        Assert.assertEquals(Collections.emptySet(),
                new HashSet<>(repositoryResult.getServiceEntities()));
        Assert.assertEquals(1, repositoryResult.getBusinessAccounts().size());
        Assert.assertEquals(0, repositoryResult.getServiceEntities().size());
        final BusinessUnitApiDTO buReturned =
                repositoryResult.getBusinessAccounts().iterator().next();
        Assert.assertEquals("1", buReturned.getUuid());
    }

    /**
     * Tests retrieving business accounts and service entities by OIDs.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testGetBusinessAccountsAndServiceEntities() throws Exception {
        final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build();
        final ApiPartialEntity vm = ApiPartialEntity.newBuilder()
                .setOid(2L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        Mockito.when(repoBackend.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyType(TopologyType.SOURCE)
                        .setReturnType(Type.FULL)
                        .addEntityOids(1L)
                        .setTopologyContextId(realtimeContextId)
                        .addEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .build()))
                .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder().setFullEntity(businessAccount))
                        .build()));
        Mockito.when(repoBackend.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyType(TopologyType.SOURCE)
                        .setReturnType(Type.API)
                        .addEntityOids(1L)
                        .addEntityOids(2L)
                        .setTopologyContextId(realtimeContextId)
                        .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build()))
                .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder().setApi(vm))
                        .build()));
        final BusinessUnitApiDTO buDto = new BusinessUnitApiDTO();
        buDto.setUuid("1");
        final ServiceEntityApiDTO vmDto = new ServiceEntityApiDTO();
        vmDto.setUuid("2");
        Mockito.when(
                businessAccountMapper.convert(Collections.singletonList(businessAccount), false))
                .thenReturn(Collections.singletonList(buDto));
        Mockito.when(serviceEntityMapper.toServiceEntityApiDTO(Collections.singletonList(vm)))
                .thenReturn(Collections.singletonList(vmDto));
        final RepositoryRequestResult repositoryResult =
                repositoryApi.getByIds(Arrays.asList(1L, 2L),
                        EnumSet.of(EntityType.BUSINESS_ACCOUNT, EntityType.VIRTUAL_MACHINE), false);
        Mockito.verify(repoBackend, Mockito.times(2)).retrieveTopologyEntities(Mockito.any());
        Assert.assertEquals(1, repositoryResult.getBusinessAccounts().size());
        Assert.assertEquals(1, repositoryResult.getServiceEntities().size());
        final BusinessUnitApiDTO buReturned =
                repositoryResult.getBusinessAccounts().iterator().next();
        Assert.assertEquals("1", buReturned.getUuid());
        final ServiceEntityApiDTO vmReturned =
                repositoryResult.getServiceEntities().iterator().next();
        Assert.assertEquals("2", vmReturned.getUuid());
    }

    /**
     * Tests retrieving only service entities by OIDs.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testGetByIdsServiceEntities() throws Exception {
        final ApiPartialEntity vm = ApiPartialEntity.newBuilder()
                .setOid(2L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        Mockito.when(repoBackend.retrieveTopologyEntities(Mockito.any()))
                .thenReturn(Collections.emptyList());
        Mockito.when(repoBackend.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyType(TopologyType.SOURCE)
                        .setReturnType(Type.API)
                        .addEntityOids(1L)
                        .addEntityOids(2L)
                        .setTopologyContextId(realtimeContextId)
                        .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build()))
                .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder().setApi(vm))
                        .build()));
        final ServiceEntityApiDTO vmDto = new ServiceEntityApiDTO();
        vmDto.setUuid("2");
        Mockito.when(serviceEntityMapper.toServiceEntityApiDTO(Collections.singletonList(vm)))
                .thenReturn(Collections.singletonList(vmDto));
        final RepositoryRequestResult repositoryResult =
                repositoryApi.getByIds(Arrays.asList(1L, 2L),
                        EnumSet.of(EntityType.BUSINESS_ACCOUNT, EntityType.VIRTUAL_MACHINE), false);
        Mockito.verify(repoBackend, Mockito.times(2)).retrieveTopologyEntities(Mockito.any());
        Assert.assertEquals(0, repositoryResult.getBusinessAccounts().size());
        Assert.assertEquals(1, repositoryResult.getServiceEntities().size());
        final ServiceEntityApiDTO vmReturned =
                repositoryResult.getServiceEntities().iterator().next();
        Assert.assertEquals("2", vmReturned.getUuid());
    }

    /**
     * Tests retrieving for empty collection of OIDs.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testGetByIdEmptyOidsRequest() throws Exception {
        final RepositoryRequestResult result =
                repositoryApi.getByIds(Collections.emptySet(), EnumSet.allOf(EntityType.class),
                        false);
        Assert.assertEquals(Collections.emptySet(), new HashSet<>(result.getServiceEntities()));
        Assert.assertEquals(Collections.emptySet(), new HashSet<>(result.getBusinessAccounts()));
        Mockito.verifyZeroInteractions(repoBackend);
    }

    /**
     * Tests retrieving business accounts from repository without specifying any entity types.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testGetBusinessAccountsWithoutEntityTypes() throws Exception {
        final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build();
        final ApiPartialEntity buServiceEntity = ApiPartialEntity.newBuilder()
                .setOid(1L)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build();

        Mockito.when(repoBackend.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyType(TopologyType.SOURCE)
                        .setReturnType(Type.FULL)
                        .addEntityOids(1L)
                        .addEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .setTopologyContextId(realtimeContextId)
                        .build()))
                .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder().setFullEntity(businessAccount))
                        .build()));
        Mockito.when(repoBackend.retrieveTopologyEntities(
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyType(TopologyType.SOURCE)
                        .setReturnType(Type.API)
                        .addEntityOids(1L)
                        .setTopologyContextId(realtimeContextId)
                        .build()))
                .thenReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
                        .addEntities(PartialEntity.newBuilder().setApi(buServiceEntity))
                        .build()));
        final BusinessUnitApiDTO buDto = new BusinessUnitApiDTO();
        buDto.setUuid("1");
        Mockito.when(
                businessAccountMapper.convert(Collections.singletonList(businessAccount), false))
                .thenReturn(Collections.singletonList(buDto));
        final RepositoryRequestResult repositoryResult =
                repositoryApi.getByIds(Arrays.asList(1L), Collections.emptySet(), false);
        Mockito.verify(serviceEntityMapper, Mockito.never())
                .toServiceEntityApiDTO(Mockito.any(ApiPartialEntity.class));
        Mockito.verify(serviceEntityMapper, Mockito.never())
                .toServiceEntityApiDTO(Mockito.any(TopologyEntityDTO.class));
        Mockito.verify(repoBackend, Mockito.times(2)).retrieveTopologyEntities(Mockito.any());
        Assert.assertEquals(1, repositoryResult.getBusinessAccounts().size());
        Assert.assertEquals(Collections.emptySet(),
                new HashSet<>(repositoryResult.getServiceEntities()));
        final BusinessUnitApiDTO buReturned =
                repositoryResult.getBusinessAccounts().iterator().next();
        Assert.assertEquals("1", buReturned.getUuid());
    }

    /**
     * Test expanding service providers to connected regions.
     */
    @Test
    public void testExpandServiceProvidersToRegions() {
        final RepositoryApi repositoryApi1 = Mockito.spy(repositoryApi);
        final Set<Long> serviceProviders = ImmutableSet.of(1L, 2L);
        final Set<Long> regionIdsSet = ImmutableSet.of(7L);
        final MultiEntityRequest multiEntityRequest = mock(MultiEntityRequest.class);
        Mockito.when(multiEntityRequest.getEntitiesWithConnections())
                .thenReturn(serviceProviders.stream()
                        .map(oid -> EntityWithConnections.newBuilder()
                                .setOid(oid)
                                .addConnectedEntities(ConnectedEntity.newBuilder()
                                        .setConnectedEntityId(7L)
                                        .setConnectedEntityType(EntityType.REGION_VALUE)
                                        .setConnectionType(ConnectionType.OWNS_CONNECTION))
                                .build()));
        Mockito.doReturn(multiEntityRequest).when(repositoryApi1).entitiesRequest(serviceProviders);
        Assert.assertEquals(regionIdsSet, repositoryApi1.expandServiceProvidersToRegions(serviceProviders));
    }

    /**
     * Tests {@link SearchRequest#getPaginatedSEList}.
     * @throws InterruptedException if current thread has been interrupted while pupulating
     *         severity data
     * @throws ConversionException if error faced during severity population
     */
    @Test
    public void testSearchRequestGetPaginatedSEList() throws InterruptedException, ConversionException {
        //GIVEN
        Search.SearchParameters build = SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.discoveredBy(5L))
                .build();
        RepositoryApi.SearchRequest searchRequest = repositoryApi.newSearchRequest(build);

        searchRequest.usePriceIndexPopulator(true);

        final String cursor = "cursor";
        int totalRecordCount = 100;
        PaginationResponse paginationResponse = PaginationResponse.newBuilder().setNextCursor(
                cursor)
                .setTotalRecordCount(totalRecordCount)
                .build();
        ApiPartialEntity apiPartialEntity = ApiPartialEntity.newBuilder()
                .setOid(123L)
                .setEntityType(1)
                .build();

        PartialEntity partialEntity = PartialEntity.newBuilder()
                .setApi(apiPartialEntity)
                .build();
        final List emptyList = Collections.singletonList(partialEntity);

        SearchEntitiesResponse grpcResponse = SearchEntitiesResponse.newBuilder()
                .setPaginationResponse(paginationResponse)
                .addAllEntities(emptyList)
                .build();

        Mockito.doReturn(grpcResponse)
                .when(searchBackend)
                .searchEntities(org.mockito.Matchers.any());
        List<ServiceEntityApiDTO> serviceEntityApiDTOList = Collections.singletonList(new ServiceEntityApiDTO());
        doReturn(serviceEntityApiDTOList).when(serviceEntityMapper).toServiceEntityApiDTO(Collections.singletonList(apiPartialEntity));
        PaginationParameters paginationParameters = PaginationParameters.getDefaultInstance();

        //WHEN
        ResponseEntity<List<ServiceEntityApiDTO>> response = searchRequest.getPaginatedSEList(paginationParameters);

        //THEN
        assertTrue(response.getHeaders().get("X-Next-Cursor").get(0).equals(cursor));
        assertTrue(response.getHeaders().get("X-Total-Record-Count").get(0).equals(String.valueOf(totalRecordCount)));
        assertEquals(response.getBody(), serviceEntityApiDTOList);
        verify(this.serviceEntityMapper).toServiceEntityApiDTO(Collections.singletonList(apiPartialEntity));
        //Checks that severity information populated to dtos
        verify(this.severityPopulator).populate(this.realtimeContextId, serviceEntityApiDTOList);
        verify(priceIndexPopulatorMock, times(1)).populateRealTimeEntities(serviceEntityApiDTOList);
    }

    /**
     * Tests {@link PaginatedSearchRequest#getResponse()} setting up rpc call correctly.
     * @throws Exception something wrong
     */
    @Test
    public void testPaginatedSearchRequestGetResponseCreatesRpcRequest() throws Exception {
        //GIVEN
        final SearchQuery searchQuery = SearchQuery.getDefaultInstance();
        final Set<Long> scopeIds = Collections.EMPTY_SET;
        final SearchPaginationRequest searchPaginationRequest =
                        new SearchPaginationRequest(null, null, true, null);
        final PaginatedSearchRequest paginatedSearchRequest =
                        repositoryApi.newPaginatedSearch(searchQuery, scopeIds, searchPaginationRequest);
        final PaginationParameters paginationParameters = PaginationParameters.getDefaultInstance();
        doReturn(PaginationParameters.getDefaultInstance()).when(paginationMapper).toProtoParams(searchPaginationRequest);
        //WHEN
        paginatedSearchRequest.getResponse();

        //THEN
        ArgumentCaptor<SearchEntitiesRequest> searchEntitiesRequestArgumentCaptor =
                        ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        verify(searchBackend).searchEntities(searchEntitiesRequestArgumentCaptor.capture());

        final SearchEntitiesRequest searchEntitiesRequest = searchEntitiesRequestArgumentCaptor.getValue();
        assertEquals(searchQuery, searchEntitiesRequest.getSearch());
        assertEquals(Type.API, searchEntitiesRequest.getReturnType());
        assertEquals(scopeIds, new HashSet<Long>(searchEntitiesRequest.getEntityOidList()));
        assertEquals(paginationParameters, searchEntitiesRequest.getPaginationParams());
    }


    /**
     * Tests {@link PaginatedSearchRequest#getResponse()} setting up rpc with full aspects.
     * @throws Exception something wrong
     */
    @Test
    public void testPaginatedSearchRequestGetResponseCreatesRpcRequestWithFullAspects() throws Exception {
        //GIVEN
        final SearchQuery searchQuery = SearchQuery.getDefaultInstance();
        final Set<Long> scopeIds = Collections.EMPTY_SET;
        final SearchPaginationRequest searchPaginationRequest =
                        new SearchPaginationRequest(null, null, true, null);
        final PaginatedSearchRequest paginatedSearchRequest =
                        repositoryApi.newPaginatedSearch(searchQuery, scopeIds, searchPaginationRequest);

        //Set aspects
        final List<String> aspectNames = Collections.singletonList("aspects");
        paginatedSearchRequest.requestAspects(mock(EntityAspectMapper.class), aspectNames);

        doReturn(PaginationParameters.getDefaultInstance()).when(paginationMapper).toProtoParams(searchPaginationRequest);
        //WHEN
        paginatedSearchRequest.getResponse();

        //THEN
        ArgumentCaptor<SearchEntitiesRequest> searchEntitiesRequestArgumentCaptor =
                        ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        verify(searchBackend).searchEntities(searchEntitiesRequestArgumentCaptor.capture());

        final SearchEntitiesRequest searchEntitiesRequest = searchEntitiesRequestArgumentCaptor.getValue();
        assertEquals(Type.FULL, searchEntitiesRequest.getReturnType());
    }

    /**
     * Tests {@link PaginatedSearchRequest#getResponse()} mapping results based on full aspects.
     * @throws Exception something wrong
     */
    @Test
    public void testPaginatedSearchRequestGetResponseMapResultsWithAspects() throws Exception {
        //GIVEN
        final SearchQuery searchQuery = SearchQuery.getDefaultInstance();
        final Set<Long> scopeIds = Collections.EMPTY_SET;
        final SearchPaginationRequest searchPaginationRequest =
                        new SearchPaginationRequest(null, null, true, null);
        final PaginatedSearchRequest paginatedSearchRequest =
                        repositoryApi.newPaginatedSearch(searchQuery, scopeIds, searchPaginationRequest);

        //Set aspects
        final List<String> aspectNames = Collections.singletonList("aspects");
        paginatedSearchRequest.requestAspects(mock(EntityAspectMapper.class), aspectNames);

        doReturn(PaginationParameters.getDefaultInstance())
                        .when(paginationMapper).toProtoParams(searchPaginationRequest);

        //WHEN
        paginatedSearchRequest.getResponse();

        //THEN
        verify(serviceEntityMapper, times(1))
                        .entitiesWithAspects(any(), any(), Mockito.eq(aspectNames));
    }

    /**
     * Tests {@link PaginatedSearchRequest#getResponse()} mapping results based on partial aspects.
     * @throws Exception something wrong
     */
    @Test
    public void testPaginatedSearchRequestGetResponseMapResultsWithoutAspects() throws Exception {
        //GIVEN
        final SearchQuery searchQuery = SearchQuery.getDefaultInstance();
        final Set<Long> scopeIds = Collections.EMPTY_SET;
        final SearchPaginationRequest searchPaginationRequest =
                        new SearchPaginationRequest(null, null, true, null);
        final PaginatedSearchRequest paginatedSearchRequest =
                        repositoryApi.newPaginatedSearch(searchQuery, scopeIds, searchPaginationRequest);

        doReturn(PaginationParameters.getDefaultInstance())
                        .when(paginationMapper).toProtoParams(searchPaginationRequest);

        //WHEN
        paginatedSearchRequest.getResponse();

        //THEN
        verify(serviceEntityMapper, times(1)).toServiceEntityApiDTOMap(any());
    }

    /**
     * Tests {@link SearchRequest#getPaginatedSEList}.
     * @throws InterruptedException if current thread has been interrupted while pupulating
     *         severity data
     * @throws InvalidOperationException if error faced during creation of the Pagination Request.
     */
    @Test
    public void testGetBusinessUnitsResponse() throws InvalidOperationException {
        //GIVEN
        final SearchQuery searchQuery = SearchQuery.getDefaultInstance();
        final Set<Long> scopeIds = Collections.EMPTY_SET;
        final SearchPaginationRequest paginationRequest = new SearchPaginationRequest(null, null,
                true, null);
        final String cursor = "cursor";
        int totalRecordCount = 100;
        long businessUnitOid = 123L;
        PaginationResponse paginationResponse = PaginationResponse.newBuilder().setNextCursor(
                cursor)
                .setTotalRecordCount(totalRecordCount)
                .build();
        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setOid(businessUnitOid)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build();
        PartialEntity partialEntity = PartialEntity.newBuilder()
                .setFullEntity(topologyEntityDTO)
                .build();
        SearchEntitiesResponse grpcResponse = SearchEntitiesResponse.newBuilder()
                .setPaginationResponse(paginationResponse)
                .addEntities(partialEntity)
                .build();
        Mockito.doReturn(grpcResponse)
                .when(searchBackend)
                .searchEntities(org.mockito.Matchers.any());
        BusinessUnitApiDTO apiDTO = new BusinessUnitApiDTO();
        apiDTO.setUuid(String.valueOf(businessUnitOid));
        List<BusinessUnitApiDTO> businessUnitApiDTOList = Collections.singletonList(apiDTO);
        doReturn(businessUnitApiDTOList).when(businessAccountMapper).convert(any(), anyBoolean());
        Mockito.when(paginationMapper.toProtoParams(paginationRequest)).thenCallRealMethod();

        //WHEN
        PaginatedSearchRequest paginatedRequest = repositoryApi.newPaginatedSearch(searchQuery, scopeIds, paginationRequest);
        ResponseEntity<List<BaseApiDTO>> response = paginatedRequest.getBusinessUnitsResponse(true).getRestResponse();

        //THEN
        assertTrue(response.getHeaders().get("X-Next-Cursor").get(0).equals(cursor));
        assertTrue(response.getHeaders().get("X-Total-Record-Count").get(0).equals(String.valueOf(totalRecordCount)));
        assertEquals(response.getBody(), businessUnitApiDTOList);
    }
}
