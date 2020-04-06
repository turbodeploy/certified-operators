package com.vmturbo.api.component.communication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import io.grpc.ManagedChannel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.communication.RepositoryApi.RepositoryRequestResult;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.businessaccount.BusinessAccountMapper;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.EntityCountResponse;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class RepositoryApiTest {

    private static final SearchParameters SEARCH_PARAMS =
        SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(1)).build();

    private final long realtimeContextId = 777777L;

    private SeverityPopulator severityPopulator = mock(SeverityPopulator.class);

    private SearchServiceMole searchBackend = spy(new SearchServiceMole());

    private RepositoryServiceMole repoBackend = spy(new RepositoryServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(searchBackend, repoBackend);

    private ServiceEntityMapper serviceEntityMapper = mock(ServiceEntityMapper.class);

    private RepositoryApi repositoryApi;
    private BusinessAccountMapper businessAccountMapper;

    @Before
    public void setup() {
        this.businessAccountMapper = Mockito.mock(BusinessAccountMapper.class);
        final ManagedChannel grpcChannel = grpcTestServer.getChannel();
        repositoryApi = new RepositoryApi(severityPopulator,
            RepositoryServiceGrpc.newBlockingStub(grpcChannel),
            RepositoryServiceGrpc.newStub(grpcChannel),
            SearchServiceGrpc.newBlockingStub(grpcChannel),
            SearchServiceGrpc.newStub(grpcChannel),
            serviceEntityMapper,
            businessAccountMapper,
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
        MinimalEntity ret = minimal(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setMinimal(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entityRequest(7L).getMinimalEntity().get(), is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getReturnType(), is(Type.MINIMAL));
    }

    @Test
    public void testGetEntity() {
        // arrange
        final ApiPartialEntity ret = entity(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());

        // act
        final ApiPartialEntity response = repositoryApi.entityRequest(7L).getEntity().get();

        // assert
        assertThat(response, is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getReturnType(), is(Type.API));
    }

    @Test
    public void testGetEntityProjected() {
        // arrange
        final ApiPartialEntity ret = entity(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());

        // act
        final ApiPartialEntity response = repositoryApi.entityRequest(7L)
            .projectedTopology()
            .getEntity()
            .get();

        // assert
        assertThat(response, is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getTopologyType(), is(TopologyType.PROJECTED));
        assertThat(req.getReturnType(), is(Type.API));
    }

    @Test
    public void testGetEntityPlanTargetsProjected() {
        // arrange
        final ApiPartialEntity ret = entity(7L);
        final long planContextId = realtimeContextId + 1;

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());

        // act
        final ApiPartialEntity response = repositoryApi.entityRequest(7L)
            .projectedTopology()
            .contextId(planContextId)
            .getEntity()
            .get();

        // assert
        assertThat(response, is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(planContextId));
        // Should still look in the projected topology!
        assertThat(req.getTopologyType(), is(TopologyType.PROJECTED));
        assertThat(req.getReturnType(), is(Type.API));
    }

    @Test
    public void testGetEntityPlanTargetsSource() {
        // arrange
        final ApiPartialEntity ret = entity(7L);
        final long planContextId = realtimeContextId + 1;

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());

        // act
        final ApiPartialEntity response = repositoryApi.entityRequest(7L)
            .contextId(planContextId)
            .getEntity()
            .get();

        // assert
        assertThat(response, is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(planContextId));
        // Should still look in the projected topology!
        assertThat(req.getTopologyType(), is(TopologyType.SOURCE));
        assertThat(req.getReturnType(), is(Type.API));
    }

    @Test
    public void testGetFullEntity() {
        final TopologyEntityDTO ret = full(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setFullEntity(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entityRequest(7L).getFullEntity().get(), is(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getReturnType(), is(Type.FULL));
    }

    @Test
    public void testGetSE() throws Exception {
        ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        when(serviceEntityMapper.toServiceEntityApiDTO(any(ApiPartialEntity.class))).thenReturn(se);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entityRequest(7L)
            .getSE().get(), is(se));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getTopologyType(), is(TopologyType.SOURCE));
        assertThat(req.getReturnType(), is(Type.API));

        verify(serviceEntityMapper).toServiceEntityApiDTO(ret);
    }

    @Test
    public void testGetSEUseAspectMapper() throws Exception {
        EntityAspectMapper aspectMapper = mock(EntityAspectMapper.class);

        ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final TopologyEntityDTO ret = full(7L);
        when(serviceEntityMapper.toServiceEntityApiDTO(any(TopologyEntityDTO.class))).thenReturn(se);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setFullEntity(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entityRequest(7L)
            .useAspectMapper(aspectMapper, Collections.singletonList(AspectName.CLOUD.getApiName()))
            .getSE().get(), is(se));
        assertThat(se.getAspects(), is(nullValue()));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getTopologyType(), is(TopologyType.SOURCE));
        assertThat(req.getReturnType(), is(Type.FULL));

        verify(serviceEntityMapper).toServiceEntityApiDTO(ret);
        verify(severityPopulator).populate(realtimeContextId, Collections.singletonList(se));
    }

    @Test
    public void testGetUseContext() throws Exception {
        ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        when(serviceEntityMapper.toServiceEntityApiDTO(any(ApiPartialEntity.class))).thenReturn(se);

        final long contextId = 123;

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entityRequest(7L)
            .contextId(contextId)
            .getSE().get(), is(se));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getTopologyContextId(), is(contextId));

        verify(serviceEntityMapper).toServiceEntityApiDTO(ret);
        verify(severityPopulator).populate(contextId, Collections.singletonList(se));
    }

    @Test
    public void testMultiGetMinimalEntity() throws Exception {
        MinimalEntity ret = minimal(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setMinimal(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
            .getMinimalEntities()
            .collect(Collectors.toList()), contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getReturnType(), is(Type.MINIMAL));
    }

    @Test
    public void testMultiGetEntity() {
        final ApiPartialEntity ret = entity(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
            .getEntities()
            .collect(Collectors.toList()), contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getReturnType(), is(Type.API));
    }

    @Test
    public void testMultiGetEntityProjected() {
        final ApiPartialEntity ret = entity(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
            .projectedTopology()
            .getEntities().collect(Collectors.toList()), contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getTopologyType(), is(TopologyType.PROJECTED));
        assertThat(req.getReturnType(), is(Type.API));
    }

    @Test
    public void testMultiGetFullEntity() {
        final TopologyEntityDTO ret = full(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setFullEntity(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
            .getFullEntities()
            .collect(Collectors.toList()), contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getReturnType(), is(Type.FULL));
    }

    @Test
    public void testMultiGetNoAllowGetAll() {
        final TopologyEntityDTO ret = full(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setFullEntity(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entitiesRequest(Collections.emptySet())
            // Missing allow empty to mean "get all".
            .getFullEntities()
            .collect(Collectors.toList()), is(Collections.emptyList()));

        verify(repoBackend, never()).retrieveTopologyEntities(any());
    }

    @Test
    public void testMultiGetAllowGetAll() {
        final TopologyEntityDTO ret = full(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setFullEntity(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entitiesRequest(Collections.emptySet())
            // Allow empty to mean "get all".
            .allowGetAll()
            .getFullEntities()
            .collect(Collectors.toList()), contains(ret));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), is(Collections.emptyList()));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getReturnType(), is(Type.FULL));
    }

    @Test
    public void testMultiGetSEList() throws Exception {
        ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        when(serviceEntityMapper.toServiceEntityApiDTO(any(ApiPartialEntity.class))).thenReturn(se);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
            .getSEList(), contains(se));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getTopologyType(), is(TopologyType.SOURCE));
        assertThat(req.getReturnType(), is(Type.API));

        verify(serviceEntityMapper).toServiceEntityApiDTO(ret);
        verify(severityPopulator).populate(realtimeContextId, Collections.singletonList(se));
    }

    @Test
    public void testMultiGetSEMap() throws Exception {
        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        when(serviceEntityMapper.toServiceEntityApiDTOMap(Collections.singletonList(ret)))
            .thenReturn(Collections.singletonMap(ret.getOid(), se));

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
            .getSEMap(), is(ImmutableMap.of(7L, se)));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getTopologyType(), is(TopologyType.SOURCE));
        assertThat(req.getReturnType(), is(Type.API));

        verify(serviceEntityMapper).toServiceEntityApiDTOMap(Collections.singletonList(ret));
        verify(severityPopulator).populate(realtimeContextId, Collections.singleton(se));
    }

    @Test
    public void testMultiGetSEUseAspectMapper() throws Exception {
        EntityAspectMapper aspectMapper = mock(EntityAspectMapper.class);

        final ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final TopologyEntityDTO ret = full(7L);
        when(serviceEntityMapper.toServiceEntityApiDTO(any(TopologyEntityDTO.class))).thenReturn(se);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setFullEntity(ret))
            .build())).when(repoBackend).retrieveTopologyEntities(any());
        assertThat(repositoryApi.entitiesRequest(Collections.singleton(7L))
            .useAspectMapper(aspectMapper)
            .getSEList(), contains(se));

        final ArgumentCaptor<RetrieveTopologyEntitiesRequest> captor =
            ArgumentCaptor.forClass(RetrieveTopologyEntitiesRequest.class);
        verify(repoBackend).retrieveTopologyEntities(captor.capture());
        RetrieveTopologyEntitiesRequest req = captor.getValue();
        assertThat(req.getEntityOidsList(), contains(7L));
        assertThat(req.getTopologyContextId(), is(realtimeContextId));
        assertThat(req.getTopologyType(), is(TopologyType.SOURCE));
        assertThat(req.getReturnType(), is(Type.FULL));

        verify(serviceEntityMapper).toServiceEntityApiDTO(ret);
        verify(severityPopulator).populate(realtimeContextId, Collections.singletonList(se));
    }

    @Test
    public void testSearchMinimalEntity() {
        MinimalEntity ret = minimal(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setMinimal(ret))
            .build())).when(searchBackend).searchEntitiesStream(any());
        assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
            .getMinimalEntities()
            .collect(Collectors.toList()), contains(ret));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
            ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        verify(searchBackend).searchEntitiesStream(captor.capture());
        SearchEntitiesRequest req = captor.getValue();
        assertThat(req.getSearchParametersList(), contains(SEARCH_PARAMS));
        assertThat(req.getReturnType(), is(Type.MINIMAL));
    }

    @Test
    public void testSearchEntity() {
        final ApiPartialEntity ret = entity(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(searchBackend).searchEntitiesStream(any());
        assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
            .getEntities()
            .collect(Collectors.toList()), contains(ret));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
            ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        verify(searchBackend).searchEntitiesStream(captor.capture());
        SearchEntitiesRequest req = captor.getValue();
        assertThat(req.getSearchParametersList(), contains(SEARCH_PARAMS));
        assertThat(req.getReturnType(), is(Type.API));
    }

    @Test
    public void testSearchFullEntity() {
        final TopologyEntityDTO ret = full(7L);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setFullEntity(ret))
            .build())).when(searchBackend).searchEntitiesStream(any());
        assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
            .getFullEntities()
            .collect(Collectors.toList()), contains(ret));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
            ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        verify(searchBackend).searchEntitiesStream(captor.capture());
        SearchEntitiesRequest req = captor.getValue();
        assertThat(req.getSearchParametersList(), contains(SEARCH_PARAMS));
        assertThat(req.getReturnType(), is(Type.FULL));
    }

    @Test
    public void testSearchCount() {
        doReturn(EntityCountResponse.newBuilder()
            .setEntityCount(5)
            .build()).when(searchBackend).countEntities(any());

        assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
            .count(), is(5L));

        final ArgumentCaptor<CountEntitiesRequest> captor =
            ArgumentCaptor.forClass(CountEntitiesRequest.class);
        verify(searchBackend).countEntities(captor.capture());
        CountEntitiesRequest req = captor.getValue();
        assertThat(req.getSearchParametersList(), contains(SEARCH_PARAMS));
    }

    @Test
    public void testSearchOids() {
        doReturn(SearchEntityOidsResponse.newBuilder()
            .addEntities(7L)
            .build()).when(searchBackend).searchEntityOids(any());

        assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
            .getOids(), contains(7L));

        final ArgumentCaptor<SearchEntityOidsRequest> captor =
            ArgumentCaptor.forClass(SearchEntityOidsRequest.class);
        verify(searchBackend).searchEntityOids(captor.capture());
        SearchEntityOidsRequest req = captor.getValue();
        assertThat(req.getSearchParametersList(), contains(SEARCH_PARAMS));
    }

    @Test
    public void testSearchSEList() throws Exception {
        ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        when(serviceEntityMapper.toServiceEntityApiDTO(any(ApiPartialEntity.class))).thenReturn(se);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(searchBackend).searchEntitiesStream(any());
        assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
            .getSEList(), contains(se));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
            ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        verify(searchBackend).searchEntitiesStream(captor.capture());
        SearchEntitiesRequest req = captor.getValue();
        assertThat(req.getSearchParametersList(), contains(SEARCH_PARAMS));
        assertThat(req.getReturnType(), is(Type.API));

        verify(serviceEntityMapper).toServiceEntityApiDTO(ret);
        verify(severityPopulator).populate(realtimeContextId, Collections.singletonList(se));
    }

    @Test
    public void testSearchSEUseAspectMapper() throws Exception {
        EntityAspectMapper aspectMapper = mock(EntityAspectMapper.class);
        ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final TopologyEntityDTO ret = full(7L);
        when(serviceEntityMapper.toServiceEntityApiDTO(any(TopologyEntityDTO.class))).thenReturn(se);

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setFullEntity(ret))
            .build())).when(searchBackend).searchEntitiesStream(any());
        assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
            .useAspectMapper(aspectMapper)
            .getSEList(), contains(se));

        // Check to make sure we used the aspect mapper.
        verify(serviceEntityMapper).toServiceEntityApiDTO(ret);
        verify(aspectMapper).getAspectsByEntities(Collections.singletonList(ret), null);
        verify(severityPopulator).populate(realtimeContextId, Collections.singletonList(se));
    }

    @Test
    public void testSearchSEMap() throws Exception {
        ServiceEntityApiDTO se = new ServiceEntityApiDTO();
        se.setUuid("7");

        final ApiPartialEntity ret = entity(7L);
        when(serviceEntityMapper.toServiceEntityApiDTOMap(Collections.singletonList(ret)))
            .thenReturn(Collections.singletonMap(7L, se));

        doReturn(Collections.singletonList(PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setApi(ret))
            .build())).when(searchBackend).searchEntitiesStream(any());
        assertThat(repositoryApi.newSearchRequest(SEARCH_PARAMS)
            .getSEMap(), is(ImmutableMap.of(7L, se)));

        final ArgumentCaptor<SearchEntitiesRequest> captor =
            ArgumentCaptor.forClass(SearchEntitiesRequest.class);
        verify(searchBackend).searchEntitiesStream(captor.capture());
        SearchEntitiesRequest req = captor.getValue();
        assertThat(req.getSearchParametersList(), contains(SEARCH_PARAMS));
        assertThat(req.getReturnType(), is(Type.API));

        verify(serviceEntityMapper).toServiceEntityApiDTOMap(Collections.singletonList(ret));
        verify(severityPopulator).populate(realtimeContextId, Collections.singleton(se));
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
    public void testGetBusinessAccountsAndServiecEntities() throws Exception {
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
        Mockito.when(serviceEntityMapper.toServiceEntityApiDTO(vm)).thenReturn(vmDto);
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
        Mockito.when(serviceEntityMapper.toServiceEntityApiDTO(vm)).thenReturn(vmDto);
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
}
