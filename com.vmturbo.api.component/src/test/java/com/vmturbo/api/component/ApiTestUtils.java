package com.vmturbo.api.component;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

public class ApiTestUtils {

    @Nonnull
    private static RepositoryApi.SingleEntityRequest mockSingleEntityRequest() {
        SingleEntityRequest req = mock(SingleEntityRequest.class);
        when(req.allowGetAll()).thenReturn(req);
        when(req.useAspectMapper(any())).thenReturn(req);
        when(req.contextId(any())).thenReturn(req);
        when(req.projectedTopology()).thenReturn(req);
        return req;
    }

    @Nonnull
    public static RepositoryApi.SingleEntityRequest mockSingleEntityEmptyRequest() {
        SingleEntityRequest req = mockSingleEntityRequest();
        when(req.getFullEntity()).thenReturn(Optional.empty());
        when(req.getEntity()).thenReturn(Optional.empty());
        when(req.getMinimalEntity()).thenReturn(Optional.empty());
        when(req.getSE()).thenReturn(Optional.empty());
        return req;
    }

    @Nonnull
    public static RepositoryApi.SingleEntityRequest mockSingleEntityRequest(@Nonnull final TopologyEntityDTO entity) {
        SingleEntityRequest req = mockSingleEntityRequest();
        when(req.getFullEntity()).thenReturn(Optional.of(entity));
        return req;
    }

    @Nonnull
    public static RepositoryApi.SingleEntityRequest mockSingleEntityRequest(@Nonnull final MinimalEntity entity) {
        SingleEntityRequest req = mockSingleEntityRequest();
        when(req.getMinimalEntity()).thenReturn(Optional.of(entity));
        return req;
    }

    @Nonnull
    public static RepositoryApi.SingleEntityRequest mockSingleEntityRequest(@Nonnull final ApiPartialEntity entity) {
        SingleEntityRequest req = mockSingleEntityRequest();
        when(req.getEntity()).thenReturn(Optional.of(entity));
        return req;
    }

    @Nonnull
    public static RepositoryApi.SingleEntityRequest mockSingleEntityRequest(@Nonnull final ServiceEntityApiDTO entity) {
        SingleEntityRequest req = mockSingleEntityRequest();
        when(req.getSE()).thenReturn(Optional.of(entity));
        return req;
    }

    @Nonnull
    public static RepositoryApi.SingleEntityRequest mockSingleEntityRequest(@Nonnull final EntityWithConnections entity) {
        SingleEntityRequest req = mockSingleEntityRequest();
        when(req.getEntityWithConnections()).thenReturn(Optional.of(entity));
        return req;
    }


    @Nonnull
    private static RepositoryApi.MultiEntityRequest mockMultiEntityRequest() {
        MultiEntityRequest req = mock(MultiEntityRequest.class);
        when(req.allowGetAll()).thenReturn(req);
        when(req.useAspectMapper(any())).thenReturn(req);
        when(req.contextId(any())).thenReturn(req);
        when(req.projectedTopology()).thenReturn(req);
        return req;
    }

    @Nonnull
    public static RepositoryApi.MultiEntityRequest mockMultiEntityReqEmpty() {
        MultiEntityRequest req = mockMultiEntityRequest();
        when(req.getFullEntities()).then(invocation -> Stream.empty());
        when(req.getSEList()).thenReturn(Collections.emptyList());
        when(req.getSEMap()).thenReturn(Collections.emptyMap());
        when(req.getMinimalEntities()).then(invocation -> Stream.empty());
        when(req.getEntities()).then(invocation -> Stream.empty());
        return req;
    }


    @Nonnull
    public static RepositoryApi.MultiEntityRequest mockMultiFullEntityReq(List<TopologyEntityDTO> entities) {
        MultiEntityRequest req = mockMultiEntityRequest();
        when(req.getFullEntities()).then(invocation -> entities.stream());
        return req;
    }

    @Nonnull
    public static RepositoryApi.MultiEntityRequest mockMultiMinEntityReq(List<MinimalEntity> entities) {
        MultiEntityRequest req = mockMultiEntityRequest();
        when(req.getMinimalEntities()).then(invocation -> entities.stream());
        return req;
    }

    @Nonnull
    public static RepositoryApi.MultiEntityRequest mockMultiEntityWithConnectionsReq(List<EntityWithConnections> entities) {
        MultiEntityRequest req = mockMultiEntityRequest();
        when(req.getEntitiesWithConnections()).then(invocation -> entities.stream());
        return req;
    }

    @Nonnull
    public static RepositoryApi.MultiEntityRequest mockMultiEntityReq(List<ApiPartialEntity> entities) {
        MultiEntityRequest req = mockMultiEntityRequest();
        when(req.getEntities()).then(invocation -> entities.stream());
        return req;
    }

    @Nonnull
    public static RepositoryApi.MultiEntityRequest mockMultiSEReq(List<ServiceEntityApiDTO> entities) {
        MultiEntityRequest req = mockMultiEntityRequest();
        when(req.getSEMap()).then(invocation -> entities.stream()
            .collect(Collectors.toMap(e -> Long.parseLong(e.getUuid()), Function.identity())));
        when(req.getSEList()).then(invocation -> entities);
        return req;
    }

    private static RepositoryApi.SearchRequest mockSearchReq() {
        SearchRequest req = mock(SearchRequest.class);
        when(req.useAspectMapper(any())).thenReturn(req);
        return req;
    }

    @Nonnull
    public static RepositoryApi.SearchRequest mockSearchMinReq(List<MinimalEntity> entities) {
        SearchRequest req = mockSearchReq();
        when(req.getMinimalEntities()).then(invocation -> entities.stream());
        return req;
    }

    @Nonnull
    public static RepositoryApi.SearchRequest mockSearchWithConnectionsReq(List<EntityWithConnections> entities) {
        SearchRequest req = mockSearchReq();
        when(req.getEntitiesWithConnections()).then(invocation -> entities.stream());
        return req;
    }

    @Nonnull
    public static RepositoryApi.SearchRequest mockSearchFullReq(List<TopologyEntityDTO> entities) {
        SearchRequest req = mockSearchReq();
        when(req.getFullEntities()).then(invocation -> entities.stream());
        return req;
    }

    @Nonnull
    public static RepositoryApi.SearchRequest mockSearchReq(List<ApiPartialEntity> entities) {
        SearchRequest req = mockSearchReq();
        when(req.getEntities()).then(invocation -> entities.stream());
        return req;
    }

    @Nonnull
    public static RepositoryApi.SearchRequest mockEmptySearchReq() {
        SearchRequest req = mockSearchReq();
        when(req.getEntities()).then(invocation -> Stream.empty());
        when(req.getMinimalEntities()).then(invocation -> Stream.empty());
        when(req.getFullEntities()).then(invocation -> Stream.empty());
        when(req.getOids()).thenReturn(Collections.emptySet());
        when(req.count()).thenReturn(0L);
        return req;
    }

    @Nonnull
    public static RepositoryApi.SearchRequest mockSearchCountReq(long count) {
        SearchRequest req = mockSearchReq();
        when(req.count()).thenReturn(count);
        return req;
    }

    @Nonnull
    public static RepositoryApi.SearchRequest mockSearchIdReq(Set<Long> oids) {
        SearchRequest req = mockSearchReq();
        when(req.getOids()).thenReturn(oids);
        return req;
    }


    @Nonnull
    public static RepositoryApi.SearchRequest mockSearchSEReq(List<ServiceEntityApiDTO> entities) {
        SearchRequest req = mockSearchReq();
        when(req.getSEList()).thenReturn(entities);
        when(req.getSEMap()).thenReturn(entities.stream()
            .collect(Collectors.toMap(e -> Long.parseLong(e.getUuid()), Function.identity())));
        return req;
    }

    @Nonnull
    public static SupplyChainNodeFetcherBuilder mockNodeFetcherBuilder(
            @Nonnull final Map<String, SupplyChainNode> fetchResult,
            @Nonnull final Map<String, SupplyChainNode>... nextFetchResults) throws OperationFailedException {
        final SupplyChainNodeFetcherBuilder bldr = mock(SupplyChainNodeFetcherBuilder.class);
        when(bldr.apiEnvironmentType(any())).thenReturn(bldr);
        when(bldr.addSeedUuid(any())).thenReturn(bldr);
        when(bldr.environmentType(any())).thenReturn(bldr);
        when(bldr.addSeedUuids(any())).thenReturn(bldr);
        when(bldr.entityTypes(any())).thenReturn(bldr);
        when(bldr.topologyContextId(anyLong())).thenReturn(bldr);

        when(bldr.fetch()).thenReturn(fetchResult, nextFetchResults);
        return bldr;
    }

    @Nonnull
    public static SupplychainApiDTOFetcherBuilder mockApiDTOFetcherBuilder(
            @Nonnull final SupplychainApiDTO fetchResult,
            @Nonnull final SupplychainApiDTO... nextFetchResults) throws OperationFailedException, InterruptedException {
        final SupplychainApiDTOFetcherBuilder bldr = mock(SupplychainApiDTOFetcherBuilder.class);
        when(bldr.apiEnvironmentType(any())).thenReturn(bldr);
        when(bldr.addSeedUuid(any())).thenReturn(bldr);
        when(bldr.environmentType(any())).thenReturn(bldr);
        when(bldr.addSeedUuids(any())).thenReturn(bldr);
        when(bldr.entityTypes(any())).thenReturn(bldr);
        when(bldr.topologyContextId(anyLong())).thenReturn(bldr);

        when(bldr.entityDetailType(any())).thenReturn(bldr);
        when(bldr.includeHealthSummary(anyBoolean())).thenReturn(bldr);

        when(bldr.fetch()).thenReturn(fetchResult, nextFetchResults);
        return bldr;
    }

    @Nonnull
    public static ApiId mockRealtimeId(final String uuid, final long realtimeId) {
        return mockApiId(realtimeId, uuid , false, false, true, false, Optional.empty());
    }

    @Nonnull
    public static ApiId mockRealtimeId(final String uuid, final long realtimeId, @Nonnull final UuidMapper mockMapper) {
        return mockApiId(realtimeId, uuid , false, false, true, false, Optional.of(mockMapper));
    }

    @Nonnull
    public static ApiId mockPlanId(final String uuid) {
        return mockApiId(Long.valueOf(uuid), uuid, true, false, false, false, Optional.empty());
    }

    @Nonnull
    public static ApiId mockPlanId(final String uuid, @Nonnull final UuidMapper mockMapper) {
        return mockApiId(Long.valueOf(uuid), uuid, true, false, false, false, Optional.of(mockMapper));
    }

    @Nonnull
    public static ApiId mockGroupId(final String uuid) {
        return mockApiId(Long.valueOf(uuid), uuid, false, true, false, false, Optional.empty());
    }

    @Nonnull
    public static ApiId mockGroupId(final String uuid, @Nonnull final UuidMapper mockMapper) {
        return mockApiId(Long.valueOf(uuid), uuid, false, true, false, false, Optional.of(mockMapper));
    }

    @Nonnull
    public static ApiId mockEntityId(final String uuid) {
        return mockApiId(Long.valueOf(uuid), uuid, false, false, false, true, Optional.empty());
    }

    @Nonnull
    public static ApiId mockEntityId(final String uuid, @Nonnull final UuidMapper mockMapper) {
        return mockApiId(Long.valueOf(uuid), uuid, false, false, false, true, Optional.of(mockMapper));
    }

    @Nonnull
    private static ApiId mockApiId(final long id,
                                   final String uuid,
                                   final boolean isPlan,
                                   final boolean isGroup,
                                   final boolean isRealtime,
                                   final boolean isEntity,
                                   final Optional<UuidMapper> uuidMapperOpt) {
        final ApiId apiId = mock(ApiId.class);
        when(apiId.oid()).thenReturn(id);
        when(apiId.uuid()).thenReturn(uuid);
        when(apiId.isPlan()).thenReturn(isPlan);
        when(apiId.isGroup()).thenReturn(isGroup);
        when(apiId.isRealtimeMarket()).thenReturn(isRealtime);
        when(apiId.isEntity()).thenReturn(isEntity);

        uuidMapperOpt.ifPresent(uuidMapper -> {
            try {
                when(uuidMapper.fromUuid(uuid)).thenReturn(apiId);
            } catch (OperationFailedException e) {
                throw new IllegalStateException(e);
            }
            when(uuidMapper.fromOid(id)).thenReturn(apiId);
        });
        return apiId;
    }

}
