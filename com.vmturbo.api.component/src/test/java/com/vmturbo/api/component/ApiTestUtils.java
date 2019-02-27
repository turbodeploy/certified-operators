package com.vmturbo.api.component;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplyChainNodeFetcherBuilder;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;

public class ApiTestUtils {

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
            when(uuidMapper.fromUuid(uuid)).thenReturn(apiId);
            when(uuidMapper.fromOid(id)).thenReturn(apiId);
        });
        return apiId;
    }

}
