package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import io.grpc.Status;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

public class EntitySettingsCacheTest {
    private static final long TOPOLOGY_ID = 7L;
    private static final long TOPOLOGY_CONTEXT_ID = 77L;
    private static final long ENTITY_ID = 1L;
    private static final EntityType ENTITY_TYPE = EntityType.VIRTUAL_MACHINE;

    private final HttpEntity<Set> httpEntity = new HttpEntity<>(Collections.singleton(ENTITY_ID));
    private final ParameterizedTypeReference<List<ServiceEntityApiDTO>> type =
            new ParameterizedTypeReference<List<ServiceEntityApiDTO>>() {};

    private SettingPolicyServiceMole spServiceSpy = spy(new SettingPolicyServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(spServiceSpy);

    private EntitySettingsCache entitySettingsCache;

    private RestTemplate restTemplate = mock(RestTemplate.class);

    @Before
    public void setup() {
        entitySettingsCache = new EntitySettingsCache(grpcTestServer.getChannel(), restTemplate,
                "test", 123, Executors.newSingleThreadExecutor(), 5, 1000);

        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), eq(httpEntity), eq(type)))
                .thenReturn(ResponseEntity.ok(Collections.emptyList()));

    }

    @Test
    public void testRefresh() {
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName("foo")
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build();

        final ServiceEntityApiDTO entityDto = new ServiceEntityApiDTO();
        entityDto.setUuid(Long.toString(ENTITY_ID));
        entityDto.setClassName(ENTITY_TYPE.name());

        when(spServiceSpy.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setTopologySelection(TopologySelection.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(TOPOLOGY_ID))
                .setSettingFilter(EntitySettingFilter.newBuilder()
                        .addEntities(ENTITY_ID))
                .build()))
            .thenReturn(GetEntitySettingsResponse.newBuilder()
                    .addSettings(SettingsForEntity.newBuilder()
                            .setEntityId(ENTITY_ID)
                            .addSettings(setting))
                    .build());

        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), eq(httpEntity), eq(type)))
                .thenReturn(ResponseEntity.ok(Collections.singletonList(entityDto)));


        entitySettingsCache.update(Collections.singleton(ENTITY_ID), TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        final List<Setting> newSettings = entitySettingsCache.getSettingsForEntity(ENTITY_ID);
        final Optional<EntityType> newType = entitySettingsCache.getTypeForEntity(ENTITY_ID);
        assertThat(newSettings, containsInAnyOrder(setting));
        assertTrue(newType.isPresent());
        assertEquals(newType.get(), ENTITY_TYPE);
    }

    @Test
    public void testGetEmpty() {
        assertTrue(entitySettingsCache.getSettingsForEntity(ENTITY_ID).isEmpty());
        assertFalse(entitySettingsCache.getTypeForEntity(ENTITY_ID).isPresent());
    }

    @Test
    public void testRefreshError() {
        when(spServiceSpy.getEntitySettingsError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));

        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), eq(httpEntity), eq(type)))
                .thenThrow(new RestClientException("NO!"));

        entitySettingsCache.update(Collections.singleton(ENTITY_ID), TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        assertTrue(entitySettingsCache.getSettingsForEntity(ENTITY_ID).isEmpty());
        assertFalse(entitySettingsCache.getTypeForEntity(ENTITY_ID).isPresent());
    }
}
