package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingToPolicyName;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link EntitiesCache}.
 */
public class EntitiesCacheTest {
    private static final long TOPOLOGY_ID = 7L;
    private static final long TOPOLOGY_CONTEXT_ID = 77L;
    private static final long ENTITY_ID = 1L;
    private static final EntityType VM_ENTITY_TYPE = EntityType.VIRTUAL_MACHINE;
    private static final String VM_CLASSIC_ENTITY_TYPE = "VirtualMachine";

    private final HttpEntity<Set<Long>> httpEntity = new HttpEntity<>(Collections.singleton(ENTITY_ID));
    private final ParameterizedTypeReference<List<ServiceEntityApiDTO>> type =
            new ParameterizedTypeReference<List<ServiceEntityApiDTO>>() {};

    private SettingPolicyServiceMole spServiceSpy = spy(new SettingPolicyServiceMole());
    private RepositoryServiceMole repoServiceSpy = spy(new RepositoryServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(spServiceSpy);

    @Rule
    public GrpcTestServer groupTestServer = GrpcTestServer.newServer(spServiceSpy);

    @Rule
    public GrpcTestServer repoTestServer = GrpcTestServer.newServer(repoServiceSpy);

    private EntitiesCache entitySettingsCache;

    private RestTemplate restTemplate = mock(RestTemplate.class);

    @Before
    public void setup() {
        entitySettingsCache = new EntitiesCache(grpcTestServer.getChannel(), repoTestServer.getChannel());

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
        entityDto.setClassName(VM_CLASSIC_ENTITY_TYPE);

        when(spServiceSpy.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setTopologySelection(TopologySelection.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(TOPOLOGY_ID))
                .setSettingFilter(EntitySettingFilter.newBuilder()
                        .addEntities(ENTITY_ID))
                .build()))
            .thenReturn(Collections.singletonList(GetEntitySettingsResponse.newBuilder()
                    .addSettings(SettingsForEntity.newBuilder()
                            .setEntityId(ENTITY_ID)
                            .addSettings(SettingToPolicyName.newBuilder()
                                    .setSetting(setting)
                                    .build()))
                    .build()));

        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), eq(httpEntity), eq(type)))
                .thenReturn(ResponseEntity.ok(Collections.singletonList(entityDto)));


        entitySettingsCache.update(Collections.singleton(ENTITY_ID), TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        final Map<String, Setting> newSettings = entitySettingsCache.getSettingsForEntity(ENTITY_ID);
        assertTrue(newSettings.containsKey(setting.getSettingSpecName()));
        assertThat(newSettings.get(setting.getSettingSpecName()), is(setting));
    }

    @Test
    public void testGetEmpty() {
        assertTrue(entitySettingsCache.getSettingsForEntity(ENTITY_ID).isEmpty());
    }

    @Test
    public void testRefreshError() {
        when(spServiceSpy.getEntitySettingsError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));

        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), eq(httpEntity), eq(type)))
                .thenThrow(new RestClientException("NO!"));

        entitySettingsCache.update(Collections.singleton(ENTITY_ID), TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        assertTrue(entitySettingsCache.getSettingsForEntity(ENTITY_ID).isEmpty());
    }
}
