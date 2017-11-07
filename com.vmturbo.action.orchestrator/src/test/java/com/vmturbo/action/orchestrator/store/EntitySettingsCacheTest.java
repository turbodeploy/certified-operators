package com.vmturbo.action.orchestrator.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Status;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;

public class EntitySettingsCacheTest {
    private static final long TOPOLOGY_ID = 7L;
    private static final long TOPOLOGY_CONTEXT_ID = 77L;

    private SettingPolicyServiceMole spServiceSpy = spy(new SettingPolicyServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(spServiceSpy);

    private EntitySettingsCache entitySettingsCache;

    @Before
    public void setup() {
        entitySettingsCache = new EntitySettingsCache(grpcTestServer.getChannel());
    }

    @Test
    public void testRefresh() {
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName("foo")
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build();

        when(spServiceSpy.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setTopologySelection(TopologySelection.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                        .setTopologyId(TOPOLOGY_ID))
                .setSettingFilter(EntitySettingFilter.newBuilder()
                        .addEntities(1L))
                .build()))
            .thenReturn(GetEntitySettingsResponse.newBuilder()
                    .addSettings(SettingsForEntity.newBuilder()
                            .setEntityId(1L)
                            .addSettings(setting))
                    .build());

        entitySettingsCache.update(Collections.singleton(1L), TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        final List<Setting> newSettings = entitySettingsCache.getSettingsForEntity(1L);
        assertThat(newSettings, containsInAnyOrder(setting));
    }

    @Test
    public void testGetEmpty() {
        assertTrue(entitySettingsCache.getSettingsForEntity(1L).isEmpty());
    }

    @Test
    public void testRefreshError() {
        when(spServiceSpy.getEntitySettingsError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));

        entitySettingsCache.update(Collections.singleton(1L), TOPOLOGY_CONTEXT_ID, TOPOLOGY_ID);

        assertTrue(entitySettingsCache.getSettingsForEntity(1L).isEmpty());
    }
}
