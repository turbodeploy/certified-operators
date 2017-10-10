package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;

public class SettingsServiceTest {

    private GrpcTestServer grpcServer;

    private TestSettingRpcService settingRpcServiceSpy = spy(new TestSettingRpcService());

    private SettingServiceBlockingStub settingServiceStub;

    private SettingsService settingsService;

    private SettingsMapper settingsMapper = mock(SettingsMapper.class);

    private final SettingSpec vmSettingSpec = SettingSpec.newBuilder()
            .setName("moveVM")
            .setDisplayName("Move")
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                    .setTiebreaker(SettingTiebreaker.SMALLER)
                    .setEntitySettingScope(EntitySettingScope.newBuilder()
                            .setEntityTypeSet(EntityTypeSet.newBuilder().addEntityType(10))))
            .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                    .addAllEnumValues(Arrays.asList("DISABLED", "MANUAL"))
                    .setDefault("MANUAL"))
            .build();

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        grpcServer = GrpcTestServer.withServices(settingRpcServiceSpy);
        settingServiceStub = SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());

        settingsService = new SettingsService(settingServiceStub, settingsMapper);

        when(settingRpcServiceSpy.getAllSettingSpec(any()))
                .thenReturn(Arrays.asList(vmSettingSpec));
    }

    @After
    public void teardown() {
        grpcServer.close();
    }

    @Captor
    private ArgumentCaptor<List<SettingSpec>> specCaptor;

    /**
     * Verify that a request without a specific manager UUID calls the appropriate mapping method.
     */
    @Test
    public void testGetAllSpecs() throws Exception {
        SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        mgrDto.setUuid("test");

        when(settingsMapper.toManagerDtos(any())).thenReturn(Collections.singletonList(mgrDto));

        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(null, null, false);
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).getUuid());

        verify(settingsMapper).toManagerDtos(specCaptor.capture());
        assertThat(specCaptor.getValue(),containsInAnyOrder(vmSettingSpec));
    }

    /**
     * Verify that a request for specs with a specific manager UUID calls the appropriate mapping
     * method.
     */
    @Test
    public void testGetSingleMgrSpecs() throws Exception {
        final SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        mgrDto.setUuid("test");

        final String mgrId = "mgrId";
        when(settingsMapper.toManagerDto(any(), eq(mgrId))).thenReturn(Optional.of(mgrDto));
        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(mgrId, null, false);
        assertEquals(1, result.size());
        assertEquals("test", result.get(0).getUuid());
        verify(settingsMapper).toManagerDto(specCaptor.capture(), eq(mgrId));
        assertThat(specCaptor.getValue(), containsInAnyOrder(vmSettingSpec));
    }

    /**
     * Verify that a request for specs with an entity type ignores specs for other entity types.
     */
    @Test
    public void testGetSingleEntityTypeSpecs() throws Exception {
        final SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        mgrDto.setUuid("test");
        when(settingsMapper.toManagerDtos(any())).thenReturn(Collections.singletonList(mgrDto));

        List<SettingsManagerApiDTO> result =
                settingsService.getSettingsSpecs(null, "Container", false);
        verify(settingsMapper).toManagerDtos(specCaptor.capture());
        assertTrue(specCaptor.getValue().isEmpty());
    }

    @Test
    public void testSettingMatchEntityType() {
        final SettingSpec allSettingSpec = SettingSpec.newBuilder()
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setAllEntityType(AllEntityType.getDefaultInstance())))
                .build();
        final SettingSpec vmSettingSpec = SettingSpec.newBuilder()
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                    .setEntitySettingScope(EntitySettingScope.newBuilder()
                            .setEntityTypeSet(EntityTypeSet.newBuilder()
                                    .addEntityType(10))))
            .build();

        assertTrue(SettingsService.settingMatchEntityType(vmSettingSpec, null));

        assertTrue(SettingsService.settingMatchEntityType(vmSettingSpec, "VirtualMachine"));

        assertTrue(SettingsService.settingMatchEntityType(allSettingSpec, "VirtualMachine"));

        assertFalse(SettingsService.settingMatchEntityType(
            SettingSpec.newBuilder()
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .build(),
            "VirtualMachine"));

        assertFalse(SettingsService.settingMatchEntityType(
            SettingSpec.newBuilder()
                .setEntitySettingSpec(EntitySettingSpec.getDefaultInstance())
                .build(), "VirtualMachine"));
    }

    private static class TestSettingRpcService extends SettingServiceImplBase {

        public List<SettingSpec> getAllSettingSpec(SearchSettingSpecsRequest request) {
            return Collections.emptyList();
        }

        @Override
        public void searchSettingSpecs(SearchSettingSpecsRequest request,
                                      StreamObserver<SettingSpec> responseObserver) {
            getAllSettingSpec(request).forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }
    }
}
