package com.vmturbo.topology.processor.probeproperties;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableList;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.mediation.common.PropertyKeyName.PropertyKeyType;

/**
 * Unit tests for GlobalProbePropertiesSettingsLoader.
 */
public class GlobalProbePropertiesSettingsLoaderTest {
    private SettingServiceMole settingsService;
    private SettingServiceBlockingStub settingsServiceClient;
    private GrpcTestServer grpcServer;

    /**
     * Set up the test.
     *
     * @throws IOException when failed
     */
    @Before
    public void setup() throws IOException {
        settingsService = Mockito.spy(new SettingServiceMole());
        grpcServer = GrpcTestServer.newServer(settingsService);
        grpcServer.start();
        settingsServiceClient = SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * Release the test resources.
     */
    @After
    public void shutdown() {
        grpcServer.close();
    }

    /**
     * Test that global property setting value is reloaded from a service upon error with correct value.
     * @throws InterruptedException when interrupted
     */
    @Test
    public void testSettingsRepeatedlyRequested() throws InterruptedException {
        List<Setting> settingResponse = ImmutableList.of(Setting.newBuilder()
                        .setSettingSpecName(
                                        GlobalSettingSpecs.OnPremVolumeAnalysis.getSettingName())
                        .setBooleanSettingValue(
                                        BooleanSettingValue.newBuilder().setValue(true).build())
                        .build());
        // first fail, then succeed
        Mockito.when(settingsService.getMultipleGlobalSettings(Mockito.any()))
                        .thenThrow(new StatusRuntimeException(Status.DEADLINE_EXCEEDED))
                        .thenReturn(settingResponse);
        GlobalProbePropertiesSettingsLoader loader = new GlobalProbePropertiesSettingsLoader(
                        settingsServiceClient, Executors.newScheduledThreadPool(1), 0L, 1L);
        int i = 0;
        while (!loader.isLoaded() && ++i < 100) {
            Thread.sleep(100L);
        }
        if (!loader.isLoaded()) {
            Assert.fail("Failed to load global settings");
        }
        Map<String, String> probeProperties = loader.getProbeProperties();
        Assert.assertNotNull(probeProperties);
        Assert.assertEquals(1, probeProperties.size());
        Map.Entry<String, String> name2value = probeProperties.entrySet().iterator().next();
        Assert.assertEquals(
                        PropertyKeyType.GLOBAL.toString().toLowerCase() + ".."
                                        + GlobalSettingSpecs.OnPremVolumeAnalysis.getSettingName(),
                        name2value.getKey());
        Assert.assertEquals(Boolean.TRUE.toString().toLowerCase(), name2value.getValue());
    }

}
