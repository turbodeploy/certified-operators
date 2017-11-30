package com.vmturbo.api.component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import com.vmturbo.api.component.external.api.mapper.SettingSpecStyleMappingLoader;
import com.vmturbo.api.component.external.api.mapper.SettingSpecStyleMappingLoader.SettingSpecStyleMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.service.SettingsService;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.group.api.SettingPolicySetting;
import com.vmturbo.group.persistent.EnumBasedSettingSpecStore;
import com.vmturbo.group.persistent.SettingSpecStore;
import com.vmturbo.group.service.SettingRpcService;

/**
 * JUnit test to cover production shipped entity settings transformation in UI.
 */
public class SettingsMapperIntegrationTest {

    /**
     * Test ensures, that all the settings from group component are seen in the UI through the
     * public API.
     *
     * @throws Exception if some exceptions occurred.
     */
    @Test
    public void testSettingsMapping() throws Exception {
        final SettingSpecStore specStore = new EnumBasedSettingSpecStore();
        final SettingRpcService settingRpcService = new SettingRpcService(specStore);
        final Server server =
                InProcessServerBuilder.forName("test").addService(settingRpcService).build();
        server.start();

        final Channel channel = InProcessChannelBuilder.forName("test").build();
        final SettingsManagerMapping settingsManagerMapping =
                new SettingsManagerMappingLoader("settingManagers.json").getMapping();
        final SettingSpecStyleMapping settingSpecStyleMapping =
                new SettingSpecStyleMappingLoader("settingSpecStyleTest.json").getMapping();
        final SettingsMapper mapper =
                new SettingsMapper(channel, settingsManagerMapping, settingSpecStyleMapping);
        final SettingsService settingService =
                new SettingsService(SettingServiceGrpc.newBlockingStub(channel), mapper,
                        settingsManagerMapping);

        final List<SettingsManagerApiDTO> settingSpecs =
                settingService.getSettingsSpecs(null, null, false);
        final Set<String> visibleSettings = settingSpecs.stream()
                .map(SettingsManagerApiDTO::getSettings)
                .flatMap(List::stream)
                .map(SettingApiDTO::getUuid)
                .collect(Collectors.toSet());
        final Set<String> enumSettingsNames = Stream.of(SettingPolicySetting.values())
                .map(SettingPolicySetting::getSettingName)
                .collect(Collectors.toSet());
        Assert.assertEquals(enumSettingsNames, visibleSettings);

        server.shutdownNow();
    }
}
