package com.vmturbo.api.component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.ScheduleMapper;
import com.vmturbo.api.component.external.api.mapper.SettingSpecStyleMappingLoader;
import com.vmturbo.api.component.external.api.mapper.SettingSpecStyleMappingLoader.SettingSpecStyleMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.Feature;
import com.vmturbo.api.component.external.api.service.SettingsPoliciesService;
import com.vmturbo.api.component.external.api.service.SettingsService;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.common.protobuf.extractor.ExtractorSettingServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.utils.ProbeFeature;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.group.service.SettingRpcService;
import com.vmturbo.group.setting.EnumBasedSettingSpecStore;
import com.vmturbo.group.setting.SettingSpecStore;
import com.vmturbo.group.setting.SettingStore;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * JUnit test to cover production shipped entity settings transformation in UI.
 */
public class SettingsMapperIntegrationTest {

    private SettingsPoliciesService settingsPoliciesService = Mockito.mock(
            SettingsPoliciesService.class);

    private ThinTargetCache thinTargetCache;
    // Set of settings that are intentionally hidden from the UI
    private final Set<String> invisibleSettings = ImmutableSet.of(
        EntitySettingSpecs.ScalingGroupMembership.getSettingName(),
        EntitySettingSpecs.VCPURequestUtilization.getSettingName());

    /**
     * Initialise test configuration.
     */
    @Before
    public void init() {
        // emulate that there is target supported external approval and audit features in
        // order not to filter out external approval and audit settings
        thinTargetCache = Mockito.mock(ThinTargetCache.class);
        Mockito.when(thinTargetCache.getAvailableProbeFeatures())
                .thenReturn(Sets.newHashSet(ProbeFeature.ACTION_APPROVAL,
                        ProbeFeature.ACTION_AUDIT,
                        ProbeFeature.DISCOVERY));
    }

    /**
     * Test ensures, that all the settings from group component are seen in the UI through the
     * public API.
     *
     * @throws Exception if some exceptions occurred.
     */
    @Test
    public void testSettingsMapping() throws Exception {
        final SettingSpecStore specStore = new EnumBasedSettingSpecStore(false, false,
                thinTargetCache);
        final SettingStore settingStore = Mockito.mock(SettingStore.class);
        final SettingRpcService settingRpcService =
            new SettingRpcService(specStore, settingStore);
        final Server server =
                InProcessServerBuilder.forName("test").addService(settingRpcService).build();
        server.start();

        final Channel channel = InProcessChannelBuilder.forName("test").build();
        final SettingsManagerMapping settingsManagerMapping =
                new SettingsManagerMappingLoader("settingManagers.json").getMapping();
        final SettingSpecStyleMapping settingSpecStyleMapping =
                new SettingSpecStyleMappingLoader("settingSpecStyleTest.json").getMapping();
        final SettingsMapper mapper =
                new SettingsMapper(SettingServiceGrpc.newBlockingStub(channel),
                    GroupServiceGrpc.newBlockingStub(channel),
                    SettingPolicyServiceGrpc.newBlockingStub(channel),
                    settingsManagerMapping, settingSpecStyleMapping,
                    ScheduleServiceGrpc.newBlockingStub(channel), new ScheduleMapper(),
                    ImmutableMap.of(
                            Feature.CloudScaleEnhancement, true,
                            Feature.ServiceHorizontalScale, true));
        final SettingsService settingService =
                new SettingsService(SettingServiceGrpc.newBlockingStub(channel),
                        StatsHistoryServiceGrpc.newBlockingStub(channel),
                        mapper, settingsManagerMapping, settingsPoliciesService,
                        ExtractorSettingServiceGrpc.newBlockingStub(channel));

        final List<SettingsManagerApiDTO> settingSpecs =
                settingService.getSettingsSpecs(null, null, null, false);
        final Set<String> visibleSettings = settingSpecs.stream()
                .map(SettingsManagerApiDTO::getSettings)
                .flatMap(List::stream)
                .map(SettingApiDTO::getUuid)
                .collect(Collectors.toSet());
        final Set<String> enumSettingsNames = Stream.of(EntitySettingSpecs.values())
                .map(EntitySettingSpecs::getSettingName)
            .filter(name -> !invisibleSettings.contains(name))
                .collect(Collectors.toSet());
        enumSettingsNames.addAll(
            Stream.of(GlobalSettingSpecs.values())
                .map(GlobalSettingSpecs::getSettingName)
                .collect(Collectors.toSet()));
        enumSettingsNames.addAll(ActionSettingSpecs.getSettingSpecs().stream()
            .map(SettingSpec::getName)
            .collect(Collectors.toSet()));

        // remainingGcCapacityUtilization and dbCacheHitRateUtilization
        // are purposely an invisible settings
        Assert.assertTrue(enumSettingsNames.remove("remainingGcCapacityUtilization"));
        Assert.assertTrue(enumSettingsNames.remove("dbCacheHitRateUtilization"));

        // old SLO settings are deprecated - remove them
        Assert.assertTrue(enumSettingsNames.remove("responseTimeCapacity"));
        Assert.assertTrue(enumSettingsNames.remove("autoSetResponseTimeCapacity"));
        Assert.assertTrue(enumSettingsNames.remove("transactionsCapacity"));
        Assert.assertTrue(enumSettingsNames.remove("autoSetTransactionsCapacity"));
        Assert.assertTrue(enumSettingsNames.remove("slaCapacity"));

        // serviceHorizontalScale is enabled, remove scalingPolicy setting
        Assert.assertTrue(enumSettingsNames.remove(EntitySettingSpecs.ScalingPolicy.getSettingName()));

        Assert.assertEquals(testError(enumSettingsNames, visibleSettings), enumSettingsNames, visibleSettings);

        server.shutdownNow();
    }

    private String testError(Set<String> s1, Set<String> s2) {
        Set<String> missing = Sets.newHashSet(s1);
        missing.removeAll(s2);
        Set<String> extra = Sets.newHashSet(s2);
        extra.removeAll(s1);
        return String.format("Missing: %s Extra: %s", missing, extra);

    }
}
