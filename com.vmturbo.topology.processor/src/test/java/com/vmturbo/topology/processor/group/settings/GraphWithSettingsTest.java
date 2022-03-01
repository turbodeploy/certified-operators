package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.history.BaseGraphRelatedTest;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Unit tests for GraphWithSettings.
 */
public class GraphWithSettingsTest extends BaseGraphRelatedTest {
    /**
     * Test that user setting value overrides the default policy value.
     */
    @Test
    public void testUserSettingOverridesDefault() {
        Map<Long, Builder> topologyBuilderMap = new HashMap<>();
        Map<Long, EntitySettings> entitySettings = new HashMap<>();
        Map<Long, SettingPolicy> policies = new HashMap<>();

        final long oid = 157L;
        final long defValue = 12L;
        final long value = 13L;
        Setting defaultPolicy = Setting.newBuilder()
                        .setSettingSpecName(EntitySettingSpecs.DrsMaintenanceProtectionWindow.getSettingName())
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(defValue))
                        .build();
        addEntityWithSetting(oid, EntityType.PHYSICAL_MACHINE_VALUE,
                        EntitySettingSpecs.DrsMaintenanceProtectionWindow, value,
                        topologyBuilderMap, entitySettings);
        policies.put(1L, SettingPolicy.newBuilder()
                        .setInfo(SettingPolicyInfo.newBuilder().addSettings(defaultPolicy))
                        .build());

        GraphWithSettings graph = new GraphWithSettings(
                        TopologyEntityTopologyGraphCreator.newGraph(topologyBuilderMap),
                        entitySettings, policies);
        Collection<Setting> settings = graph.getSettingsForEntity(oid);

        Assert.assertNotNull(settings);
        Map<String, Setting> name2setting = settings.stream()
                        .collect(Collectors.toMap(Setting::getSettingSpecName, s -> s));
        Setting drs = name2setting.get(EntitySettingSpecs.DrsMaintenanceProtectionWindow.getSettingName());
        Assert.assertNotNull(drs);
        Assert.assertEquals(value, (long)drs.getNumericSettingValue().getValue());
    }
}
