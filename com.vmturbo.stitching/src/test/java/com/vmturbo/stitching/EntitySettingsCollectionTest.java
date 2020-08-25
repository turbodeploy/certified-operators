package com.vmturbo.stitching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTOREST.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

public class EntitySettingsCollectionTest {

    private static final long ENTITY_OID = 23345;
    private static final long DEFAULT_SETTING_ID = 23346;

    private static final Setting MOVE_DISABLED_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.Move.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.DISABLED.name()))
        .build();

    private static final Setting MOVE_AUTOMATIC_SETTING = Setting.newBuilder()
        .setSettingSpecName(ConfigurableActionSettings.Move.getSettingName())
        .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.name()))
        .build();

    private static final TopologyEntityDTO.Builder PARENT_ENTITY_DTO_BUILDER =
        TopologyEntityDTO.newBuilder().setOid(ENTITY_OID).setEntityType(100001);

    private static final TopologyEntity PARENT_ENTITY =
        TopologyEntity.newBuilder(PARENT_ENTITY_DTO_BUILDER.setOrigin(
            Origin.newBuilder().setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(1234L).lastUpdatedAt(5678L)))
        ).build();

    final Map<Long, SettingPolicy> defaultSettingPolicies = Collections.singletonMap(
        DEFAULT_SETTING_ID, SettingPolicy.newBuilder().setId(DEFAULT_SETTING_ID)
            .setInfo(SettingPolicyInfo.newBuilder().addSettings(MOVE_AUTOMATIC_SETTING))
            .build());

    final Map<Long, EntitySettings> settingsByEntity = Collections.singletonMap(
        ENTITY_OID, EntitySettings.newBuilder()
            .setEntityOid(ENTITY_OID)
            .setDefaultSettingPolicyId(DEFAULT_SETTING_ID)
            .addUserSettings(SettingToPolicyId.newBuilder().setSetting(MOVE_DISABLED_SETTING).build())
            .build());

    final Map<Long, EntitySettings> noUserSettings = Collections.singletonMap(
        ENTITY_OID, EntitySettings.newBuilder()
            .setEntityOid(ENTITY_OID)
            .setDefaultSettingPolicyId(DEFAULT_SETTING_ID)
            .build());

    @Test
    public void testGetEntityUserSetting() {
        final EntitySettingsCollection settingsCollection =
            new EntitySettingsCollection(Collections.emptyMap(), settingsByEntity);

        final Setting moveSetting = settingsCollection.getEntitySetting(PARENT_ENTITY.getOid(),
            ConfigurableActionSettings.Move.getSettingName()).get();

        assertEquals(MOVE_DISABLED_SETTING, moveSetting);
    }

    @Test
    public void testGetEntityDefaultSetting() {
        final EntitySettingsCollection settingsCollection =
            new EntitySettingsCollection(defaultSettingPolicies, noUserSettings);

        final Setting moveSetting = settingsCollection.getEntitySetting(PARENT_ENTITY.getOid(),
            ConfigurableActionSettings.Move.getSettingName()).get();
        assertEquals(MOVE_AUTOMATIC_SETTING, moveSetting);
    }

    @Test
    public void testUserSettingOverridesDefault() {
        final EntitySettingsCollection settingsCollection =
            new EntitySettingsCollection(defaultSettingPolicies, settingsByEntity);

        final Setting moveSetting = settingsCollection.getEntitySetting(PARENT_ENTITY.getOid(),
            ConfigurableActionSettings.Move.getSettingName()).get();
        assertEquals(MOVE_DISABLED_SETTING, moveSetting);
    }

    @Test
    public void testGetEntitySettingEntityNotPresent() {
        final EntitySettingsCollection settingsCollection =
            new EntitySettingsCollection(Collections.emptyMap(), Collections.emptyMap());

        assertFalse(settingsCollection.getEntitySetting(PARENT_ENTITY.getOid(),
            ConfigurableActionSettings.Move.getSettingName()).isPresent());
    }

    @Test
    public void testGetEntitySettingNoUserOrDefault() {
        final EntitySettingsCollection settingsCollection =
            new EntitySettingsCollection(Collections.emptyMap(), noUserSettings);

        assertFalse(settingsCollection.getEntitySetting(PARENT_ENTITY.getOid(),
            ConfigurableActionSettings.Move.getSettingName()).isPresent());
    }

    @Test
    public void testGetEntitySettingNameNotFound() {
        final EntitySettingsCollection settingsCollection =
            new EntitySettingsCollection(defaultSettingPolicies, settingsByEntity);

        assertFalse(settingsCollection.getEntitySetting(PARENT_ENTITY, EntitySettingSpecs.StorageAmountUtilization)
            .isPresent());
    }

    @Test
    public void testGetUserSetting() {
        final EntitySettingsCollection settingsCollection =
            new EntitySettingsCollection(defaultSettingPolicies, settingsByEntity);
        final Setting moveSetting = settingsCollection.getEntityUserSetting(PARENT_ENTITY.getOid(),
                ConfigurableActionSettings.Move.getSettingName()).get();
        assertEquals(MOVE_DISABLED_SETTING, moveSetting);
    }

    @Test
    public void testGetUserSettingIgnoresDefault() {
        final EntitySettingsCollection settingsCollection =
            new EntitySettingsCollection(defaultSettingPolicies, noUserSettings);
        assertFalse(settingsCollection.getEntityUserSetting(PARENT_ENTITY.getOid(),
            ConfigurableActionSettings.Move.getSettingName()).isPresent());
    }
}
