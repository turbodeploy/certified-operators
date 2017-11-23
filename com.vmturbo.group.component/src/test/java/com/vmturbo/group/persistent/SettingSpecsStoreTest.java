package com.vmturbo.group.persistent;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.group.api.SettingPolicySetting;

/**
 * Unit tests for {@link FileBasedSettingsSpecStore} against real (production) list of settings.
 */
public class SettingSpecsStoreTest {

    private SettingSpecStore specStore;

    @Before
    public void init() {
        specStore = new EnumBasedSettingSpecStore();
    }

    /**
     * Tests all the known settings (that they are loaded correctly by the store).
     */
    @Test
    public void testExistingSettings() {
        for (SettingPolicySetting setting : SettingPolicySetting.values()) {
            Assert.assertTrue("Could not resolve property by name " + setting,
                    specStore.getSettingSpec(setting.getSettingName()).isPresent());
        }
    }

    /**
     * Tests checks, that all the settings, mentioned in {@link SettingPolicySetting} enum are registered
     * in JSON file and vice versa.
     */
    @Test
    public void testAllSettingsExist() {
        final Set<String> enumSettingNames = Stream.of(SettingPolicySetting.values())
                .map(SettingPolicySetting::getSettingName)
                .collect(Collectors.toSet());
        final Set<String> jsonSettingNames = specStore.getAllSettingSpec()
                .stream()
                .map(SettingSpec::getName)
                .collect(Collectors.toSet());
        Assert.assertEquals(enumSettingNames, jsonSettingNames);
    }

    /**
     * Tests if settings spec store is requested for the non-existing setting.
     */
    @Test
    public void testNofFound() {
        Assert.assertFalse(specStore.getSettingSpec("non-existing-setting").isPresent());
    }

    /**
     * Tests, that all the settings in JSON are unique by name.
     */
    @Test
    public void testUniqueSettingNames() {
        final Set<String> settingNames = new HashSet<>();
        for (SettingSpec spec : specStore.getAllSettingSpec()) {
            final String specName = spec.getName();
            Assert.assertTrue("Setting " + specName + " duplicated", settingNames.add(specName));
        }
    }
}
