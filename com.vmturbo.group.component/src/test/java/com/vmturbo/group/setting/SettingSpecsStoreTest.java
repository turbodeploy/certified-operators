package com.vmturbo.group.setting;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

/**
 * Unit tests for {@link FileBasedSettingsSpecStore} against real (production) list of settings.
 */
public class SettingSpecsStoreTest {

    private SettingSpecStore specStore;

    @Before
    public void init() {
        specStore = new EnumBasedSettingSpecStore(
            false, false);
    }

    /**
     * Tests all the known settings (that they are loaded correctly by the store).
     */
    @Test
    public void testExistingSettings() {
        for (EntitySettingSpecs setting : EntitySettingSpecs.values()) {
            Assert.assertTrue("Could not resolve property by name " + setting,
                    specStore.getSettingSpec(setting.getSettingName()).isPresent());
        }
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
        for (SettingSpec spec : specStore.getAllSettingSpecs()) {
            final String specName = spec.getName();
            Assert.assertTrue("Setting " + specName + " duplicated", settingNames.add(specName));
        }
    }
}
