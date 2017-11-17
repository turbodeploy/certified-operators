package com.vmturbo.group.persistent;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link FileBasedSettingsSpecStore}.
 */
public class SettingSpecsStoreTest {

    private static final Set<String> settings = ImmutableSet.<String>builder().add("move")
            .add("suspend")
            .add("cpuUtilization")
            .add("memoryUtilization")
            .add("ioThroughput")
            .add("netThroughput")
            .add("swappingUtilization")
            .add("readyQueueUtilization")
            .add("cpuOverprovisionedPercentage")
            .add("memoryOverprovisionedPercentage")
            .build();

    /**
     * Tests all the known settings (that they are loaded correctly by the store).
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testExistingSettings() throws Exception {
        final FileBasedSettingsSpecStore src =
                new FileBasedSettingsSpecStore("setting/setting-spec.json");
        for (String setting : settings) {
            Assert.assertTrue("Could not resolve property by name " + setting,
                    src.getSettingSpec(setting).isPresent());
        }
    }

    /**
     * Tests if settings spec store is requested for the non-existing setting.
     *
     * @throws Exception if exceptions occurred.
     */
    @Test
    public void testNofFound() throws Exception {
        final FileBasedSettingsSpecStore src =
                new FileBasedSettingsSpecStore("setting/setting-spec.json");
        Assert.assertFalse(src.getSettingSpec("non-existing-setting").isPresent());
    }
}
