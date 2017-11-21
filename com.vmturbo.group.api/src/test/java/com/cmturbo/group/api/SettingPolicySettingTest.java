package com.cmturbo.group.api;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.group.api.SettingPolicySetting;

/**
 * Tests group settings enum's static functions ({@link SettingPolicySetting}.
 */
public class SettingPolicySettingTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Tests null argument passed to {@link SettingPolicySetting#getSettingByName(String)}. NPE is
     * expected to be thrown.
     */
    @Test
    public void testNullArgument() {
        expectedException.expect(NullPointerException.class);
        SettingPolicySetting.getSettingByName(null);
    }

    /**
     * Tests non existing setting request. Empty optional is expected.
     */
    @Test
    public void testNoResult() {
        Assert.assertFalse(
                SettingPolicySetting.getSettingByName("non-existing-property").isPresent());
    }

    /**
     * Test existing setting request.
     */
    @Test
    public void testProperResult() {
        Assert.assertTrue(SettingPolicySetting.getSettingByName("move").isPresent());
    }

    /**
     * Checks for unique setting names across the enum.
     */
    @Test
    public void testUniqueSettingName() {
        final Set<String> settingNames = new HashSet<>();
        for (SettingPolicySetting setting : SettingPolicySetting.values()) {
            final String settingName = setting.getSettingName();
            Assert.assertTrue("Setting name " + settingName + " duplicated",
                    settingNames.add(settingName));
        }
    }
}
