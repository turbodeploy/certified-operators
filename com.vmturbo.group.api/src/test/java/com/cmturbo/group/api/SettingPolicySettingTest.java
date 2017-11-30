package com.cmturbo.group.api;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.group.api.SettingPolicySetting;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

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

    /**
     * Checks, that all the entity-type specific defaults are inside the entity scope of the
     * setting. All the settings are expected to fulfill this requirement.
     */
    @Test
    public void testLegalTypeSpecificDefaults() {
        for (SettingPolicySetting setting : SettingPolicySetting.values()) {
            final SettingSpec spec = setting.createSettingSpec();
            switch (spec.getSettingValueTypeCase()) {
                case BOOLEAN_SETTING_VALUE_TYPE: {
                    final BooleanSettingValueType valueType = spec.getBooleanSettingValueType();
                    for (Integer entry : valueType.getEntityDefaultsMap().keySet()) {
                        assertEntityType(setting,
                                spec.getEntitySettingSpec().getEntitySettingScope(), entry);
                    }
                    break;
                }
                case NUMERIC_SETTING_VALUE_TYPE: {
                    final NumericSettingValueType valueType = spec.getNumericSettingValueType();
                    for (Integer entry : valueType.getEntityDefaultsMap().keySet()) {
                        assertEntityType(setting,
                                spec.getEntitySettingSpec().getEntitySettingScope(), entry);
                    }
                    break;
                }
                case STRING_SETTING_VALUE_TYPE: {
                    final StringSettingValueType valueType = spec.getStringSettingValueType();
                    for (Integer entry : valueType.getEntityDefaultsMap().keySet()) {
                        assertEntityType(setting,
                                spec.getEntitySettingSpec().getEntitySettingScope(), entry);
                    }
                    break;
                }
                case ENUM_SETTING_VALUE_TYPE: {
                    final EnumSettingValueType valueType = spec.getEnumSettingValueType();
                    for (Integer entry : valueType.getEntityDefaultsMap().keySet()) {
                        assertEntityType(setting,
                                spec.getEntitySettingSpec().getEntitySettingScope(), entry);
                    }
                    break;
                }
                default: {
                    Assert.fail("Data structure type is unknown for policy setting " + setting);
                }
            }
        }
    }

    /**
     * Asserts that the specified entity type is inside the policy setting scope.
     *
     * @param setting setting to examine (used for verbose output only)
     * @param settingScope setting scope to examine
     * @param entityType entity type to check
     */
    private void assertEntityType(@Nonnull SettingPolicySetting setting,
            @Nonnull EntitySettingScope settingScope, int entityType) {
        if (settingScope.hasAllEntityType()) {
            return;
        }
        Assert.assertTrue("Type specific default " + EntityType.values()[entityType] +
                        " is out of entity scope for " + setting,
                settingScope.getEntityTypeSet().getEntityTypeList().contains(entityType));
    }
}
