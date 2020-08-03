package com.vmturbo.components.common.setting;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Tests group settings enum's static functions ({@link EntitySettingSpecs}.
 */
public class EntitySettingSpecsTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Tests null argument passed to {@link EntitySettingSpecs#getSettingByName(String)}. NPE is
     * expected to be thrown.
     */
    @Test
    public void testNullArgument() {
        expectedException.expect(NullPointerException.class);
        EntitySettingSpecs.getSettingByName(null);
    }

    /**
     * Tests non existing setting request. Empty optional is expected.
     */
    @Test
    public void testNoResult() {
        Assert.assertFalse(
                EntitySettingSpecs.getSettingByName("non-existing-property").isPresent());
    }

    /**
     * Test existing setting request.
     */
    @Test
    public void testProperResult() {
        Assert.assertTrue(EntitySettingSpecs.getSettingByName("move").isPresent());
    }

    /**
     * Checks for unique setting names across the enum.
     */
    @Test
    public void testUniqueSettingName() {
        final Set<String> settingNames = new HashSet<>();
        for (EntitySettingSpecs setting : EntitySettingSpecs.values()) {
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
        for (EntitySettingSpecs setting : EntitySettingSpecs.values()) {
            final SettingSpec spec = setting.getSettingSpec();
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
                case SORTED_SET_OF_OID_SETTING_VALUE_TYPE:
                    // No entity-type specific defaults.
                    break;
                default: {
                    Assert.fail("Data structure type is unknown for policy setting " + setting);
                }
            }
        }
    }

    /**
     * Test that the Orchestration policies are defined.
     */
    @Test
    public void testOrchestrationPoliciesDefined() {
        Assert.assertTrue("Expect suspendActionWorkflow",
                EntitySettingSpecs.getSettingByName("suspendActionWorkflow").isPresent());
        Assert.assertTrue("Expect provisionActionWorkflow",
                EntitySettingSpecs.getSettingByName("provisionActionWorkflow").isPresent());
    }

    /**
     * Test the overprovision min, max and default values as set
     */
    @Test
    public void testOverProvisionMinMaxDefault() {
        EntitySettingSpecs memProvSpec = EntitySettingSpecs.MemoryOverprovisionedPercentage;
        NumericSettingValueType memProvValType =
                        memProvSpec.getSettingSpec().getNumericSettingValueType();
        Assert.assertTrue(memProvValType.getMin() == 1f);
        Assert.assertTrue(memProvValType.getMax() == 1000000f);
        Assert.assertTrue(memProvValType.getDefault() == 1000f);

        EntitySettingSpecs cpuProvSpec = EntitySettingSpecs.CpuOverprovisionedPercentage;
        NumericSettingValueType cpuProvValType =
                        cpuProvSpec.getSettingSpec().getNumericSettingValueType();
        Assert.assertTrue(cpuProvValType.getMin() == 1f);
        Assert.assertTrue(cpuProvValType.getMax() == 1000000f);
        Assert.assertTrue(cpuProvValType.getDefault() == 30000f);

        EntitySettingSpecs stProvSpec = EntitySettingSpecs.StorageOverprovisionedPercentage;
        NumericSettingValueType stProvValType =
                        stProvSpec.getSettingSpec().getNumericSettingValueType();
        Assert.assertTrue(stProvValType.getMin() == 1f);
        Assert.assertTrue(stProvValType.getMax() == 1000000f);
        Assert.assertTrue(stProvValType.getDefault() == 200f);
    }

    /**
     * Asserts that the specified entity type is inside the policy setting scope.
     *
     * @param setting setting to examine (used for verbose output only)
     * @param settingScope setting scope to examine
     * @param entityType entity type to check
     */
    private void assertEntityType(@Nonnull EntitySettingSpecs setting,
            @Nonnull EntitySettingScope settingScope, int entityType) {
        if (settingScope.hasAllEntityType()) {
            return;
        }
        Assert.assertTrue("Type specific default " + EntityType.values()[entityType] +
                        " is out of entity scope for " + setting,
                settingScope.getEntityTypeSet().getEntityTypeList().contains(entityType));
    }
}
