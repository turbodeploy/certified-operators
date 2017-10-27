package com.vmturbo.group.persistent;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.group.persistent.SettingStore.DefaultSettingPolicyValidator;
import com.vmturbo.group.persistent.SettingStore.SettingSpecStore;

/**
 * Tests for {@link DefaultSettingPolicyValidator}.
 * <p>
 * Separated from {@link SettingStoreTest} so that we don't do the database setup/teardown
 * when running these.
 */
public class SettingPolicyValidatorTest {

    private static final long GROUP_ID = 99;

    private static final int ENTITY_TYPE = 10;

    private static final String SPEC_NAME = "spec";

    private final SettingSpecStore specStore = mock(SettingSpecStore.class);

    private final GroupStore groupStore = mock(GroupStore.class);

    private final DefaultSettingPolicyValidator validator =
            new DefaultSettingPolicyValidator(specStore, groupStore);

    @Before
    public void setup() throws Exception {
        when(groupStore.get(eq(GROUP_ID)))
            .thenReturn(Optional.of(Group.newBuilder()
                .setGroup(GroupInfo.newBuilder()
                        .setEntityType(ENTITY_TYPE))
                .build()));
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingWithNoName() throws InvalidSettingPolicyException {
        validator.validateSettingPolicy(newInfo()
                .addSettings(Setting.newBuilder()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingSpecNotFound() throws InvalidSettingPolicyException {
        when(specStore.getSpec(any())).thenReturn(Optional.empty());
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting())
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyForGlobal() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(SettingSpec.newBuilder()
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting())
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingNoName() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(SettingPolicyInfo.newBuilder()
                .setEntityType(10)
                .build(), Type.DEFAULT);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingNoEntityType() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(SettingPolicyInfo.newBuilder()
                .setName("Policy")
                .build(), Type.DEFAULT);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testDefaultSettingWithScope() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
            .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
            .build()));
        validator.validateSettingPolicy(SettingPolicyInfo.newBuilder()
            .setName("Policy")
            .setEntityType(10)
            .setScope(Scope.newBuilder().addGroups(GROUP_ID))
            .build(), Type.DEFAULT);
    }

    @Test
    public void testNumericSettingNoConstraints() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(10)))
                .build(), Type.USER);
    }

    @Test
    public void testNumericSettingWithinConstraints() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                    .setMin(1.0f)
                    .setMax(1.2f))
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(1.1f)))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testNumericSettingTooLow() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                        .setMin(1.0f)
                        .setMax(1.2f))
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(0.9f)))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testNumericSettingTooHigh() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                        .setMin(1.0f)
                        .setMax(1.2f))
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(1.3f)))
                .build(), Type.USER);
    }

    @Test
    public void testStringSettingNoValidationRegex() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue("foo")))
                .build(), Type.USER);
    }

    @Test
    public void testStringSettingValidationMatch() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.newBuilder()
                    .setValidationRegex("foo.*"))
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue("foo123")))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testStringSettingInvalid() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.newBuilder()
                        .setValidationRegex("foo.*"))
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue("boo123")))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testEnumSettingInvalid() throws InvalidSettingPolicyException{
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                        .addEnumValues("1").addEnumValues("2"))
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setEnumSettingValue(EnumSettingValue.newBuilder()
                                .setValue("x")))
                .build(), Type.USER);
    }

    @Test
    public void testEnumSettingValid() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                        .addEnumValues("1").addEnumValues("2"))
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setEnumSettingValue(EnumSettingValue.newBuilder()
                                .setValue("2")))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyEnumMismatchedTypes() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyBooleanMismatchedTypes() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setEnumSettingValue(EnumSettingValue.getDefaultInstance()))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyStringMismatchedTypes() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyNumericMismatchedTypes() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyInvalidGroup() throws Exception {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        when(groupStore.getGroups(any())).thenReturn(ImmutableMap.of(7L, Optional.empty()));
        validator.validateSettingPolicy(newInfo()
                .setScope(Scope.newBuilder()
                        .addGroups(7L))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testGroupScopeEntityTypeMismatch() throws Exception {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        when(groupStore.getGroups(any())).thenReturn(ImmutableMap.of(7L, Optional.of(Group.newBuilder()
                .setId(7L)
                .setGroup(GroupInfo.newBuilder()
                        .setEntityType(ENTITY_TYPE + 1))
                .build())));
        validator.validateSettingPolicy(newInfo()
                .setScope(Scope.newBuilder()
                        .addGroups(7L))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testGroupRetrievalDatabaseError() throws Exception {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .build()));
        when(groupStore.getGroups(any())).thenThrow(DatabaseException.class);
        validator.validateSettingPolicy(newInfo()
                .setScope(Scope.newBuilder()
                        .addGroups(7L))
                .build(), Type.USER);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyAndSettingSpecEntityTypeMismatch()
            throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder()
                                        .addEntityType(ENTITY_TYPE + 1))))
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting())
                .build(), Type.USER);
    }

    private SettingPolicyInfo.Builder newInfo() {
        return SettingPolicyInfo.newBuilder()
                .setName("SettingPolicy")
                .setEntityType(ENTITY_TYPE)
                .setScope(Scope.newBuilder()
                    .addGroups(GROUP_ID));
    }

    private Setting.Builder newSetting() {
        return Setting.newBuilder()
                .setSettingSpecName(SPEC_NAME)
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance());
    }

    private SettingSpec.Builder newEntitySettingSpec() {
        return SettingSpec.newBuilder()
                .setName(SPEC_NAME)
                .setEntitySettingSpec(EntitySettingSpec.getDefaultInstance())
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance());
    }

}
