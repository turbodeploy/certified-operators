package com.vmturbo.group.persistent;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.DefaultType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.ScopeType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
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

    private static final int ENTITY_TYPE = 10;

    private static final String SPEC_NAME = "spec";

    private final SettingSpecStore specStore = mock(SettingSpecStore.class);

    private final GroupStore groupStore = mock(GroupStore.class);

    private final DefaultSettingPolicyValidator validator =
            new DefaultSettingPolicyValidator(specStore, groupStore);

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingWithNoName() throws InvalidSettingPolicyException {
        validator.validateSettingPolicy(newInfo()
                .addSettings(Setting.newBuilder()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingSpecNotFound() throws InvalidSettingPolicyException {
        when(specStore.getSpec(any())).thenReturn(Optional.empty());
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting())
                .build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyForGlobal() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(SettingSpec.newBuilder()
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting())
                .build());
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
                .build());
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
                .build());
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
                .build());
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
                .build());
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
                .build());
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
                .build());
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
                .build());
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
                .build());
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
                .build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyEnumMismatchedTypes() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyBooleanMismatchedTypes() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setEnumSettingValue(EnumSettingValue.getDefaultInstance()))
                .build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyStringMismatchedTypes() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyNumericMismatchedTypes() throws InvalidSettingPolicyException {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance())
                .build()));
        validator.validateSettingPolicy(newInfo()
                .addSettings(newSetting()
                        .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance()))
                .build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testSettingPolicyInvalidGroup() throws Exception {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        when(groupStore.getGroups(any())).thenReturn(ImmutableMap.of(7L, Optional.empty()));
        validator.validateSettingPolicy(newInfo()
                .setScope(ScopeType.newBuilder()
                        .addGroups(7L))
                .build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testGroupScopeEntityTypeMismatch() throws Exception {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build()));
        when(groupStore.getGroups(any())).thenReturn(ImmutableMap.of(7L, Optional.of(Group.newBuilder()
                .setId(7L)
                .setInfo(GroupInfo.newBuilder()
                        .setEntityType(ENTITY_TYPE + 1))
                .build())));
        validator.validateSettingPolicy(newInfo()
                .setScope(ScopeType.newBuilder()
                        .addGroups(7L))
                .build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testGroupRetrievalDatabaseError() throws Exception {
        when(specStore.getSpec(eq(SPEC_NAME))).thenReturn(Optional.of(newEntitySettingSpec()
                .build()));
        when(groupStore.getGroups(any())).thenThrow(DatabaseException.class);
        validator.validateSettingPolicy(newInfo()
                .setScope(ScopeType.newBuilder()
                        .addGroups(7L))
                .build());
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
                .build());
    }


    private SettingPolicyInfo.Builder newInfo() {
        return SettingPolicyInfo.newBuilder()
                .setName("SettingPolicy")
                .setEntityType(ENTITY_TYPE)
                .setDefault(DefaultType.getDefaultInstance());
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
