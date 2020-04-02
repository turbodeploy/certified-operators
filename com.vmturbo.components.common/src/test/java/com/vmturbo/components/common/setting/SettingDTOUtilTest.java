package com.vmturbo.components.common.setting;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;

/**
 * Unit tests for {@link SettingDTOUtil}.
 */
public class SettingDTOUtilTest {

    @Test
    public void testIndexSettingsByEntity() {
        final Setting setting1 = Setting.newBuilder()
            .setSettingSpecName("name1")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();
        final Setting setting2 = Setting.newBuilder()
            .setSettingSpecName("name2")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();
        final Setting setting3 = Setting.newBuilder()
            .setSettingSpecName("name3")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();

        EntitySettingGroup group1 = EntitySettingGroup.newBuilder()
            .addEntityOids(1L)
            .addEntityOids(2L)
            .setSetting(setting1)
            .build();

        EntitySettingGroup group2 = EntitySettingGroup.newBuilder()
            .addEntityOids(3L)
            .setSetting(setting2)
            .addPolicyId(SettingPolicyId.newBuilder()
                .setDisplayName("foo")
                .setPolicyId(123))
            .build();

        EntitySettingGroup group3 = EntitySettingGroup.newBuilder()
            .addEntityOids(4L)
            .setSetting(setting3)
            .addPolicyId(SettingPolicyId.newBuilder()
                .setDisplayName("bar")
                .setPolicyId(456))
            .build();

        final Map<Long, Map<String, Setting>> result =
            SettingDTOUtil.indexSettingsByEntity(Stream.of(group1, group2, group3));
        assertThat(result.keySet(), containsInAnyOrder(1L, 2L, 3L, 4L));
        assertThat(result.get(1L).keySet(), containsInAnyOrder("name1"));
        assertThat(result.get(1L).get("name1"), is(setting1));
        assertThat(result.get(2L).keySet(), containsInAnyOrder("name1"));
        assertThat(result.get(2L).get("name1"), is(setting1));
        assertThat(result.get(3L).keySet(), containsInAnyOrder("name2"));
        assertThat(result.get(3L).get("name2"), is(setting2));
        assertThat(result.get(4L).keySet(), containsInAnyOrder("name3"));
        assertThat(result.get(4L).get("name3"), is(setting3));

    }

    @Test
    public void testFlattenIterator() {
        EntitySettingGroup group1 = EntitySettingGroup.newBuilder()
            .addEntityOids(1L)
            .build();

        EntitySettingGroup group2 = EntitySettingGroup.newBuilder()
            .addEntityOids(2L)
            .build();
        GetEntitySettingsResponse response1 = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(group1)
            .build();
        GetEntitySettingsResponse response2 = GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(group2)
            .build();

        List<EntitySettingGroup> flattened = SettingDTOUtil.flattenEntitySettings(
            ImmutableList.of(response1, response2).iterator())
                .collect(Collectors.toList());

        assertThat(flattened, containsInAnyOrder(group1, group2));
    }

    @Test
    public void testInvolvedGroups() {
        final SettingPolicy policy1 = SettingPolicy.newBuilder()
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setScope(Scope.newBuilder()
                                .addGroups(7L)))
                .build();
        final SettingPolicy policy2 = SettingPolicy.newBuilder()
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setScope(Scope.newBuilder()
                                .addGroups(8L)))
                .build();
        final Set<Long> involvedGroups =
                SettingDTOUtil.getInvolvedGroups(Arrays.asList(policy1, policy2));
        assertEquals(Sets.newHashSet(7L, 8L), involvedGroups);
    }

    @Test
    public void testInvolvedGroupsDefault() {
        final SettingPolicy policy1 = SettingPolicy.newBuilder()
                .setInfo(SettingPolicyInfo.newBuilder())
                .build();
        final Set<Long> involvedGroups =
                SettingDTOUtil.getInvolvedGroups(Collections.singletonList(policy1));
        assertTrue(involvedGroups.isEmpty());
    }

    @Test
    public void testInvolvedGroupsSingle() {
        final SettingPolicy policy1 = SettingPolicy.newBuilder()
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setScope(Scope.newBuilder()
                                .addGroups(7L)))
                .build();
        final Set<Long> involvedGroups =
                SettingDTOUtil.getInvolvedGroups(policy1);
        assertEquals(Collections.singleton(7L), involvedGroups);
    }

    @Test
    public void testOverlappingEntities() {
        final SettingSpec spec1 = SettingSpec.newBuilder()
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder()
                                        .addEntityType(1)
                                        .addEntityType(2))))
                .build();
        final SettingSpec spec2 = SettingSpec.newBuilder()
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder()
                                        .addEntityType(2)
                                        .addEntityType(3))))
                .build();
        final Optional<Set<Integer>> result =
                SettingDTOUtil.getOverlappingEntityTypes(Arrays.asList(spec1, spec2));
        assertTrue(result.isPresent());
        assertEquals(Collections.singleton(2), result.get());
    }

    @Test
    public void testOverlappingEntitiesNoEntityTypes() {
        final SettingSpec spec1 = SettingSpec.newBuilder()
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setAllEntityType(AllEntityType.getDefaultInstance())))
                .build();
        assertFalse(SettingDTOUtil.getOverlappingEntityTypes(Collections.singleton(spec1)).isPresent());
    }

    @Test
    public void testOverlappingEntitiesGlobalSpecs() {
        final SettingSpec spec1 = SettingSpec.newBuilder()
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .build();
        assertFalse(SettingDTOUtil.getOverlappingEntityTypes(Collections.singleton(spec1)).isPresent());
    }

    @Test
    public void testExtractDefaultSettingPolicies() {
        final SettingPolicy policy1 =
            SettingPolicy.newBuilder()
                .setSettingPolicyType(SettingPolicy.Type.DEFAULT)
                .build();
        final SettingPolicy policy2 =
            SettingPolicy.newBuilder()
                .setSettingPolicyType(SettingPolicy.Type.USER)
                .build();

        List<SettingPolicy> defaultSettings =
            SettingDTOUtil.extractDefaultSettingPolicies(Arrays.asList(policy1, policy2));

        assertThat(defaultSettings.size(), is(1));
        assertThat(defaultSettings.get(0), is(policy1));
    }

    @Test
    public void testExtractDefaultSettingPoliciesNoDefault() {
        final SettingPolicy policy1 =
            SettingPolicy.newBuilder()
                .setSettingPolicyType(SettingPolicy.Type.USER)
                .build();

        List<SettingPolicy> defaultSettings =
            SettingDTOUtil.extractDefaultSettingPolicies(Arrays.asList(policy1));

        assertThat(defaultSettings.size(), is(0));
    }

    @Test
    public void testExtractUserAndDiscoveredSettingPolicies() {
        final SettingPolicy policy1 =
            SettingPolicy.newBuilder()
                .setSettingPolicyType(SettingPolicy.Type.DEFAULT)
                .build();
        final SettingPolicy policy2 =
            SettingPolicy.newBuilder()
                .setSettingPolicyType(SettingPolicy.Type.USER)
                .build();
        final SettingPolicy policy3 =
            SettingPolicy.newBuilder()
                .setSettingPolicyType(Type.DISCOVERED)
                .build();

        List<SettingPolicy> userSettings =
            SettingDTOUtil.extractUserAndDiscoveredSettingPolicies(Arrays.asList(policy1, policy2, policy3));

        assertThat(userSettings.size(), is(2));
        assertThat(userSettings, containsInAnyOrder(policy2, policy3));
    }

    @Test
    public void testExtractUserSettingPoliciesNoUserSetting() {
        final SettingPolicy policy =
            SettingPolicy.newBuilder()
                .setSettingPolicyType(SettingPolicy.Type.DEFAULT)
                .build();

        List<SettingPolicy> userSettings =
            SettingDTOUtil.extractUserAndDiscoveredSettingPolicies(Arrays.asList(policy));

        assertThat(userSettings.size(), is(0));
    }

    @Test
    public void testArrangeByEntityType() {

        int entityType = 5;
        long spId = 111L;
        final SettingPolicy policy =
            SettingPolicy.newBuilder()
                .setId(111L)
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setEntityType(entityType)
                        .build())
                .build();

        Map<Integer, SettingPolicy> entityTypeSPMap =
            SettingDTOUtil.arrangeByEntityType(Arrays.asList(policy));

        assertThat(entityTypeSPMap.size(), is(1));
        assertThat(entityTypeSPMap.get(entityType), is(policy));
    }

    @Test
    public void testCompareEnumSettingValues() {
        EnumSettingValueType type =
            EnumSettingValueType.newBuilder()
                .addAllEnumValues(Arrays.asList("value1", "value2", "value3"))
                .build();

        //gt
        assertThat(
            SettingDTOUtil.compareEnumSettingValues(
                createEnumSettingValue("value3"),
                createEnumSettingValue("value1"), type),
            greaterThan(0));

        //eq
        assertThat(
            SettingDTOUtil.compareEnumSettingValues(
                createEnumSettingValue("value2"),
                createEnumSettingValue("value2"), type),
            is(0));

        //lt
        assertThat(
            SettingDTOUtil.compareEnumSettingValues(
                createEnumSettingValue("value1"),
                createEnumSettingValue("value2"), type),
            lessThan(0));
    }

    /**
     * Test using the utility method to create a SettingCategoryPath with three levels.
     */
    @Test
    public void testCreateSettingCategoryPath() {
        // arrange
        final String PATH_ROOT = "a";
        final String PATH_CHILD_1 = "b";
        final String PATH_CHILD_2 = "c";
        List<String> CATEGORY_PATHS = ImmutableList.of(PATH_ROOT, PATH_CHILD_1, PATH_CHILD_2);
        SettingCategoryPath expectedCategoryPath = SettingCategoryPath.newBuilder()
                .setRootPathNode(SettingCategoryPathNode.newBuilder()
                        .setNodeName(PATH_ROOT)
                        .setChildNode(SettingCategoryPathNode.newBuilder()
                                .setNodeName(PATH_CHILD_1)
                                .setChildNode(SettingCategoryPathNode.newBuilder()
                                        .setNodeName(PATH_CHILD_2)
                                        .build())
                                .build())
                        .build())
                .build();
        // act
        SettingCategoryPath categoryPath = SettingDTOUtil.createSettingCategoryPath(CATEGORY_PATHS);
        // assert
        assertThat(categoryPath, equalTo(expectedCategoryPath));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 equal boolean values.
     */
    @Test
    public void testAreValuesEqualForEqualBooleans() {
        final Setting setting1 = Setting.newBuilder()
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                .build();

        assertTrue(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 not equal boolean values.
     */
    @Test
    public void testAreValuesEqualForNonEqualBooleans() {
        final Setting setting1 = Setting.newBuilder()
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(false))
                .build();

        assertFalse(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 equal numeric values.
     */
    @Test
    public void testAreValuesEqualForEqualNumbers() {
        final Setting setting1 = Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(1))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(1))
                .build();

        assertTrue(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 not equal numeric values.
     */
    @Test
    public void testAreValuesEqualForNonEqualNumbers() {
        final Setting setting1 = Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(1))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(2))
                .build();

        assertFalse(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 equal string values.
     */
    @Test
    public void testAreValuesEqualForEqualStrings() {
        final Setting setting1 = Setting.newBuilder()
                .setStringSettingValue(StringSettingValue.newBuilder().setValue("1"))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setStringSettingValue(StringSettingValue.newBuilder().setValue("1"))
                .build();

        assertTrue(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 not equal string values.
     */
    @Test
    public void testAreValuesEqualForNonEqualStrings() {
        final Setting setting1 = Setting.newBuilder()
                .setStringSettingValue(StringSettingValue.newBuilder().setValue("1"))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setStringSettingValue(StringSettingValue.newBuilder().setValue("2"))
                .build();

        assertFalse(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 equal enum values.
     */
    @Test
    public void testAreValuesEqualForEqualEnums() {
        final Setting setting1 = Setting.newBuilder()
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("1"))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("1"))
                .build();

        assertTrue(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 not equal enum values.
     */
    @Test
    public void testAreValuesEqualForNonEqualEnums() {
        final Setting setting1 = Setting.newBuilder()
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("1"))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("2"))
                .build();

        assertFalse(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 equal
     * {@code SortedSetOfOidSetting} values.
     */
    @Test
    public void testAreValuesEqualForEqualSortedSetOfOids() {
        final Setting setting1 = Setting.newBuilder()
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue
                        .newBuilder()
                        .addOids(1)
                        .addOids(2))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue
                        .newBuilder()
                        .addOids(1)
                        .addOids(2))
                .build();

        assertTrue(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 not equal
     * {@code SortedSetOfOidSetting} values.
     */
    @Test
    public void testAreValuesEqualForNonEqualSortedSetOfOids() {
        final Setting setting1 = Setting.newBuilder()
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue
                        .newBuilder()
                        .addOids(1)
                        .addOids(2))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue
                        .newBuilder()
                        .addOids(2)
                        .addOids(1))
                .build();

        assertFalse(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    /**
     * Test {@code SettingDTOUtil#areValuesEqual} method for 2 different value types.
     */
    @Test
    public void testAreValuesEqualForDifferentValueTypes() {
        final Setting setting1 = Setting.newBuilder()
                .setStringSettingValue(StringSettingValue.newBuilder().setValue("1"))
                .build();
        final Setting setting2 = Setting.newBuilder()
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("1"))
                .build();

        assertFalse(SettingDTOUtil.areValuesEqual(setting1, setting2));
    }

    private static EnumSettingValue createEnumSettingValue(String value) {
        return EnumSettingValue.newBuilder()
                .setValue(value)
                .build();
    }
}
