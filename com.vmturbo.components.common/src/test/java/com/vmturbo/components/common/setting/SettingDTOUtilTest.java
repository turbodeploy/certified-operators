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

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;

/**
 * Unit tests for {@link SettingDTOUtil}.
 */
public class SettingDTOUtilTest {

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

    private EnumSettingValue createEnumSettingValue(String value) {
        return EnumSettingValue.newBuilder()
                .setValue(value)
                .build();
    }
}
