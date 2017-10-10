package com.vmturbo.common.protobuf;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.setting.SettingProto.DefaultType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.ScopeType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
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
                        .setScope(ScopeType.newBuilder()
                                .addGroups(7L)))
                .build();
        final SettingPolicy policy2 = SettingPolicy.newBuilder()
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setScope(ScopeType.newBuilder()
                                .addGroups(8L)))
                .build();
        final Set<Long> involvedGroups =
                SettingDTOUtil.getInvolvedGroups(Arrays.asList(policy1, policy2));
        assertEquals(Sets.newHashSet(7L, 8L), involvedGroups);
    }

    @Test
    public void testInvolvedGroupsDefault() {
        final SettingPolicy policy1 = SettingPolicy.newBuilder()
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setDefault(DefaultType.newBuilder()))
                .build();
        final Set<Long> involvedGroups =
                SettingDTOUtil.getInvolvedGroups(Collections.singletonList(policy1));
        assertTrue(involvedGroups.isEmpty());
    }

    @Test
    public void testInvolvedGroupsSingle() {
        final SettingPolicy policy1 = SettingPolicy.newBuilder()
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setScope(ScopeType.newBuilder()
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
}
