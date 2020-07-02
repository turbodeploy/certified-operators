package com.vmturbo.group.setting;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.identity.IdentityProvider;

/**
 * Unit test for {@link DiscoveredSettingPoliciesUpdater}.
 */
public class DiscoveredSettingPoliciesUpdaterTest {

    private static final long TARGET_ID = 12345L;
    private static final long GROUP_ID = 100L;
    private static final long OID1 = 2L;
    private static final long OID2 = 3L;

    private static final String SETTING_NAME = "exclusions";
    private static final Setting UPDATED_SETTING = Setting.newBuilder()
            .setSettingSpecName(SETTING_NAME)
            .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(false))
            .build();
    private static final String GROUP_NAME = "all the students";
    private static final String POLICY_NAME_1 = "Do Not Enter The Forbidden Forest";
    private static final String POLICY_NAME_2 =
            "The Students' Belongings Must Be Searched For Contraband";
    private static final String POLICY_NAME_3 = "Butterbeer Is Not Allowed On School Grounds";

    private static final Table<Long, String, Long> GROUP_IDS =
            ImmutableTable.<Long, String, Long>builder().put(TARGET_ID, GROUP_NAME, GROUP_ID)
                    .build();

    private ISettingPolicyStore settingPolicyStore;
    private DiscoveredSettingPoliciesUpdater updater;
    private IdentityProvider identityProvider;
    @Captor
    private ArgumentCaptor<Collection<SettingPolicy>> newPolicies;
    @Captor
    private ArgumentCaptor<Collection<Long>> deletedPolicies;

    /**
     * Initializes the test.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        identityProvider = Mockito.spy(new IdentityProvider(0));
        settingPolicyStore = Mockito.mock(ISettingPolicyStore.class);
        updater = new DiscoveredSettingPoliciesUpdater(identityProvider);
    }

    /**
     * Tests creation of a new discovered setting policy. It is expected to be requested for
     * addition.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void createNewPolicy() throws Exception {
        final DiscoveredSettingPolicyInfo spInfo = DiscoveredSettingPolicyInfo.newBuilder()
                .setEntityType(1)
                .setName(POLICY_NAME_1)
                .addDiscoveredGroupNames(GROUP_NAME)
                .addSettings(UPDATED_SETTING)
                .build();

        Mockito.when(settingPolicyStore.getDiscoveredPolicies()).thenReturn(Collections.emptyMap());
        Mockito.when(identityProvider.next()).thenReturn(44L);
        updater.updateSettingPolicies(settingPolicyStore,
                Collections.singletonMap(TARGET_ID, Collections.singletonList(spInfo)), GROUP_IDS,
                Collections.emptySet());

        Mockito.verify(settingPolicyStore)
                .deletePolicies(Collections.emptySet(), SettingPolicy.Type.DISCOVERED);
        Mockito.verify(settingPolicyStore).createSettingPolicies(newPolicies.capture());
        Assert.assertEquals(1, newPolicies.getValue().size());
        final SettingPolicy newPolicy = newPolicies.getValue().iterator().next();
        Assert.assertEquals(44L, newPolicy.getId());
        Assert.assertEquals(spInfo.getEntityType(), newPolicy.getInfo().getEntityType());
        Assert.assertEquals(spInfo.getName(), newPolicy.getInfo().getName());
        Assert.assertEquals(Collections.singletonList(GROUP_ID),
                newPolicy.getInfo().getScope().getGroupsList());
        Assert.assertEquals(Collections.singletonList(UPDATED_SETTING),
                newPolicy.getInfo().getSettingsList());
        Assert.assertEquals(Type.DISCOVERED, newPolicy.getSettingPolicyType());
    }

    /**
     * Tests updating existing setting policy. It is expected that policy 2 is treated as unchanged.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testUpdateTargetSettingPoliciesUpdate() throws Exception {
        final DiscoveredSettingPolicyInfo spInfo1 = DiscoveredSettingPolicyInfo.newBuilder()
                .setEntityType(1)
                .setName(POLICY_NAME_1)
                .addDiscoveredGroupNames(GROUP_NAME)
                .addSettings(UPDATED_SETTING)
                .build();
        final DiscoveredSettingPolicyInfo spInfo2 = DiscoveredSettingPolicyInfo.newBuilder()
                .setEntityType(2)
                .setName(POLICY_NAME_2)
                .addDiscoveredGroupNames(GROUP_NAME)
                .addSettings(UPDATED_SETTING)
                .build();
        final SettingPolicyInfo spInfoInt2 = new DiscoveredSettingPoliciesMapper(TARGET_ID,
                GROUP_IDS.row(TARGET_ID)).mapToSettingPolicyInfo(spInfo2).get();
        final byte[] policy2Hash = SettingPolicyHash.hash(spInfoInt2);

        Mockito.when(settingPolicyStore.getDiscoveredPolicies()).thenReturn(
                Collections.singletonMap(TARGET_ID,
                        ImmutableMap.of(POLICY_NAME_1, new DiscoveredObjectVersionIdentity(OID1, null),
                                POLICY_NAME_2, new DiscoveredObjectVersionIdentity(OID2, policy2Hash))));
        updater.updateSettingPolicies(settingPolicyStore,
                Collections.singletonMap(TARGET_ID, Arrays.asList(spInfo1, spInfo2)), GROUP_IDS,
                Collections.emptySet());

        Mockito.verify(settingPolicyStore)
                .deletePolicies(Collections.singleton(OID1), SettingPolicy.Type.DISCOVERED);
        Mockito.verify(settingPolicyStore).createSettingPolicies(newPolicies.capture());
        Assert.assertEquals(1, newPolicies.getValue().size());
        final SettingPolicy newPolicy = newPolicies.getValue().iterator().next();
        Assert.assertEquals(2L, newPolicy.getId());
        Assert.assertEquals(spInfo1.getEntityType(), newPolicy.getInfo().getEntityType());
        Assert.assertEquals(spInfo1.getName(), newPolicy.getInfo().getName());
        Assert.assertEquals(Collections.singletonList(GROUP_ID),
                newPolicy.getInfo().getScope().getGroupsList());
        Assert.assertEquals(Collections.singletonList(UPDATED_SETTING),
                newPolicy.getInfo().getSettingsList());
        Assert.assertEquals(Type.DISCOVERED, newPolicy.getSettingPolicyType());
    }

    /**
     * Tests the case when a set of initial policies differs from the set of new policies
     * reported by targets.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testPolicySetChanges() throws Exception {
        final DiscoveredSettingPolicyInfo spInfo1 = DiscoveredSettingPolicyInfo.newBuilder()
                .setEntityType(1)
                .setName(POLICY_NAME_2)
                .addDiscoveredGroupNames(GROUP_NAME)
                .addSettings(UPDATED_SETTING)
                .build();
        final DiscoveredSettingPolicyInfo spInfo2 = DiscoveredSettingPolicyInfo.newBuilder()
                .setEntityType(1)
                .setName(POLICY_NAME_3)
                .addDiscoveredGroupNames(GROUP_NAME)
                .addSettings(UPDATED_SETTING)
                .build();
        Mockito.when(settingPolicyStore.getDiscoveredPolicies())
                .thenReturn(Collections.singletonMap(TARGET_ID,
                        ImmutableMap.of(POLICY_NAME_1, new DiscoveredObjectVersionIdentity(1L, null),
                                POLICY_NAME_2, new DiscoveredObjectVersionIdentity(2L, null))));
        Mockito.when(identityProvider.next()).thenReturn(3L);
        updater.updateSettingPolicies(settingPolicyStore,
                Collections.singletonMap(TARGET_ID, Arrays.asList(spInfo1, spInfo2)), GROUP_IDS,
                Collections.emptySet());
        Mockito.verify(settingPolicyStore)
                .deletePolicies(deletedPolicies.capture(), Mockito.eq(Type.DISCOVERED));
        Assert.assertEquals(Sets.newHashSet(1L, 2L), new HashSet<>(deletedPolicies.getValue()));
        Mockito.verify(settingPolicyStore).createSettingPolicies(newPolicies.capture());
        Assert.assertEquals(Sets.newHashSet(2L, 3L), newPolicies.getValue()
                .stream()
                .map(SettingPolicy::getId)
                .collect(Collectors.toSet()));
    }
}
