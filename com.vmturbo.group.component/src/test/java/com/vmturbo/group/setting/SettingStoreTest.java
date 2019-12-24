package com.vmturbo.group.setting;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ProtocolStringList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.ScopeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.SettingValueTypeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableSettingPolicyUpdateException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.group.DbCleanupRule;
import com.vmturbo.group.group.DbConfigurationRule;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.schedule.ScheduleStore;
import com.vmturbo.group.schedule.ScheduleValidator;

/**
 * Unit test for {@link SettingStore}.
 */
public class SettingStoreTest {

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule("group_component");
    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = new DbCleanupRule(dbConfig, GroupComponent.GROUP_COMPONENT);

    private final Logger logger = LogManager.getLogger();

    private SettingStore settingStore;
    private SettingSpecStore settingSpecStore;
    private ScheduleStore scheduleStore;
    private final ScheduleValidator scheduleValidator = mock(ScheduleValidator.class);

    private final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
            .setName("test")
            .addAllSettings(Arrays.asList(Setting.newBuilder().setSettingSpecName("TestSetting").build()))
            .build();

    private static final String SETTING_TEST_JSON_SETTING_SPEC_JSON =
            "setting-test-json/setting-spec.json";

    private SettingPolicyValidator settingPolicyValidator = mock(SettingPolicyValidator.class);

    private IdentityProvider identityProviderSpy = spy(new IdentityProvider(0));

    private SettingsUpdatesSender settingsUpdatesSender = mock(SettingsUpdatesSender.class);

    @Before
    public void setUp() {
        settingSpecStore = new FileBasedSettingsSpecStore(SETTING_TEST_JSON_SETTING_SPEC_JSON);
        settingStore =
                new SettingStore(settingSpecStore, dbConfig.getDslContext(), identityProviderSpy,
                        settingPolicyValidator, settingsUpdatesSender);
        scheduleStore = new ScheduleStore(dbConfig.getDslContext(), scheduleValidator,
            identityProviderSpy, settingStore);
    }

    @Test(expected = DuplicateNameException.class)
    public void testCreateDuplicatePolicy() throws Exception {
        settingStore.createUserSettingPolicy(info);
        settingStore.createUserSettingPolicy(info);
    }

    @Test(expected = InvalidItemException.class)
    public void testCreateInvalidSetting() throws Exception {
        doThrow(new InvalidItemException("ERROR"))
            .when(settingPolicyValidator).validateSettingPolicy(any(), any());
        settingStore.createUserSettingPolicy(info);
    }

    @Test
    public void testCreateUserThenGetById() throws Exception {
        SettingPolicy policy = settingStore.createUserSettingPolicy(info);

        assertEquals(info, policy.getInfo());
        assertTrue(policy.hasId());

        Optional<SettingPolicy> gotPolicy = settingStore.getSettingPolicy(policy.getId());
        assertTrue(gotPolicy.isPresent());
        assertEquals(policy, gotPolicy.get());
    }

    /**
     * Test create user setting policy with schedule.
     *
     * @throws Exception If any exceptions
     */
    @Test
    public void testCreateUserWithScheduleThenGetById() throws Exception {
        Schedule schedule1 = Schedule.newBuilder()
            .setDisplayName("Test schedule1")
            .setStartTime(1446760800000L)
            .setEndTime(1446766200000L)
            .setPerpetual(Perpetual.newBuilder().build())
            .setTimezoneId("Test timezone")
            .build();
        final Schedule savedSchedule = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule.hasId());

        long scheduleId = savedSchedule.getId();
        SettingPolicyInfo infoWithSchedule = info.toBuilder().setScheduleId(scheduleId).build();
        SettingPolicy policy = settingStore.createUserSettingPolicy(infoWithSchedule);

        assertEquals(infoWithSchedule, policy.getInfo());
        assertTrue(policy.hasId());

        Optional<SettingPolicy> savedPolicy = settingStore.getSettingPolicy(policy.getId());
        assertTrue(savedPolicy.isPresent());
        assertEquals(policy, savedPolicy.get());
        assertTrue(savedPolicy.get().getInfo().hasScheduleId());
        assertEquals(scheduleId, savedPolicy.get().getInfo().getScheduleId());
    }

    @Test
    public void testGetByIdMissing() {
        assertFalse(settingStore.getSettingPolicy(10).isPresent());
    }

    @Test
    public void testGetByNameMissing() {
        assertFalse(settingStore.getSettingPolicy(info.getName()).isPresent());
    }

    @Test
    public void testCreateThenGetByName() throws Exception {
        when(identityProviderSpy.next()).thenReturn(7L);
        SettingPolicy policy = settingStore.createUserSettingPolicy(info);

        assertEquals(info, policy.getInfo());
        assertEquals(7L, policy.getId());
        assertEquals(Type.USER, policy.getSettingPolicyType());

        Optional<SettingPolicy> gotPolicy = settingStore.getSettingPolicy(info.getName());
        assertTrue(gotPolicy.isPresent());
        assertEquals(policy, gotPolicy.get());
    }

    private final SettingPolicyInfo updatedInfo = SettingPolicyInfo.newBuilder(info)
            .setName("test2")
            .build();

    @Test
    public void testUpdateSettingPolicy() throws Exception {

        final SettingPolicy policy = settingStore.createUserSettingPolicy(info);

        final SettingPolicy updatedPolicy =
                settingStore.updateSettingPolicy(policy.getId(), updatedInfo);
        assertEquals(updatedInfo, updatedPolicy.getInfo());
        Optional<SettingPolicy> gotPolicy = settingStore.getSettingPolicy(policy.getId());
        assertTrue(gotPolicy.isPresent());
        assertEquals(updatedPolicy, gotPolicy.get());
    }

    /**
     * Test update settings policy with schedule.
     *
     * @throws Exception If any exceptions.
     */
    @Test
    public void testUpdateSettingPolicyUpdateSchedule() throws Exception {
        Schedule schedule1 = Schedule.newBuilder()
            .setDisplayName("Test schedule1")
            .setStartTime(1446760800000L)
            .setEndTime(1446766200000L)
            .setPerpetual(Perpetual.newBuilder().build())
            .setTimezoneId("Test timezone")
            .build();
        final Schedule savedSchedule1 = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule1.hasId());

        Schedule schedule2 = schedule1.toBuilder().setDisplayName("Test schedule2").build();
        final Schedule savedSchedule2 = scheduleStore.createSchedule(schedule2);
        assertTrue(savedSchedule2.hasId());

        long scheduleId1 = savedSchedule1.getId();
        SettingPolicyInfo infoWithSchedule = info.toBuilder().setScheduleId(scheduleId1).build();
        SettingPolicy policy = settingStore.createUserSettingPolicy(infoWithSchedule);

        Optional<SettingPolicy> savedPolicy = settingStore.getSettingPolicy(policy.getId());
        assertTrue(savedPolicy.isPresent());
        assertEquals(policy, savedPolicy.get());
        assertTrue(savedPolicy.get().getInfo().hasScheduleId());
        assertEquals(scheduleId1, savedPolicy.get().getInfo().getScheduleId());

        long scheduleId2 = savedSchedule2.getId();
        SettingPolicyInfo updatedInfoWithSchedule = savedPolicy.get().getInfo().toBuilder()
            .setScheduleId(scheduleId2).build();
        SettingPolicy updatedPolicy = policy.toBuilder().setInfo(updatedInfoWithSchedule).build();
        SettingPolicy updatedSavedPolicy = settingStore.updateSettingPolicy(savedPolicy.get().getId(),
            updatedInfoWithSchedule);
        assertEquals(updatedPolicy, updatedSavedPolicy);
        assertEquals(scheduleId2, updatedSavedPolicy.getInfo().getScheduleId());
    }

    @Test(expected = SettingPolicyNotFoundException.class)
    public void testUpdateSettingPolicyNotFound() throws Exception {
        settingStore.updateSettingPolicy(7, info);
    }

    @Test(expected = InvalidItemException.class)
    public void testUpdateSettingPolicyWithInvalidInfo() throws Exception {
        final SettingPolicy policy = settingStore.createUserSettingPolicy(info);

        doThrow(new InvalidItemException(""))
            .when(settingPolicyValidator).validateSettingPolicy(eq(updatedInfo), any());

        settingStore.updateSettingPolicy(policy.getId(), updatedInfo);
    }

    @Test(expected = DuplicateNameException.class)
    public void testUpdateSettingPolicyToDuplicateName() throws Exception {
        final SettingPolicy policy = settingStore.createUserSettingPolicy(info);

        // Make sure there is another setting policy with the same name as the updated info.
        settingStore.createUserSettingPolicy(
            SettingPolicyInfo.newBuilder()
                .setName(updatedInfo.getName())
                .build());

        settingStore.updateSettingPolicy(policy.getId(), updatedInfo);
    }

    @Test
    public void testResetSettingPolicy() throws Exception {
        final Map<Integer, SettingPolicyInfo> defaultSettingPolicies =
            DefaultSettingPolicyCreator.defaultSettingPoliciesFromSpecs(
                settingSpecStore.getAllSettingSpecs());
        final SettingPolicyInfo vmSettingPolicy = defaultSettingPolicies.get(10);
        final SettingProto.SettingPolicy settingPolicy = settingStore.createDefaultSettingPolicy(
            vmSettingPolicy.toBuilder()
                .setName("testName")
                .build());

        assertThat(settingPolicy, not(vmSettingPolicy));

        final SettingProto.SettingPolicy postResetPolicy =
                settingStore.resetSettingPolicy(settingPolicy.getId());
        assertThat(postResetPolicy.getId(), is(settingPolicy.getId()));
        assertThat(postResetPolicy.getInfo(), is(vmSettingPolicy));
    }

    @Test(expected = SettingPolicyNotFoundException.class)
    public void testResetSettingPolicyNotFound() throws Exception {
        settingStore.resetSettingPolicy(7L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResetNonDefaultSettingPolicy() throws Exception {
        final SettingPolicy policy = settingStore.createUserSettingPolicy(info);
        settingStore.resetSettingPolicy(policy.getId());
    }

    @Test
    public void testCreateAndGetDefaultSettingPolicy() throws Exception {
        when(identityProviderSpy.next()).thenReturn(7L);
        final SettingPolicy policy = settingStore.createDefaultSettingPolicy(info);
        assertEquals(7L, policy.getId());
        assertEquals(info, policy.getInfo());
        assertEquals(Type.DEFAULT, policy.getSettingPolicyType());

        final Optional<SettingPolicy> gotPolicy =
                settingStore.getSettingPolicy(policy.getId());
        assertTrue(gotPolicy.isPresent());
        assertEquals(policy, gotPolicy.get());
    }

    @Test
    public void testUpdateDefaultSettingPolicy() throws Exception {
        final String settingName = "set me";
        final SettingPolicyInfo updatedInfo = info.toBuilder()
            .addSettings(Setting.newBuilder()
                    .setSettingSpecName(settingName)
                    .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                    .build())
            .build();
        final SettingPolicy policy =
                settingStore.createDefaultSettingPolicy(info);
        final SettingPolicy updatedPolicy = settingStore.updateSettingPolicy(policy.getId(),
            updatedInfo);

        assertEquals(updatedInfo, updatedPolicy.getInfo());
        assertEquals(policy.getId(), updatedPolicy.getId());

        final Optional<SettingPolicy> gotPolicy =
                settingStore.getSettingPolicy(updatedPolicy.getId());
        assertTrue(gotPolicy.isPresent());
        assertEquals(updatedPolicy, gotPolicy.get());
    }

    @Test(expected = InvalidItemException.class)
    public void testUpdateDefaultSettingPolicyChangeEntityTypeFail() throws Exception {
        final SettingPolicy policy =
                settingStore.createDefaultSettingPolicy(info);
        settingStore.updateSettingPolicy(policy.getId(),
                policy.getInfo().toBuilder().setEntityType(9001).build());
    }

    @Test(expected = InvalidItemException.class)
    public void testUpdateDefaultSettingPolicyChangeNameFail() throws Exception {
        final SettingPolicy policy =
                settingStore.createDefaultSettingPolicy(info);
        settingStore.updateSettingPolicy(policy.getId(),
                policy.getInfo().toBuilder().setName("blah").build());
    }

    @Test(expected = InvalidItemException.class)
    public void testUpdateDiscoveredSettingPolicyFail() throws Exception {
        final SettingPolicy policy = settingStore.createDiscoveredSettingPolicy(info);
        settingStore.updateSettingPolicy(policy.getId(), policy.getInfo());
    }

    @Test(expected = InvalidItemException.class)
    public void testUpdateDefaultSettingPolicyChangeDefaultSettings() throws Exception {
        //Throws an exception since one of the settings is missing in the updated SettingPolicyInfo
        final SettingPolicy policy =
            settingStore.createDefaultSettingPolicy(info);
        settingStore.updateSettingPolicy(policy.getId(),
            SettingPolicyInfo.newBuilder().setName(policy.getInfo().getName()).build());
    }

    @Test
    public void testDeleteSettingPolicy() throws Exception {
        final SettingPolicy policy = settingStore.createUserSettingPolicy(info);
        final SettingPolicy deletedPolicy = settingStore.deleteUserSettingPolicy(policy.getId());
        assertEquals(policy, deletedPolicy);
        assertFalse(settingStore.getSettingPolicy(policy.getId()).isPresent());
    }

    @Test(expected = ImmutableSettingPolicyUpdateException.class)
    public void testDeleteDefaultFail() throws Exception {
        final SettingPolicy policy = settingStore.createDefaultSettingPolicy(info);
        settingStore.deleteUserSettingPolicy(policy.getId());
    }

    @Test(expected = ImmutableSettingPolicyUpdateException.class)
    public void testDeleteDiscoveredFailWhenByUser() throws Exception {
        final SettingPolicy policy = settingStore.createDiscoveredSettingPolicy(info);
        settingStore.deleteUserSettingPolicy(policy.getId());
    }

    public void testDeleteDiscoveredSuccessWhenNotByUser() throws Exception {
        final SettingPolicy policy = settingStore.createDiscoveredSettingPolicy(info);
        assertTrue(settingStore.getSettingPolicy(policy.getId()).isPresent());

        settingStore.deleteUserSettingPolicy(policy.getId());
        assertFalse(settingStore.getSettingPolicy(policy.getId()).isPresent());
    }

    @Test(expected = SettingPolicyNotFoundException.class)
    public void testDeleteNotFound() throws Exception {
        settingStore.deleteUserSettingPolicy(77);
    }

    @Test
    public void testGetAllPolicies() throws Exception {
        final SettingPolicy policy = settingStore.createUserSettingPolicy(info);
        final SettingPolicy policy2 = settingStore.createUserSettingPolicy(SettingPolicyInfo.newBuilder()
            .setName("bar")
            .build());

        List<SettingPolicy> allPolicies =
                settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder().build())
                    .collect(Collectors.toList());
        assertTrue(allPolicies.contains(policy));
        assertTrue(allPolicies.contains(policy2));
    }

    @Test
    public void testGetSettingSpec() {
        // this test is depending on the JSON file that the store is parsing, so altering it
        // might make this test to fail

        String existingSpecName1 = "name1";
        String existingSpecName2 = "name2";

        String existingNodeName = "Automation";

        String numericSettingValueTypeString = "NUMERIC_SETTING_VALUE_TYPE";
        String enumSettingValueTypeString = "ENUM_SETTING_VALUE_TYPE";

        String allEntityTypeString = "ALL_ENTITY_TYPE";
        String specificEntityTypeString = "ENTITY_TYPE_SET";

        float min = 1.0f;
        float max = 99.9f;
        float delta = 0.001f;

        String fooEnumString = "FOO";
        String barEnumString = "BAR";

        // get 1st spec
        Optional<SettingSpec> specOpt = settingSpecStore.getSettingSpec(existingSpecName1);
        assertTrue(specOpt.isPresent());
        SettingSpec retrievedSettingSpec = specOpt.get();


        SettingCategoryPath path = retrievedSettingSpec.getPath();
        SettingCategoryPathNode rootPathNode = path.getRootPathNode();
        String nodeName = rootPathNode.getNodeName();
        SettingValueTypeCase settingValueTypeCase = retrievedSettingSpec.getSettingValueTypeCase();

        EntitySettingSpec entitySettingSpec = retrievedSettingSpec.getEntitySettingSpec();
        SettingTiebreaker tiebreaker = entitySettingSpec.getTiebreaker();

        EntitySettingScope entitySettingScope = entitySettingSpec.getEntitySettingScope();
        ScopeCase scopeCase = entitySettingScope.getScopeCase();

        assertNotNull(retrievedSettingSpec);
        assertNotNull(path);
        assertNotNull(rootPathNode);
        assertEquals(nodeName, existingNodeName);
        assertEquals(settingValueTypeCase.name(), numericSettingValueTypeString);

        NumericSettingValueType numericSettingValueType = retrievedSettingSpec.getNumericSettingValueType();
        assertEquals(min, numericSettingValueType.getMin(), delta);
        assertEquals(max, numericSettingValueType.getMax(), delta);

        assertEquals(tiebreaker, SettingTiebreaker.BIGGER);
        assertEquals(scopeCase.name(), allEntityTypeString);


        // get 2nd spec
        Optional<SettingSpec> specOpt2 = settingSpecStore.getSettingSpec(existingSpecName2);
        assertTrue(specOpt2.isPresent());
        SettingSpec retrievedSettingSpec2 = specOpt2.get();

        SettingValueTypeCase settingValueTypeCase2 = retrievedSettingSpec2.getSettingValueTypeCase();

        EntitySettingSpec entitySettingSpec2 = retrievedSettingSpec2.getEntitySettingSpec();
        SettingTiebreaker tiebreaker2 = entitySettingSpec2.getTiebreaker();

        EntitySettingScope entitySettingScope2 = entitySettingSpec2.getEntitySettingScope();
        ScopeCase scopeCase2 = entitySettingScope2.getScopeCase();
        EntityTypeSet entityTypeSet = entitySettingScope2.getEntityTypeSet();
        List<Integer> entityTypeList = entityTypeSet.getEntityTypeList();

        EnumSettingValueType enumSettingValueType = retrievedSettingSpec2.getEnumSettingValueType();
        ProtocolStringList enumValuesList = enumSettingValueType.getEnumValuesList();

        assertEquals(settingValueTypeCase2.name(), enumSettingValueTypeString);
        assertEquals(tiebreaker2, SettingTiebreaker.SMALLER);
        assertEquals(scopeCase2.name(), specificEntityTypeString);
        assertTrue(entityTypeList.contains(1));
        assertTrue(entityTypeList.contains(3));
        assertTrue(entityTypeList.contains(5));
        assertTrue(enumValuesList.contains(fooEnumString));
        assertTrue(enumValuesList.contains(barEnumString));

    }

    @Test
    public void testGetAllSettingSpec() {
        // this test is depending on the JSON file that the store is parsing, so altering it
        // might make this test to fail

        Collection<SettingSpec> retrievedSettingSpecs = settingSpecStore.getAllSettingSpecs();
        assertEquals(retrievedSettingSpecs.size(), 4);
    }

    @Test
    public void testUpdateTargetSettingPoliciesUpdate() {
        final long targetId = 12345L;
        final Setting originalSetting = Setting.newBuilder()
                .setSettingSpecName("some-spec")
                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    .setValue(true))
                .build();
        final Setting updatedSetting = originalSetting.toBuilder()
                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                        .setValue(false))
                .build();

        final DiscoveredSettingPolicyInfo spInfo = DiscoveredSettingPolicyInfo.newBuilder()
                .setEntityType(1)
                .setName("a")
                .addDiscoveredGroupNames("0-group-a")
                .build();

        final Map<String, Long> groupOids = ImmutableMap.of(spInfo.getDiscoveredGroupNames(0), 1L);

        // Create a discovered setting policies, a and b.
        settingStore.updateTargetSettingPolicies(dbConfig.getDslContext(), targetId,
                Collections.singletonList(spInfo.toBuilder().addSettings(originalSetting).build()),
                groupOids);

        final SettingPolicy policy = settingStore.getSettingPolicy(spInfo.getName()).get();

        assertThat(policy.getInfo().getSettings(0), is(originalSetting));
        settingStore.updateTargetSettingPolicies(dbConfig.getDslContext(), targetId,
                Collections.singletonList(spInfo.toBuilder().addSettings(updatedSetting).build()),
                groupOids);

        final SettingPolicy updatedPolicy = settingStore.getSettingPolicy(policy.getId()).get();
        assertThat(updatedPolicy.getInfo().getSettings(0), is(updatedSetting));
    }

    @Test
    public void testUpdateTargetSettingPolicies() {
        final long targetId = 12345L;
        final Setting setting = Setting.newBuilder()
            .setSettingSpecName("some-spec")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();

        final DiscoveredSettingPolicyInfo settingPolicyA = DiscoveredSettingPolicyInfo.newBuilder()
            .setEntityType(1)
            .setName("a")
            .addSettings(setting)
            .addDiscoveredGroupNames("0-group-a")
            .build();

        final DiscoveredSettingPolicyInfo settingPolicyB = DiscoveredSettingPolicyInfo.newBuilder()
            .setEntityType(2)
            .setName("b")
            .addSettings(setting)
            .addDiscoveredGroupNames("0-group-b")
            .build();

        final DiscoveredSettingPolicyInfo settingPolicyC = DiscoveredSettingPolicyInfo.newBuilder()
            .setEntityType(1)
            .setName("c")
            .addSettings(setting)
            .addDiscoveredGroupNames("0-group-c")
            .build();

        final Map<String, Long> groupOids = ImmutableMap.of(
                settingPolicyA.getDiscoveredGroupNames(0), 1L,
                settingPolicyB.getDiscoveredGroupNames(0), 2L,
                settingPolicyC.getDiscoveredGroupNames(0), 3L);

        // Create 2 discovered setting policies, a and b.
        settingStore.updateTargetSettingPolicies(dbConfig.getDslContext(), targetId,
            Arrays.asList(settingPolicyA, settingPolicyB),
            groupOids);

        // Make sure they are created.
        final Collection<SettingPolicy> firstCreated =
                settingStore.getSettingPoliciesDiscoveredByTarget(dbConfig.getDslContext(),
                        targetId);
        assertEquals(targetId, (long)firstCreated.stream()
            .map(settingPolicy -> settingPolicy.getInfo().getTargetId())
            .distinct()
            .findFirst()
            .get());
        assertThat(firstCreated.stream()
            .flatMap(settingPolicy -> settingPolicy.getInfo().getScope().getGroupsList().stream())
            .collect(Collectors.toList()), containsInAnyOrder(1L, 2L));
        assertThat(firstCreated.stream()
            .map(settingPolicy -> settingPolicy.getInfo().getName())
            .collect(Collectors.toList()), containsInAnyOrder("a", "b"));

        // Have 2 discovered setting policies, b and c.
        // a should be deleted, b should be retained, c should be created.
        settingStore.updateTargetSettingPolicies(dbConfig.getDslContext(), targetId,
            Arrays.asList(settingPolicyC, settingPolicyB),
            groupOids);

        final Collection<SettingPolicy> secondCreated =
                settingStore.getSettingPoliciesDiscoveredByTarget(dbConfig.getDslContext(),
                        targetId);
        assertEquals(targetId, (long)secondCreated.stream()
            .map(settingPolicy -> settingPolicy.getInfo().getTargetId())
            .distinct()
            .findFirst()
            .get());
        assertThat(secondCreated.stream()
            .flatMap(settingPolicy -> settingPolicy.getInfo().getScope().getGroupsList().stream())
            .collect(Collectors.toList()), containsInAnyOrder(2L, 3L));
        assertThat(secondCreated.stream()
            .map(settingPolicy -> settingPolicy.getInfo().getName())
            .collect(Collectors.toList()), containsInAnyOrder("b", "c"));
    }

    @Test
    public void testSettingPolicyScopeAdjustmentAfterGroupRemoval()
            throws InvalidItemException, DuplicateNameException {
        settingStore.createUserSettingPolicy(SettingPolicyInfo.newBuilder()
                .setName("policy1")
                .setScope(Scope.newBuilder().addGroups(1L))
                .build());
        settingStore.createUserSettingPolicy(SettingPolicyInfo.newBuilder()
                .setName("policy12")
                .setScope(Scope.newBuilder().addGroups(1L).addGroups(2L))
                .build());
        settingStore.createUserSettingPolicy(SettingPolicyInfo.newBuilder()
                .setName("policy2")
                .setScope(Scope.newBuilder().addGroups(2L))
                .build());

        // verify that policy1 should still have one scope group
        final SettingPolicy policy1FromDB = settingStore.getSettingPolicy("policy1").get();
        assertEquals(1, policy1FromDB.getInfo().getScope().getGroupsList().size());

        // verify that policy12 should still have two groups in scope
        final SettingPolicy policy12FromDB = settingStore.getSettingPolicy("policy12").get();
        assertEquals(2, policy12FromDB.getInfo().getScope().getGroupsList().size());

        // when we remove group 1 and it's a user-created group, we should see two policies updated
        // -- policy1 and policy12
        assertEquals(2, settingStore.onGroupDeleted(dbConfig.getDslContext(), 1L));

        // verify that policy1 no longer has any scope groups
        final SettingPolicy policy1AfterUpdate = settingStore.getSettingPolicy("policy1").get();
        assertEquals(0, policy1AfterUpdate.getInfo().getScope().getGroupsList().size());

        // verify that policy12 should still have one group in scope
        final SettingPolicy policy12AfterUpdate = settingStore.getSettingPolicy("policy12").get();
        assertEquals(1, policy12AfterUpdate.getInfo().getScope().getGroupsList().size());
    }

    /**
     * Test get setting policies using schedule.
     *
     * @throws Exception If any exceptions thrown during test execution.
     */
    @Test
    public void testGetSettingPoliciesUsingSchedule() throws Exception {
        Schedule schedule1 = Schedule.newBuilder()
            .setDisplayName("Test schedule1")
            .setStartTime(1446760800000L)
            .setEndTime(1446766200000L)
            .setPerpetual(Perpetual.newBuilder().build())
            .setTimezoneId("Test timezone")
            .build();
        final Schedule savedSchedule = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule.hasId());

        // No policies using this schedule yet
        List<SettingPolicy> settingPolicies = settingStore.getSettingPoliciesUsingSchedule(
            savedSchedule.getId()).collect(Collectors.toList());
        assertTrue(settingPolicies.isEmpty());

        final SettingPolicy policy1 = settingStore.createUserSettingPolicy(info);
        assertTrue(policy1.hasId());
        scheduleStore.assignScheduleToSettingPolicy(policy1.getId(), savedSchedule.getId());

        final SettingPolicyInfo info2 = info.toBuilder().setName("test2").build();
        final SettingPolicy policy2 = settingStore.createUserSettingPolicy(info2);
        assertTrue(policy2.hasId());
        assertNotEquals(policy1.getId(), policy2.getId());
        scheduleStore.assignScheduleToSettingPolicy(policy2.getId(), savedSchedule.getId());

        // Now 2 policies should be using this schedule
        settingPolicies = settingStore.getSettingPoliciesUsingSchedule(
            savedSchedule.getId()).collect(Collectors.toList());
        assertEquals(2, settingPolicies.size());
        settingPolicies.forEach(sPolicy -> assertTrue(sPolicy.getInfo().hasScheduleId()));

        // Add 1 more policy without schedule
        final SettingPolicyInfo info3 = info.toBuilder().setName("test3").build();
        final SettingPolicy policy3 = settingStore.createUserSettingPolicy(info3);
        assertTrue(policy3.hasId());

        List<SettingPolicy> allPolicies =
            settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder().build())
                .collect(Collectors.toList());
        assertEquals(3, allPolicies.size());

        // Still 2 policies should be using this schedule
        settingPolicies = settingStore.getSettingPoliciesUsingSchedule(
            savedSchedule.getId()).collect(Collectors.toList());
        assertEquals(2, settingPolicies.size());
        settingPolicies.forEach(sPolicy -> assertTrue(sPolicy.getInfo().hasScheduleId()));

    }
}
