package com.vmturbo.group.setting;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.ProtocolStringList;

import io.grpc.Status;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.ScopeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting.ValueCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.SettingValueTypeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemNotFoundException.SettingNotFoundException;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.ProtobufMessageMatcher;
import com.vmturbo.group.group.TestGroupGenerator;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.schedule.ScheduleStore;
import com.vmturbo.group.schedule.ScheduleValidator;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.group.setting.SettingStore.SettingAdapter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit test for {@link SettingStore}.
 */
public class SettingStoreTest {

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(GroupComponent.GROUP_COMPONENT);
    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();
    /**
     * Rule to expect exceptions, if required.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final Logger logger = LogManager.getLogger();

    private SettingStore settingStore;
    private SettingSpecStore settingSpecStore;
    private ScheduleStore scheduleStore;
    private GroupDAO groupStore;
    private final ScheduleValidator scheduleValidator = mock(ScheduleValidator.class);

    private final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
            .setName("test")
            .setDisplayName("Some very helpfull policy")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnabled(true)
            .setScope(Scope.newBuilder().build())
            .addSettings(Setting.newBuilder()
                    .setSettingSpecName("TestSetting")
                    .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(123.45F)))
            .build();
    private final SettingPolicyInfo updatedInfo = SettingPolicyInfo.newBuilder(info)
            .setName("test2")
            .build();
    private SettingPolicy userPolicy;

    private static final String SETTING_TEST_JSON_SETTING_SPEC_JSON =
            "setting-test-json/setting-spec.json";

    private SettingPolicyValidator settingPolicyValidator = mock(SettingPolicyValidator.class);

    private IdentityProvider identityProviderSpy = spy(new IdentityProvider(0));

    private SettingsUpdatesSender settingsUpdatesSender = mock(SettingsUpdatesSender.class);

    @Before
    public void setUp() {
        userPolicy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(info)
                .setSettingPolicyType(Type.USER)
                .build();
        settingSpecStore = new FileBasedSettingsSpecStore(SETTING_TEST_JSON_SETTING_SPEC_JSON);
        settingStore =
                new SettingStore(settingSpecStore, dbConfig.getDslContext(), settingPolicyValidator,
                        settingsUpdatesSender);
        groupStore = new GroupDAO(dbConfig.getDslContext());
        scheduleStore = new ScheduleStore(dbConfig.getDslContext(), scheduleValidator,
            identityProviderSpy, settingStore);
    }

    /**
     * Tests creating setting policies with duplicated names one by one - in different calls.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testCreateDuplicatePolicyOneByOne() throws Exception {
        final SettingPolicy policy1 = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(info)
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(policy1));
        final SettingPolicy policy2 = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(info)
                .setSettingPolicyType(Type.USER)
                .build();
        expectedException.expect(StoreOperationException.class);
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(policy2));
    }

    /**
     * Tests creating setting policies with duplicated names at once - in a single call.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testCreateDuplicatePolicyAtOnce() throws Exception {
        final SettingPolicy policy1 = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(info)
                .setSettingPolicyType(Type.USER)
                .build();
        final SettingPolicy policy2 = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(info)
                .setSettingPolicyType(Type.USER)
                .build();
        expectedException.expect(StoreOperationException.class);
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Arrays.asList(policy1, policy2));
    }

    /**
     * Tests creation of invalid setting.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testCreateInvalidSetting() throws Exception {
        doThrow(new InvalidItemException("ERROR"))
            .when(settingPolicyValidator).validateSettingPolicy(any(), any());
        expectedException.expect(StoreOperationException.class);
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(userPolicy));
    }

    @Test
    public void testCreateUserThenGetById() throws Exception {
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(userPolicy));
        final SettingPolicy policy = settingStore.getSettingPolicies(dbConfig.getDslContext(),
                SettingPolicyFilter.newBuilder().build()).iterator().next();

        Assert.assertEquals(userPolicy, policy);

        final Optional<SettingPolicy> gotPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), policy.getId());
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
        final Schedule schedule1 = createSchedule("Test schedule1");
        final Schedule savedSchedule = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule.hasId());

        long scheduleId = savedSchedule.getId();
        SettingPolicyInfo infoWithSchedule = info.toBuilder().setScheduleId(scheduleId).build();
        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(infoWithSchedule)
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy));

        final Optional<SettingPolicy> savedPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), policy.getId());
        assertTrue(savedPolicy.isPresent());
        assertEquals(policy, savedPolicy.get());
        assertTrue(savedPolicy.get().getInfo().hasScheduleId());
        assertEquals(scheduleId, savedPolicy.get().getInfo().getScheduleId());
    }

    /**
     * Test creating setting policy with execution schedule window configured through
     * ExecutionSchedule setting.
     *
     * @throws Exception it shouldn't happen
     */
    @Test
    public void testCreateSettingPolicyWithExecutionSchedule() throws Exception {
        final long groupId = 23L;
        createGroup(groupId);

        // create schedule - execution windows in policy
        final Schedule schedule1 = createSchedule("Test schedule1");
        final Schedule savedSchedule = scheduleStore.createSchedule(schedule1);

        final Setting actionModeSetting = Setting.newBuilder()
                .setEnumSettingValue(
                        EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.name()).build())
                .setSettingSpecName(
                        EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds.getSettingName())
                .build();

        final Setting executionScheduleSetting = Setting.newBuilder()
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addAllOids(Collections.singletonList(savedSchedule.getId()))
                        .build())
                .setSettingSpecName(
                        EntitySettingSpecs.ResizeVcpuDownInBetweenThresholdsExecutionSchedule.getSettingName())
                .build();

        final SettingPolicyInfo settingInfo = SettingPolicyInfo.newBuilder()
                .setName("policy_1")
                .setDisplayName("Policy 1")
                .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                .setEnabled(true)
                .setScope(Scope.newBuilder().addGroups(groupId).build())
                .addAllSettings(Arrays.asList(actionModeSetting, executionScheduleSetting))
                .build();

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(settingInfo)
                .setSettingPolicyType(Type.USER)
                .build();

        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(settingPolicy));

        final SettingPolicy policyFromDB = settingStore.getSettingPolicies(dbConfig.getDslContext(),
                SettingPolicyFilter.newBuilder().build()).iterator().next();
        final SettingPolicyInfo settingPolicyInfo = settingPolicy.getInfo();
        final SettingPolicyInfo policyFromDBInfo = policyFromDB.getInfo();
        Assert.assertEquals(settingPolicy.getId(), policyFromDB.getId());
        Assert.assertEquals(settingPolicyInfo.getEntityType(), policyFromDBInfo.getEntityType());
        Assert.assertEquals(settingPolicyInfo.getScope(), policyFromDBInfo.getScope());
        Assert.assertTrue(CollectionUtils.isEqualCollection(settingPolicyInfo.getSettingsList(),
                policyFromDBInfo.getSettingsList()));

        final Optional<SettingPolicy> getCertainPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), policyFromDB.getId());
        assertTrue(getCertainPolicy.isPresent());
        assertEquals(policyFromDB, getCertainPolicy.get());
    }

    @Test
    public void testGetByIdMissing() throws Exception {
        Assert.assertFalse(settingStore.getSettingPolicy(dbConfig.getDslContext(), 10).isPresent());
    }

    @Test
    public void testGetByNameMissing() throws Exception {
        Assert.assertEquals(Collections.emptySet(), new HashSet<>(
                settingStore.getSettingPolicies(dbConfig.getDslContext(),
                        SettingPolicyFilter.newBuilder().withName(info.getName()).build())));
    }

    @Test
    public void testCreateThenGetByName() throws Exception {
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(userPolicy));

        final Collection<SettingPolicy> gotPolicy = settingStore.getSettingPolicies(
                SettingPolicyFilter.newBuilder().withName(info.getName()).build());
        Assert.assertEquals(Collections.singleton(userPolicy), new HashSet<>(gotPolicy));
    }


    @Test
    public void testUpdateSettingPolicy() throws Exception {
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(userPolicy));

        final SettingPolicy updatedPolicy =
                settingStore.updateSettingPolicy(userPolicy.getId(), updatedInfo).getFirst();
        assertEquals(updatedInfo, updatedPolicy.getInfo());
        final Optional<SettingPolicy> gotPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), userPolicy.getId());
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
        final Schedule schedule1 = createSchedule("Test schedule1");
        final Schedule savedSchedule1 = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule1.hasId());

        Schedule schedule2 = schedule1.toBuilder().setDisplayName("Test schedule2").build();
        final Schedule savedSchedule2 = scheduleStore.createSchedule(schedule2);
        assertTrue(savedSchedule2.hasId());

        long scheduleId1 = savedSchedule1.getId();
        SettingPolicyInfo infoWithSchedule = info.toBuilder().setScheduleId(scheduleId1).build();
        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(infoWithSchedule)
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(policy));

        final Optional<SettingPolicy> savedPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), policy.getId());
        assertTrue(savedPolicy.isPresent());
        assertEquals(policy, savedPolicy.get());
        assertTrue(savedPolicy.get().getInfo().hasScheduleId());
        assertEquals(scheduleId1, savedPolicy.get().getInfo().getScheduleId());

        long scheduleId2 = savedSchedule2.getId();
        SettingPolicyInfo updatedInfoWithSchedule = savedPolicy.get().getInfo().toBuilder()
            .setScheduleId(scheduleId2).build();
        SettingPolicy updatedPolicy = policy.toBuilder().setInfo(updatedInfoWithSchedule).build();
        SettingPolicy updatedSavedPolicy = settingStore.updateSettingPolicy(savedPolicy.get().getId(),
            updatedInfoWithSchedule).getFirst();
        assertEquals(updatedPolicy, updatedSavedPolicy);
        assertEquals(scheduleId2, updatedSavedPolicy.getInfo().getScheduleId());
        Optional<SettingPolicy> refetchedUpdatedSavedPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), updatedSavedPolicy.getId());
        assertTrue(refetchedUpdatedSavedPolicy.isPresent());
        assertEquals(scheduleId2, refetchedUpdatedSavedPolicy.get().getInfo().getScheduleId());
    }

    /**
     * Test case when updated policy not found.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testUpdateSettingPolicyNotFound() throws Exception {
        final long policyId = identityProviderSpy.next();
        expectedException.expect(StoreOperationException.class);
        expectedException.expectMessage(
                Matchers.containsString("Setting Policy " + policyId + " not " + "found."));
        settingStore.updateSettingPolicy(policyId, info);
    }

    /**
     * Test case when new setting policy info is invalid.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testUpdateSettingPolicyWithInvalidInfo() throws Exception {
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(userPolicy));

        doThrow(new InvalidItemException("")).when(settingPolicyValidator)
                .validateSettingPolicy(eq(updatedInfo), any());

        expectedException.expect(StoreOperationException.class);
        settingStore.updateSettingPolicy(userPolicy.getId(), updatedInfo);
    }

    /**
     * Test case when updated policy has duplicate name.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testUpdateSettingPolicyToDuplicateName() throws Exception {
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(userPolicy));
        final SettingPolicy duplicatedPolicy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(SettingPolicyInfo.newBuilder().setName(updatedInfo.getName()).build())
                .setSettingPolicyType(Type.USER)
                .build();
        expectedException.expect(StoreOperationException.class);
        expectedException.expectMessage(
                Matchers.containsString("Duplicated policy names found: " + updatedInfo.getName()));

        // Make sure there is another setting policy with the same name as the updated info.
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(duplicatedPolicy));

        settingStore.updateSettingPolicy(userPolicy.getId(), updatedInfo);
    }

    @Test
    public void testResetSettingPolicy() throws Exception {
        final Map<Integer, SettingPolicyInfo> defaultSettingPolicies =
            DefaultSettingPolicyCreator.defaultSettingPoliciesFromSpecs(
                settingSpecStore.getAllSettingSpecs());
        final SettingPolicyInfo vmSettingPolicy = defaultSettingPolicies.get(10);
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(
                SettingPolicy.newBuilder()
                        .setId(identityProviderSpy.next())
                        .setInfo(vmSettingPolicy)
                        .setSettingPolicyType(Type.DEFAULT)
                        .build()));
        final SettingPolicy settingPolicy =
                settingStore.getSettingPolicies(dbConfig.getDslContext(),
                        SettingPolicyFilter.newBuilder().build()).iterator().next();

        final SettingProto.SettingPolicy postResetPolicy =
                settingStore.resetSettingPolicy(settingPolicy.getId()).getFirst();
        Assert.assertEquals(postResetPolicy.getId(), settingPolicy.getId());
        Assert.assertEquals(postResetPolicy.getInfo(), vmSettingPolicy);
    }

    @Test(expected = SettingPolicyNotFoundException.class)
    public void testResetSettingPolicyNotFound() throws Exception {
        settingStore.resetSettingPolicy(7L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResetNonDefaultSettingPolicy() throws Exception {
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(userPolicy));
        settingStore.resetSettingPolicy(userPolicy.getId());
    }

    @Test
    public void testCreateAndGetDefaultSettingPolicy() throws Exception {
        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(7L)
                .setInfo(info)
                .setSettingPolicyType(Type.DEFAULT)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(policy));
        Assert.assertEquals(policy,
                settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder().build())
                        .iterator()
                        .next());
    }

    /**
     * Test case when updating discovery policy.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testUpdateDiscoveredSettingPolicyFail() throws Exception {
        long policyId = identityProviderSpy.next();
        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(policyId)
                .setSettingPolicyType(Type.DISCOVERED)
                .setInfo(info)
                .build();
        expectedException.expect(StoreOperationException.class);
        expectedException.expectMessage(Matchers.containsString(
                "Illegal attempt to modify a discovered setting policy " + policyId));
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy));
        settingStore.updateSettingPolicy(policy.getId(), policy.getInfo());
    }

    /**
     * Tests removal of setting policy specified by the proper type.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDeleteSettingPolicy() throws Exception {
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(userPolicy));
        Assert.assertEquals(Collections.singleton(userPolicy.getId()),
                settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder().build())
                        .stream()
                        .map(SettingPolicy::getId)
                        .collect(Collectors.toSet()));
        settingStore.deleteSettingPolcies(dbConfig.getDslContext(),
                Collections.singleton(userPolicy.getId()), Type.USER);
        Assert.assertEquals(Collections.emptySet(),
                settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder().build())
                        .stream()
                        .map(SettingPolicy::getId)
                        .collect(Collectors.toSet()));
        Assert.assertFalse(
                settingStore.getSettingPolicy(dbConfig.getDslContext(), userPolicy.getId())
                        .isPresent());
    }

    /**
     * Tests removal is requested for the wrong type. Exception is expected.
     * Store data should not change after the failed operation
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDeleteIncorrectType() throws Exception {
        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(info)
                .setSettingPolicyType(Type.DEFAULT)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(policy));
        try {
            settingStore.deleteSettingPolcies(dbConfig.getDslContext(),
                    Collections.singleton(policy.getId()), Type.DISCOVERED);
            Assert.fail("StoreOperationException is expected");
        } catch (StoreOperationException e) {
            Assert.assertEquals(Status.INVALID_ARGUMENT, e.getStatus());
        }
        Assert.assertEquals(Collections.singleton(policy.getId()), new HashSet<>(
                settingStore.getSettingPolicies(dbConfig.getDslContext(),
                        SettingPolicyFilter.newBuilder().build())).stream()
                .map(SettingPolicy::getId)
                .collect(Collectors.toSet()));
    }

    /**
     * Tests removal of non-existing policy. Exception is expected. Store data should not change.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDeleteNonExistingPolicy() throws Exception {
        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setSettingPolicyType(Type.DISCOVERED)
                .setInfo(info)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy));
        try {
            settingStore.deleteSettingPolcies(dbConfig.getDslContext(),
                    Collections.singleton(policy.getId() + 1), Type.DISCOVERED);
            Assert.fail("StoreOperationException is expected");
        } catch (StoreOperationException e) {
            Assert.assertEquals(Status.NOT_FOUND, e.getStatus());
        }
        Assert.assertEquals(Collections.singleton(policy.getId()), new HashSet<>(
                settingStore.getSettingPolicies(dbConfig.getDslContext(),
                        SettingPolicyFilter.newBuilder().build())).stream()
                .map(SettingPolicy::getId)
                .collect(Collectors.toSet()));
    }

    @Test
    public void testGetAllPolicies() throws Exception {
        final SettingPolicy policy2 = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setName("bar")
                        .setDisplayName("some-new-policy")
                        .setScope(Scope.newBuilder())
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnabled(true))
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Arrays.asList(userPolicy, policy2));

        final Collection<SettingPolicy> allPolicies =
                settingStore.getSettingPolicies(SettingPolicyFilter.newBuilder().build());
        Assert.assertEquals(Sets.newHashSet(userPolicy, policy2), new HashSet<>(allPolicies));
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
        assertEquals(retrievedSettingSpecs.size(), 5);
    }


    /**
     * Test get setting policies using schedule.
     *
     * @throws Exception If any exceptions thrown during test execution.
     */
    @Test
    public void testGetSettingPoliciesUsingSchedule() throws Exception {
        final Schedule schedule1 = createSchedule("Test schedule1");
        final Schedule savedSchedule = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule.hasId());

        // No policies using this schedule yet
        Collection<SettingPolicy> settingPolicies =
                settingStore.getSettingPoliciesUsingSchedule(dbConfig.getDslContext(),
                        savedSchedule.getId());
        assertTrue(settingPolicies.isEmpty());

        final SettingPolicy policy1 = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(info)
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(),
                Collections.singleton(policy1));
        // TODO
        assertTrue(policy1.hasId());
        scheduleStore.assignScheduleToSettingPolicy(policy1.getId(), savedSchedule.getId());

        final SettingPolicyInfo info2 = info.toBuilder().setName("test2").build();
        final SettingPolicy policy2 = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(info2)
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy2));

        // TODO
        assertTrue(policy2.hasId());
        assertNotEquals(policy1.getId(), policy2.getId());
        scheduleStore.assignScheduleToSettingPolicy(policy2.getId(), savedSchedule.getId());

        // Now 2 policies should be using this schedule
        settingPolicies = settingStore.getSettingPoliciesUsingSchedule(dbConfig.getDslContext(),
                savedSchedule.getId());
        assertEquals(2, settingPolicies.size());
        settingPolicies.forEach(sPolicy -> assertTrue(sPolicy.getInfo().hasScheduleId()));

        // Add 1 more policy without schedule
        final SettingPolicyInfo info3 = info.toBuilder().setName("test3").build();
        final SettingPolicy policy3 = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(info3)
                .setSettingPolicyType(Type.USER)
                .build();
        // TODO
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy3));
        assertTrue(policy3.hasId());

        final Collection<SettingPolicy> allPolicies = settingStore.getSettingPolicies(dbConfig.getDslContext(),
                SettingPolicyFilter.newBuilder().build());
        assertEquals(3, allPolicies.size());

        // Still 2 policies should be using this schedule
        settingPolicies = settingStore.getSettingPoliciesUsingSchedule(dbConfig.getDslContext(),
                savedSchedule.getId());
        assertEquals(2, settingPolicies.size());
        settingPolicies.forEach(sPolicy -> assertTrue(sPolicy.getInfo().hasScheduleId()));
    }

    private static final String NAME = "foo";
    private static final String WRONG_NAME = "bar";
    private static final String DEFAULT_VALUE = "aaa";
    private static final String STR_VALUE = "bbb";

    /**
     * Test global settings handling.
     *
     * @throws Exception If any exceptions thrown during test execution.
     */
    @Test
    public void testGlobalSettings() throws Exception {
        settingStore.insertGlobalSettings(Lists.newArrayList(settingWithStringValue(NAME, DEFAULT_VALUE)));
        assertEquals(1, settingStore.getAllGlobalSettings().size());

        // Verify that update works
        settingStore.updateGlobalSetting(settingWithStringValue(NAME, STR_VALUE));
        assertEquals(1, settingStore.getAllGlobalSettings().size());
        String value = settingStore.getGlobalSetting(NAME)
                .map(Setting::getStringSettingValue)
                .map(SettingProto.StringSettingValue::getValue)
                .get();
        assertEquals(STR_VALUE, value);

        // Update of a missing name should not insert
        settingStore.updateGlobalSetting(settingWithStringValue(WRONG_NAME, STR_VALUE));
        assertFalse(settingStore.getGlobalSetting(WRONG_NAME).isPresent());

        // Reset the global setting that exists
        settingStore.resetGlobalSetting(Collections.singletonList(NAME));
        value = settingStore.getGlobalSetting(NAME)
            .map(Setting::getStringSettingValue)
            .map(SettingProto.StringSettingValue::getValue)
            .get();
        assertEquals(DEFAULT_VALUE, value);
    }

    /**
     * Test {@link SettingStore#resetGlobalSetting(Collection)}.
     *
     * @throws Exception If any exceptions thrown during test execution.
     */
    @Test(expected = SettingNotFoundException.class)
    public void testGlobalSettingNotFound() throws Exception {
        settingStore.resetGlobalSetting(Collections.singletonList(WRONG_NAME));
    }

    /**
     * Tests creating a setting policy holding a sorted set of OIDs as a value.
     *
     * @throws Exception on exceptions faced
     */
    @Test
    public void testOidSetSetting() throws Exception {
        final Setting emptySetting = Setting.newBuilder()
                .setSettingSpecName("some-spec1")
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder())
                .build();
        final Setting singleton = Setting.newBuilder()
                .setSettingSpecName("some-spec2")
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addOids(123L))
                .build();
        final Setting multiple = Setting.newBuilder()
                .setSettingSpecName("some-spec3")
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addOids(123L))
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addOids(234L))
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addOids(345L))
                .build();
        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setSettingPolicyType(Type.USER)
                .setInfo(SettingPolicyInfo.newBuilder(info)
                        .clearSettings()
                        .addSettings(emptySetting)
                        .addSettings(singleton)
                        .addSettings(multiple))
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy));
        final Collection<SettingPolicy> actual =
                settingStore.getSettingPolicies(dbConfig.getDslContext(),
                        SettingPolicyFilter.newBuilder().build());
        Assert.assertEquals(1, actual.size());
        Assert.assertThat(actual.iterator().next(), new SettingPolicyMatcher(policy));
    }

    private static Setting settingWithStringValue(String name, String value) {
        return Setting.newBuilder()
                .setSettingSpecName(name)
                .setStringSettingValue(
                        SettingProto.StringSettingValue.newBuilder()
                                .setValue(value)
                                .build())
                .build();
    }

    /**
     * Matcher to capture setting policy.
     */
    private static class SettingPolicyMatcher extends
            ProtobufMessageMatcher<SettingProto.SettingPolicy> {
        SettingPolicyMatcher(@Nonnull SettingPolicy expected) {
            super(expected, Collections.singleton("info.settings"));
        }
    }

    /**
     * Test the conversion of Setting protobuf object to values to be inserted into database.
     */
    @Test
    public void testSettingProtobufToDbValues() {
        String settingName = "Setting name";
        Setting boolSetting = Setting.newBuilder()
                .setSettingSpecName(settingName)
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true)
                        .build())
                .build();
        verifyProtobufToDbValues(boolSetting);

        Setting numericSetting = Setting.newBuilder()
                .setSettingSpecName(settingName)
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(100)
                        .build())
                .build();
        verifyProtobufToDbValues(numericSetting);

        Setting stringSetting = Setting.newBuilder()
                .setSettingSpecName(settingName)
                .setStringSettingValue(StringSettingValue.newBuilder().setValue("abc")
                        .build())
                .build();
        verifyProtobufToDbValues(stringSetting);

        Setting enumSetting = Setting.newBuilder()
                .setSettingSpecName(settingName)
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("enum")
                        .build())
                .build();
        verifyProtobufToDbValues(enumSetting);

        List<Long> oidList = Arrays.asList(1L, 2L, 3L, 4L);
        Setting oidListSetting = Setting.newBuilder()
                .setSettingSpecName(settingName)
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addAllOids(oidList)
                        .build())
                .build();
        verifyProtobufToDbValues(oidListSetting);
    }

    private void verifyProtobufToDbValues(Setting setting) {
        SettingAdapter adapter = new SettingAdapter(setting);
        assertEquals(setting.getValueCase(), adapter.getSettingType());
        assertEquals(setting.getSettingSpecName(), adapter.getSettingName());
        switch (setting.getValueCase()) {
            case BOOLEAN_SETTING_VALUE:
                assertEquals(Boolean.toString(setting.getBooleanSettingValue().getValue()),
                        adapter.getValue());
                break;
            case NUMERIC_SETTING_VALUE:
                assertEquals(Float.toString(setting.getNumericSettingValue().getValue()),
                        adapter.getValue());
                break;
            case STRING_SETTING_VALUE:
                assertEquals(setting.getStringSettingValue().getValue(),
                        adapter.getValue());
                break;
            case ENUM_SETTING_VALUE:
                assertEquals(setting.getEnumSettingValue().getValue(),
                        adapter.getValue());
                break;
            case SORTED_SET_OF_OID_SETTING_VALUE:
                assertEquals(setting.getSortedSetOfOidSettingValue().getOidsList(),
                        adapter.getOidList());
                break;
            default:
                fail("unsupported setting value case: " + setting.getValueCase().name());
        }
    }

    /**
     * Test conversion of setting values in database to Setting protobuf object.
     */
    @Test
    public void testDbValueToSettingProtobuf() {
        String settingName = "Setting name";
        List<Long> oidList = new ArrayList<>();
        verifyDbToSettingProtobuf(settingName, ValueCase.BOOLEAN_SETTING_VALUE, "true", oidList);
        verifyDbToSettingProtobuf(settingName, ValueCase.NUMERIC_SETTING_VALUE, "100", oidList);
        verifyDbToSettingProtobuf(settingName, ValueCase.STRING_SETTING_VALUE, "abc", oidList);
        verifyDbToSettingProtobuf(settingName, ValueCase.ENUM_SETTING_VALUE, "enum", oidList);
        oidList.add(1L);
        oidList.add(2L);
        oidList.add(3L);
        verifyDbToSettingProtobuf(settingName, ValueCase.SORTED_SET_OF_OID_SETTING_VALUE, "-", oidList);
    }

    private void verifyDbToSettingProtobuf(String settingName, ValueCase settingType,
                                           String value, List<Long> oidListValue) {
        SettingAdapter settingAdapter = new SettingAdapter(settingName, settingType, value, oidListValue);
        Setting setting = settingAdapter.getSetting();
        assertEquals(settingName, setting.getSettingSpecName());
        assertEquals(settingType, setting.getValueCase());
        switch (setting.getValueCase()) {
            case BOOLEAN_SETTING_VALUE:
                assertEquals(Boolean.valueOf(value),
                        setting.getBooleanSettingValue().getValue());
                break;
            case NUMERIC_SETTING_VALUE:
                assertEquals(Float.parseFloat(value),
                        setting.getNumericSettingValue().getValue(), 0);
                break;
            case STRING_SETTING_VALUE:
                assertEquals(value,
                        setting.getStringSettingValue().getValue());
                break;
            case ENUM_SETTING_VALUE:
                assertEquals(value,
                        setting.getEnumSettingValue().getValue());
                break;
            case SORTED_SET_OF_OID_SETTING_VALUE:
                assertEquals(oidListValue,
                        setting.getSortedSetOfOidSettingValue().getOidsList());
                break;
            default:
                fail("unsupported setting value case: " + setting.getValueCase().name());
        }
    }

    /**
     * Test setting store methods that save a setting for a VM of a plan, get the setting back
     * by calling getPlanEntitySettings by providing plan ID and the VM ID, and delete the plan
     * and verify the settings data for the plan is removed from database.
     *
     * @throws Exception
     */
    @Test
    public void testSaveGetAndDeletePlanSettings() throws Exception {
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName("maxObservationPeriodVirtualMachine")
                .setNumericSettingValue(NumericSettingValue.getDefaultInstance())
                .build();
        final long planId = 1L;
        final long vmId = 100L;
        final Multimap<Long, Setting> entityToSettingMap = HashMultimap.create();
        final Multimap<Setting, Long> settingToEntityMap = HashMultimap.create();
        entityToSettingMap.put(vmId, setting);
        settingToEntityMap.put(setting, vmId);
        settingStore.savePlanEntitySettings(planId, entityToSettingMap, settingToEntityMap);

        Map<Long, Collection<SettingToPolicyId>> settingsMap =
                settingStore.getPlanEntitySettings(planId, Collections.singletonList(vmId));
        Collection<SettingToPolicyId> settingToPolicyIds = settingsMap.get(vmId);
        assertNotNull(settingToPolicyIds);
        assertEquals(1, settingsMap.keySet().size());
        assertEquals(vmId, settingsMap.keySet().stream().findFirst().get().longValue());
        assertThat(settingStore.getContextsWithSettings(), containsInAnyOrder(planId));

        settingStore.deletePlanSettings(planId);

        settingsMap =
                settingStore.getPlanEntitySettings(planId, Collections.singletonList(vmId));
        assertEquals(0, settingsMap.keySet().size());
    }

    /**
     * Tests that acceptances for actions associated with policy weren't removed if existed
     * execution schedule settings with associated action mode settings will not be changed.
     * In this test case we update policy display name and add new pair of execution
     * schedule and corresponding action mode settings.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testUpdatePolicyWithoutRemovingAcceptancesForActions() throws Exception {
        final long groupId = 23L;
        createGroup(groupId);

        final Schedule schedule1 = createSchedule("Schedule1");
        final Schedule savedSchedule1 = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule1.hasId());

        final Schedule schedule2 = createSchedule("Schedule2");
        final Schedule savedSchedule2 = scheduleStore.createSchedule(schedule2);
        assertTrue(savedSchedule2.hasId());

        final Setting actionModeSetting =
                createActionModeSetting(EntitySettingSpecs.ResizeVmemUpInBetweenThresholds.name(),
                        ActionMode.MANUAL);

        final Setting executionScheduleSetting = createExecutionScheduleSetting(
                EntitySettingSpecs.ResizeVmemUpInBetweenThresholdsExecutionSchedule.name(),
                Arrays.asList(savedSchedule1.getId(), savedSchedule2.getId()));

        final SettingPolicyInfo settingPolicyInfo =
                createSettingPolicyInfo(Arrays.asList(actionModeSetting, executionScheduleSetting),
                        groupId);

        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(settingPolicyInfo)
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy));

        final Optional<SettingPolicy> savedPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), policy.getId());
        assertTrue(savedPolicy.isPresent());

        final Setting newActionModeSetting =
                createActionModeSetting(EntitySettingSpecs.Move.name(), ActionMode.RECOMMEND);
        final Setting newExecutionScheduleSetting =
                createExecutionScheduleSetting(EntitySettingSpecs.MoveExecutionSchedule.name(),
                        Collections.singletonList(savedSchedule1.getId()));

        final SettingPolicyInfo updatedSettingPolicyInfo = savedPolicy.get()
                .getInfo()
                .toBuilder()
                .setDisplayName("Updated display name")
                .addAllSettings(Arrays.asList(newActionModeSetting, newExecutionScheduleSetting))
                .build();
        final Pair<SettingPolicy, Boolean> updateSettingPolicyResults =
                settingStore.updateSettingPolicy(savedPolicy.get().getId(),
                        updatedSettingPolicyInfo);
        final SettingPolicy updatedSavedPolicy = updateSettingPolicyResults.getFirst();
        assertEquals("Updated display name", updatedSavedPolicy.getInfo().getDisplayName());
        // don't remove acceptances if there is no changes related to execution schedule settings
        // with associated action mode settings
        assertFalse(updateSettingPolicyResults.getSecond());
    }

    /**
     * Tests removing acceptances for actions associated with policy if removed execution
     * schedule setting from policy.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testUpdatePolicyWithRemovingAcceptancesWhenExecutionScheduleSettingDeleted()
            throws Exception {
        final long groupId = 23L;
        createGroup(groupId);

        final Schedule schedule1 = createSchedule("Schedule1");
        final Schedule savedSchedule1 = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule1.hasId());

        final Schedule schedule2 = createSchedule("Schedule2");
        final Schedule savedSchedule2 = scheduleStore.createSchedule(schedule2);
        assertTrue(savedSchedule2.hasId());

        final Setting actionModeSetting = createActionModeSetting(
                EntitySettingSpecs.ResizeVmemUpInBetweenThresholds.getSettingName(),
                ActionMode.MANUAL);

        final Setting executionScheduleSetting = createExecutionScheduleSetting(
                EntitySettingSpecs.ResizeVmemUpInBetweenThresholdsExecutionSchedule.getSettingName(),
                Arrays.asList(savedSchedule1.getId(), savedSchedule2.getId()));

        final SettingPolicyInfo settingPolicyInfo =
                createSettingPolicyInfo(Arrays.asList(actionModeSetting, executionScheduleSetting),
                        groupId);

        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(settingPolicyInfo)
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy));

        final Optional<SettingPolicy> savedPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), policy.getId());
        assertTrue(savedPolicy.isPresent());

        final SettingPolicyInfo updatedSettingPolicyInfo = savedPolicy.get()
                .getInfo()
                .toBuilder()
                .clearSettings()
                .addSettings(actionModeSetting)
                .build();
        final Pair<SettingPolicy, Boolean> updateSettingPolicyResults =
                settingStore.updateSettingPolicy(savedPolicy.get().getId(),
                        updatedSettingPolicyInfo);
        final SettingPolicy updatedSavedPolicy = updateSettingPolicyResults.getFirst();
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(updatedSettingPolicyInfo.getSettingsList(),
                        updatedSavedPolicy.getInfo().getSettingsList()));
        // remove acceptance if removed execution schedule setting
        Assert.assertTrue(updateSettingPolicyResults.getSecond());
    }

    /**
     * Tests that acceptances for actions associated with policy weren't removed if execution
     * schedule setting value was modified.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testUpdatePolicyWithoutRemovingAcceptancesWhenExecutionScheduleSettingModified()
            throws Exception {
        final long groupId = 23L;
        createGroup(groupId);

        final Schedule schedule1 = createSchedule("Schedule1");
        final Schedule savedSchedule1 = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule1.hasId());

        final Schedule schedule2 = createSchedule("Schedule2");
        final Schedule savedSchedule2 = scheduleStore.createSchedule(schedule2);
        assertTrue(savedSchedule2.hasId());

        final Setting actionModeSetting = createActionModeSetting(
                EntitySettingSpecs.ResizeVmemUpInBetweenThresholds.getSettingName(),
                ActionMode.MANUAL);

        final Setting executionScheduleSetting = createExecutionScheduleSetting(
                EntitySettingSpecs.ResizeVmemUpInBetweenThresholdsExecutionSchedule.getSettingName(),
                Collections.singletonList(savedSchedule1.getId()));

        final SettingPolicyInfo settingPolicyInfo =
                createSettingPolicyInfo(Arrays.asList(actionModeSetting, executionScheduleSetting),
                        groupId);

        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(settingPolicyInfo)
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy));

        final Optional<SettingPolicy> savedPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), policy.getId());
        assertTrue(savedPolicy.isPresent());

        final Setting updatedExecutionScheduleSetting = createExecutionScheduleSetting(
                EntitySettingSpecs.ResizeVmemUpInBetweenThresholdsExecutionSchedule.getSettingName(),
                Collections.singletonList(savedSchedule2.getId()));

        final SettingPolicyInfo updatedSettingPolicyInfo = savedPolicy.get()
                .getInfo()
                .toBuilder()
                .clearSettings()
                .addAllSettings(Arrays.asList(actionModeSetting, updatedExecutionScheduleSetting))
                .build();
        final Pair<SettingPolicy, Boolean> updateSettingPolicyResults =
                settingStore.updateSettingPolicy(savedPolicy.get().getId(),
                        updatedSettingPolicyInfo);
        final SettingPolicy updatedSavedPolicy = updateSettingPolicyResults.getFirst();
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(updatedSettingPolicyInfo.getSettingsList(),
                        updatedSavedPolicy.getInfo().getSettingsList()));
        // remove acceptance if execution schedule setting value was changed
        Assert.assertFalse(updateSettingPolicyResults.getSecond());
    }

    /**
     * Tests removing acceptances for actions associated with policy when action mode associated
     * with execution schedule was changed from MANUAL to other one.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testUpdatePolicyWithRemovingAcceptancesWhenActionModeWasChanged() throws Exception {
        final long groupId = 23L;
        createGroup(groupId);

        final Schedule schedule1 = createSchedule("Schedule1");
        final Schedule savedSchedule1 = scheduleStore.createSchedule(schedule1);
        assertTrue(savedSchedule1.hasId());

        final Schedule schedule2 = createSchedule("Schedule2");
        final Schedule savedSchedule2 = scheduleStore.createSchedule(schedule2);
        assertTrue(savedSchedule2.hasId());

        final Setting actionModeSetting = createActionModeSetting(
                EntitySettingSpecs.ResizeVmemUpInBetweenThresholds.getSettingName(),
                ActionMode.MANUAL);

        final Setting executionScheduleSetting = createExecutionScheduleSetting(
                EntitySettingSpecs.ResizeVmemUpInBetweenThresholdsExecutionSchedule.getSettingName(),
                Arrays.asList(savedSchedule1.getId(), savedSchedule2.getId()));

        final SettingPolicyInfo settingPolicyInfo =
                createSettingPolicyInfo(Arrays.asList(actionModeSetting, executionScheduleSetting),
                        groupId);

        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setId(identityProviderSpy.next())
                .setInfo(settingPolicyInfo)
                .setSettingPolicyType(Type.USER)
                .build();
        settingStore.createSettingPolicies(dbConfig.getDslContext(), Collections.singleton(policy));

        final Optional<SettingPolicy> savedPolicy =
                settingStore.getSettingPolicy(dbConfig.getDslContext(), policy.getId());
        assertTrue(savedPolicy.isPresent());

        final Setting updatedActionModeSetting = createActionModeSetting(
                EntitySettingSpecs.ResizeVmemUpInBetweenThresholds.getSettingName(),
                ActionMode.AUTOMATIC);

        final SettingPolicyInfo updatedSettingPolicyInfo = savedPolicy.get()
                .getInfo()
                .toBuilder()
                .clearSettings()
                .addAllSettings(Arrays.asList(updatedActionModeSetting, executionScheduleSetting))
                .build();
        final Pair<SettingPolicy, Boolean> updateSettingPolicyResults =
                settingStore.updateSettingPolicy(savedPolicy.get().getId(),
                        updatedSettingPolicyInfo);
        final SettingPolicy updatedSavedPolicy = updateSettingPolicyResults.getFirst();
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(updatedSettingPolicyInfo.getSettingsList(),
                        updatedSavedPolicy.getInfo().getSettingsList()));
        // remove acceptance if action mode changed from MANUAL to AUTOMATIC
        Assert.assertTrue(updateSettingPolicyResults.getSecond());
    }


    @Nonnull
    private Schedule createSchedule(@Nonnull final String scheduleName) {
        return Schedule.newBuilder()
                .setDisplayName(scheduleName)
                .setStartTime(1446760800000L)
                .setEndTime(1446766200000L)
                .setPerpetual(Perpetual.newBuilder().build())
                .setTimezoneId("Test timezone")
                .build();
    }

    private Setting createActionModeSetting(@Nonnull final String settingName,
            @Nonnull final ActionMode actionMode) {
        return Setting.newBuilder()
                .setSettingSpecName(settingName)
                .setEnumSettingValue(
                        EnumSettingValue.newBuilder().setValue(actionMode.name()).build())
                .build();
    }

    private Setting createExecutionScheduleSetting(@Nonnull final String settingName,
            @Nonnull final Collection<Long> executionSchedules) {
        return Setting.newBuilder()
                .setSettingSpecName(settingName)
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                        .addAllOids(executionSchedules)
                        .build())
                .build();
    }

    private SettingPolicyInfo createSettingPolicyInfo(@Nonnull final Collection<Setting> settings,
            long groupId) {
        return SettingPolicyInfo.newBuilder()
                .setName("Test")
                .setDisplayName("Test Policy")
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnabled(true)
                .setScope(Scope.newBuilder().addGroups(groupId).build())
                .addAllSettings(settings)
                .build();
    }

    private void createGroup(final long groupId) throws StoreOperationException {
        final TestGroupGenerator groupGenerator = new TestGroupGenerator();
        final Origin userOrigin = groupGenerator.createUserOrigin();
        final GroupDefinition groupDefinition = groupGenerator.createGroupDefinition();

        // create group - policy scope
        groupStore.createGroup(groupId, userOrigin, groupDefinition,
                Collections.singleton(MemberType.newBuilder().setEntity(1).build()), false);
    }
}
