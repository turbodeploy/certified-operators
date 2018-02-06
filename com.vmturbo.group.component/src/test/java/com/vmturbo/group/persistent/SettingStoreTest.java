package com.vmturbo.group.persistent;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ProtocolStringList;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.ScopeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.SettingValueTypeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    loader = AnnotationConfigContextLoader.class,
    classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=group_component"})
public class SettingStoreTest {

    private final Logger logger = LogManager.getLogger();

    private SettingStore settingStore;
    private SettingSpecStore settingSpecStore;

    private final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
            .setName("test")
            .build();

    private static final String SETTING_TEST_JSON_SETTING_SPEC_JSON =
            "setting-test-json/setting-spec.json";

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private SettingPolicyValidator settingPolicyValidator = mock(SettingPolicyValidator.class);

    private IdentityProvider identityProviderSpy = spy(new IdentityProvider(0));

    @Before
    public void setUp() throws Exception {
        final DSLContext dsl = prepareDatabase();
        settingSpecStore = new FileBasedSettingsSpecStore(SETTING_TEST_JSON_SETTING_SPEC_JSON);
        settingStore = new SettingStore(settingSpecStore, dsl, identityProviderSpy,
                settingPolicyValidator);
    }

    private DSLContext prepareDatabase() throws Exception {
        flyway = dbConfig.flyway();
        final DSLContext dsl = dbConfig.dsl();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();

        return dsl;
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test(expected = DuplicateNameException.class)
    public void testCreateDuplicatePolicy() throws Exception {
        settingStore.createUserSettingPolicy(info);
        settingStore.createUserSettingPolicy(info);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testCreateInvalidSetting() throws Exception {
        doThrow(new InvalidSettingPolicyException("ERROR"))
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

    @Test(expected = SettingPolicyNotFoundException.class)
    public void testUpdateSettingPolicyNotFound() throws Exception {
        settingStore.updateSettingPolicy(7, info);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testUpdateSettingPolicyWithInvalidInfo() throws Exception {
        final SettingPolicy policy = settingStore.createUserSettingPolicy(info);

        doThrow(new InvalidSettingPolicyException(""))
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

    @Test(expected = InvalidSettingPolicyException.class)
    public void testUpdateDefaultSettingPolicyChangeEntityTypeFail() throws Exception {
        final SettingPolicy policy =
                settingStore.createDefaultSettingPolicy(info);
        settingStore.updateSettingPolicy(policy.getId(),
                policy.getInfo().toBuilder().setEntityType(9001).build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testUpdateDefaultSettingPolicyChangeNameFail() throws Exception {
        final SettingPolicy policy =
                settingStore.createDefaultSettingPolicy(info);
        settingStore.updateSettingPolicy(policy.getId(),
                policy.getInfo().toBuilder().setName("blah").build());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testUpdateDiscoveredSettingPolicyFail() throws Exception {
        final SettingPolicy policy = settingStore.createDiscoveredSettingPolicy(info);
        settingStore.updateSettingPolicy(policy.getId(), policy.getInfo());
    }

    @Test
    public void testDeleteSettingPolicy() throws Exception {
        final SettingPolicy policy = settingStore.createUserSettingPolicy(info);
        final SettingPolicy deletedPolicy = settingStore.deleteSettingPolicy(policy.getId(), true);
        assertEquals(policy, deletedPolicy);
        assertFalse(settingStore.getSettingPolicy(policy.getId()).isPresent());
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testDeleteDefaultFail() throws Exception {
        final SettingPolicy policy = settingStore.createDefaultSettingPolicy(info);
        settingStore.deleteSettingPolicy(policy.getId(), true);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testDeleteDiscoveredFailWhenByUser() throws Exception {
        final SettingPolicy policy = settingStore.createDiscoveredSettingPolicy(info);
        settingStore.deleteSettingPolicy(policy.getId(), true);
    }

    public void testDeleteDiscoveredSuccessWhenNotByUser() throws Exception {
        final SettingPolicy policy = settingStore.createDiscoveredSettingPolicy(info);
        assertTrue(settingStore.getSettingPolicy(policy.getId()).isPresent());

        settingStore.deleteSettingPolicy(policy.getId(), false);
        assertFalse(settingStore.getSettingPolicy(policy.getId()).isPresent());
    }

    @Test(expected = SettingPolicyNotFoundException.class)
    public void testDeleteNotFound() throws Exception {
        settingStore.deleteSettingPolicy(77, true);
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
    public void testUpdateTargetSettingPolicies() throws DatabaseException {
        final long targetId = 12345L;
        final Setting setting = Setting.newBuilder()
            .setSettingSpecName("some-spec")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();

        final DiscoveredSettingPolicyInfo settingPolicyA = DiscoveredSettingPolicyInfo.newBuilder()
            .setEntityType(1)
            .setName("a")
            .addSettings(setting)
            .addDiscoveredGroupNames("group-a")
            .build();

        final DiscoveredSettingPolicyInfo settingPolicyB = DiscoveredSettingPolicyInfo.newBuilder()
            .setEntityType(2)
            .setName("b")
            .addSettings(setting)
            .addDiscoveredGroupNames("group-b")
            .build();

        final DiscoveredSettingPolicyInfo settingPolicyC = DiscoveredSettingPolicyInfo.newBuilder()
            .setEntityType(1)
            .setName("c")
            .addSettings(setting)
            .addDiscoveredGroupNames("group-c")
            .build();

        final Map<String, Long> groupOids = ImmutableMap.of("group-a", 1L,
            "group-b", 2L,
            "group-c", 3L);

        // Create 2 discovered setting policies, a and b.
        settingStore.updateTargetSettingPolicies(targetId,
            Arrays.asList(settingPolicyA, settingPolicyB),
            groupOids);

        // Make sure they are created.
        final Collection<SettingPolicy> firstCreated = settingStore.getSettingPoliciesDiscoveredByTarget(targetId);
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
        settingStore.updateTargetSettingPolicies(targetId,
            Arrays.asList(settingPolicyC, settingPolicyB),
            groupOids);

        final Collection<SettingPolicy> secondCreated = settingStore.getSettingPoliciesDiscoveredByTarget(targetId);
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
}
