package com.vmturbo.group.persistent;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.List;
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

import com.google.protobuf.ProtocolStringList;

import com.vmturbo.common.protobuf.setting.SettingProto.DefaultType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.ScopeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.SettingValueTypeCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.SettingStore.SettingPolicyValidator;
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

    private final SettingPolicyInfo info = SettingPolicyInfo.newBuilder()
            .setName("test")
            .setDefault(DefaultType.getDefaultInstance())
            .build();

    private String settingSpecJsonFile = "setting-test-json/setting-spec.json";

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private SettingPolicyValidator settingPolicyValidator = mock(SettingPolicyValidator.class);

    @Before
    public void setUp() throws Exception {
        final DSLContext dsl = prepareDatabase();
        settingStore = new SettingStore(settingSpecJsonFile, dsl,
                new IdentityProvider(0), settingPolicyValidator);
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
        settingStore.createSettingPolicy(info);
        settingStore.createSettingPolicy(info);
    }

    @Test(expected = InvalidSettingPolicyException.class)
    public void testCreateInvalidSetting() throws Exception {
        doThrow(new InvalidSettingPolicyException("ERROR"))
            .when(settingPolicyValidator).validateSettingPolicy(any());
        settingStore.createSettingPolicy(info);
    }

    @Test
    public void testCreateThenGetById() throws Exception {
        SettingPolicy policy = settingStore.createSettingPolicy(info);

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
        SettingPolicy policy = settingStore.createSettingPolicy(info);

        assertEquals(info, policy.getInfo());
        assertTrue(policy.hasId());

        Optional<SettingPolicy> gotPolicy = settingStore.getSettingPolicy(info.getName());
        assertTrue(gotPolicy.isPresent());
        assertEquals(policy, gotPolicy.get());
    }

    @Test
    public void testGetAllPolicies() throws Exception {
        final SettingPolicy policy = settingStore.createSettingPolicy(info);
        final SettingPolicy policy2 = settingStore.createSettingPolicy(SettingPolicyInfo.newBuilder()
            .setName("bar")
            .setDefault(DefaultType.getDefaultInstance())
            .build());

        List<SettingPolicy> allPolicies =
                settingStore.getAllSettingPolicies().collect(Collectors.toList());
        assertThat(allPolicies, containsInAnyOrder(policy, policy2));
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
        Optional<SettingSpec> specOpt = settingStore.getSettingSpec(existingSpecName1);
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
        Optional<SettingSpec> specOpt2 = settingStore.getSettingSpec(existingSpecName2);
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

        Collection<SettingSpec> retrievedSettingSpecs = settingStore.getAllSettingSpec();
        assertEquals(retrievedSettingSpecs.size(), 4);
    }
}