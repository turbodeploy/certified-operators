package com.vmturbo.group.persistent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.protobuf.ProtocolStringList;

import com.vmturbo.common.protobuf.setting.SettingOuterClass.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.EntitySettingScope.ScopeCase;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.SettingSpec.SettingValueTypeCase;
import com.vmturbo.common.protobuf.setting.SettingOuterClass.SettingTiebreaker;


@RunWith(MockitoJUnitRunner.class)
public class SettingStoreTest {

    private final Logger logger = LogManager.getLogger();

    private SettingStore settingStore;

    private String settingSpecJsonFile = "setting-test-json/setting-spec.json";


    @Before
    public void setUp() throws Exception {
        settingStore = new SettingStore(settingSpecJsonFile);
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
        SettingSpec retrievedSettingSpec = settingStore.getSettingSpec(existingSpecName1);

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
        SettingSpec retrievedSettingSpec2 = settingStore.getSettingSpec(existingSpecName2);

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