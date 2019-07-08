package com.vmturbo.auth.component.migration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.auth.component.store.AuthProvider;
import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;

/**
 * Test that oid is assigned to existing security groups stored in consul during migration.
 */
public class V_01_00_00__Assign_Oid_To_Security_GroupsTest {

    private static final String groupName1 = "no_oid_group_1";
    private static final String groupName2 = "no_oid_group_2";
    private static final String groupType1 = "DedicatedCustomer";
    private static final String groupType2 = "SharedCustomer";
    private static final String groupRole1 = "administrator";
    private static final String groupRole2 = "observer";
    private static final List<Long> groupScope1 = Lists.newArrayList(123L);
    private static final List<Long> groupScope2 = Lists.newArrayList(111L, 112L);

    private static final Gson GSON = new GsonBuilder().create();

    private KeyValueStore keyValueStore = new MapKeyValueStore();

    private V_01_00_00__Assign_Oid_To_Security_Groups migrationScript =
        new V_01_00_00__Assign_Oid_To_Security_Groups(keyValueStore);

    @Before
    public void init() {
        IdentityGenerator.initPrefix(2);
    }

    @Test
    public void testAssignOidToExistingSecurityGroups() {
        SecurityGroupDTO g1 = new SecurityGroupDTO(groupName1, groupType1, groupRole1, groupScope1,
            null);
        SecurityGroupDTO g2 = new SecurityGroupDTO(groupName2, groupType2, groupRole2, groupScope2,
            null);
        keyValueStore.put(AuthProvider.composeExternalGroupInfoKey(groupName1), GSON.toJson(g1));
        keyValueStore.put(AuthProvider.composeExternalGroupInfoKey(groupName2), GSON.toJson(g2));

        Map<String, SecurityGroupDTO> groupByNameBefore = getSecurityGroupsMap();
        // before migration, group does't have oid
        assertEquals(2, groupByNameBefore.size());
        assertNull(groupByNameBefore.get(groupName1).getOid());
        assertNull(groupByNameBefore.get(groupName2).getOid());

        // perform migration
        final MigrationProgressInfo migrationResult = migrationScript.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        Map<String, SecurityGroupDTO> groupByNameAfter = getSecurityGroupsMap();
        SecurityGroupDTO group1 = groupByNameAfter.get(groupName1);
        SecurityGroupDTO group2 = groupByNameAfter.get(groupName2);
        // after migration, oid should be assigned
        assertEquals(2, groupByNameAfter.size());
        assertNotNull(group1.getOid());
        assertNotNull(group2.getOid());
        // all other fields are not change
        assertEquals(groupType1, group1.getType());
        assertEquals(groupRole1, group1.getRoleName());
        assertThat(group1.getScopeGroups(), containsInAnyOrder(groupScope1.toArray()));
        assertEquals(groupType2, group2.getType());
        assertEquals(groupRole2, group2.getRoleName());
        assertThat(group2.getScopeGroups(), containsInAnyOrder(groupScope2.toArray()));
    }

    /**
     * Get the stored security groups and return in a map from its name to itself.
     */
    private Map<String, SecurityGroupDTO> getSecurityGroupsMap() {
        return keyValueStore.getByPrefix(AuthProvider.PREFIX_GROUP).values().stream()
            .map(jsonData -> GSON.fromJson(jsonData, SecurityGroupDTO.class))
            .collect(Collectors.toMap(SecurityGroupDTO::getDisplayName, Function.identity()));
    }
}
