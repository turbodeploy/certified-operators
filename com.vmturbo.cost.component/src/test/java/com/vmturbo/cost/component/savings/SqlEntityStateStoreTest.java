package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.AdditionalMatchers.leq;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.scheduling.TaskScheduler;

import com.vmturbo.cloud.common.entity.scope.EntityCloudScope;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.entity.scope.SQLCloudScopeStore;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Test operations of SqlEntityStateStore.
 */
public class SqlEntityStateStoreTest {

    /**
     * Entity State Store.
     */
    private EntityStateStore store;

    /**
     * Config providing access to DB. Also ClassRule to init Db and upgrade to latest.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to clean up temp test DB.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Context to execute DB queries and inserts.
     */
    private final DSLContext dsl = dbConfig.getDslContext();

    /**
     * Cloud scope store
     */
    private final SQLCloudScopeStore cloudScopeStore = new SQLCloudScopeStore(
            dsl, mock(TaskScheduler.class), Duration.ZERO, 100, 100);

    /**
     * Initializing store.
     *
     * @throws Exception Throw on DB init errors.
     */
    @Before
    public void setup() throws Exception {
        store = new SqlEntityStateStore(dsl, 2);
    }

    @Test
    public void testStoreOperations() throws Exception {
        // Insert states into store. Start with 10 states.
        Set<EntityState> stateSet = new HashSet<>();
        for (long i = 1; i <= 10; i++) {
            stateSet.add(createState(i));
        }

        // entity_cloud_scope table should be empty at the beginning.
        List<EntityCloudScope> scopeEntries = cloudScopeStore.streamAll().collect(Collectors.toList());
        assertEquals(0, scopeEntries.size());

        TopologyEntityCloudTopology cloudTopology = getCloudTopology(1000L);
        store.updateEntityStates(stateSet.stream().collect(Collectors.toMap(EntityState::getEntityId, Function.identity())),
                cloudTopology);

        // Get states by IDs.
        Set<Long> entityIds = ImmutableSet.of(3L, 4L, 5L, 100L);
        Map<Long, EntityState> stateMap = store.getEntityStates(entityIds);
        assertEquals(3, stateMap.size());

        // Validate records inserted to entity_cloud_scope
        scopeEntries = cloudScopeStore.streamAll().collect(Collectors.toList());
        assertEquals(10, scopeEntries.size());

        // Delete 2 states.
        Set<Long> entitiesToDelete = ImmutableSet.of(1L, 2L);
        stateMap = store.getEntityStates(entitiesToDelete);
        assertEquals(2, stateMap.size());
        store.deleteEntityStates(entitiesToDelete);
        stateMap = store.getEntityStates(entitiesToDelete);
        assertEquals(0, stateMap.size());

        // Verify the corresponding scope records are removed after state is deleted and the cloud
        // scope table cleanup is executed.
        cloudScopeStore.cleanupCloudScopeRecords();
        scopeEntries = cloudScopeStore.streamAll().collect(Collectors.toList());
        assertEquals(8, scopeEntries.size());
        List<Long> idsInScopeTable = scopeEntries.stream().map(EntityCloudScope::entityOid).collect(Collectors.toList());
        idsInScopeTable.retainAll(entitiesToDelete);
        assertEquals(0, idsInScopeTable.size());

        // Update existing state and insert new state.
        // Change state for entity id 3 to have action list = [1,2,3,4,5]
        // Add a new state for entity id 11.
        Set<EntityState> stateSetToUpdate = new HashSet<>();
        stateSetToUpdate.add(createState(11L));
        EntityState entity3 = createState(3L);
        entity3.setActionList(Arrays.asList(1d, 2d, 3d, 4d, 5d));
        stateSetToUpdate.add(entity3);
        store.updateEntityStates(stateSetToUpdate.stream().collect(Collectors.toMap(EntityState::getEntityId, Function.identity())),
                cloudTopology);
        Set<Long> entitiesUpdated = ImmutableSet.of(3L, 11L);
        stateMap = store.getEntityStates(entitiesUpdated);
        assertEquals(2, stateMap.size());
        assertNotNull(stateMap.get(3L));
        assertEquals(5, stateMap.get(3L).getActionList().size());
        assertNotNull(stateMap.get(11L));

        // get all states
        List<EntityState> stateList = new ArrayList<>();
        store.getAllEntityStates().forEach(stateList::add);
        assertEquals(9, stateList.size());

        // Get all states with the updated flag = 1.
        Map<Long, EntityState> updated = store.getForcedUpdateEntityStates(LocalDateTime.MIN);
        assertEquals(0, updated.size());
        EntityState updatedState = createState(12L);
        updatedState.setUpdated(true);
        stateSetToUpdate = new HashSet<>();
        stateSetToUpdate.add(updatedState);
        store.updateEntityStates(stateSetToUpdate.stream().collect(Collectors.toMap(EntityState::getEntityId, Function.identity())),
                cloudTopology);
        updated = store.getForcedUpdateEntityStates(LocalDateTime.MIN);
        assertEquals(1, updated.size());
        // the updated flag should not be serialized with the state object, so state is false
        // when deserializing the state.
        updated.values().forEach(state -> assertFalse(state.isUpdated()));

        // Clear the updated flags
        store.clearUpdatedFlags();
        updated = store.getForcedUpdateEntityStates(LocalDateTime.MIN);
        assertEquals(0, updated.size());
    }

    /**
     * Test the scenario where the entity is not in the topology (e.g. deleted) but it has a record
     * in the scope table. In this case, verify the state of the deleted entity is still updated.
     *
     * @throws Exception any exception
     */
    @Test
    public void testGetScopeRecordsForDeletedEntities() throws Exception {
        // Insert states into store. Start with 11 states - 1 to 10 and 5000.
        Set<EntityState> stateSet = new HashSet<>();
        for (long i = 1; i <= 10; i++) {
            stateSet.add(createState(i));
        }
        stateSet.add(createState(5000L));

        // The topology only contains entities with OID less than 1000.
        TopologyEntityCloudTopology cloudTopology = getCloudTopology(1000L);

        long entityOidInScopeTableButNotInTopology = 5000L;
        EntityCloudScopeRecord r1 = createEntityCloudScopeRecord(entityOidInScopeTableButNotInTopology,
                12345L, 12345L, 12345L, 12345L, 12345L);
        dsl.insertInto(Tables.ENTITY_CLOUD_SCOPE).set(r1).execute();
        store.updateEntityStates(stateSet.stream().collect(Collectors.toMap(EntityState::getEntityId, Function.identity())),
                cloudTopology);

        List<EntityState> states = store.getAllEntityStates().collect(Collectors.toList());
        assertEquals(11, states.size());
    }

    /**
     * Create an entity state that has missed savings having the same value as entity ID.
     *
     * @param entityId entity OID
     * @return entity state
     */
    private EntityState createState(Long entityId) {
        EntityState state = new EntityState(entityId);
        state.setActionList(Arrays.asList(1d, 2d, 3d));
        return state;
    }

    /**
     * Create a mock of a cloud topology. This mock will return the entity and related entities
     * given an entity OID. If the entity OID is above the maxOidNumber specified in the parameter,
     * it will return Optional.empty() to simulate the fact that the entity requested is not in the
     * topology.
     *
     * @param maxOidNumber maximum entity OID in topology
     * @return a mock object for the cloud topology
     */
    public static TopologyEntityCloudTopology getCloudTopology(long maxOidNumber) {
        final OS osInfo = OS.newBuilder().setGuestOsName("Linux").setGuestOsType(OSType.LINUX).build();
        final VirtualMachineInfo info = VirtualMachineInfo.newBuilder().setGuestOsInfo(osInfo)
                .setTenancy(Tenancy.DEFAULT).setBillingType(VMBillingType.ONDEMAND).build();

        Long vmId = 1234L;
        TopologyEntityDTO vm1 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("Test1")
                .setEntityState(TopologyDTO.EntityState.POWERED_ON)
                .setOid(vmId)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualMachine(info)
                        .build())
                .build();

        // The same VM TopologyEntityDTO is returned for all VM OIDs because this call is to
        // get the entity type only.
        TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(cloudTopology.getEntity(leq(maxOidNumber))).thenReturn(Optional.of(vm1));
        when(cloudTopology.getEntity(gt(maxOidNumber))).thenReturn(Optional.empty());

        final TopologyEntityDTO serviceProvider = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.SERVICE_PROVIDER_VALUE).setOid(9999L).build();
        when(cloudTopology.getServiceProvider(anyLong())).thenReturn(Optional.of(serviceProvider));

        final TopologyEntityDTO region1 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE).setDisplayName("us-east-1").setOid(5454L).build();
        when(cloudTopology.getConnectedRegion(anyLong())).thenReturn(Optional.of(region1));

        final TopologyEntityDTO availabilityZone = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE).setDisplayName("us-east-1a").setOid(6666L)
                .build();
        when(cloudTopology.getConnectedAvailabilityZone(anyLong())).thenReturn(Optional.of(availabilityZone));

        final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).setOid(7777L).build();
        when(cloudTopology.getOwner(anyLong())).thenReturn(Optional.of(businessAccount));

        Set<Long> memberEntities = ImmutableSet.of(1L, 2L, 3L);
        final GroupAndMembers resourceGroup = ImmutableGroupAndMembers.builder()
                .group(Grouping.newBuilder()
                        .setId(333L)
                        .setDefinition(GroupDefinition.newBuilder()
                                .setType(GroupType.RESOURCE)
                                .build())
                        .build())
                .members(memberEntities)
                .entities(memberEntities)
                .build();
        when(cloudTopology.getResourceGroup(anyLong())).thenReturn(Optional.of(resourceGroup));

        Set<Long> memberAccounts = ImmutableSet.of(7777L, 7778L);
        final GroupAndMembers billingFamily = ImmutableGroupAndMembers.builder()
                .group(Grouping.newBuilder()
                        .setId(555L)
                        .setDefinition(GroupDefinition.newBuilder()
                                .setType(GroupType.BILLING_FAMILY)
                                .build())
                        .build())
                .members(memberAccounts)
                .entities(memberAccounts)
                .build();
        when(cloudTopology.getBillingFamilyForEntity(anyLong())).thenReturn(Optional.of(billingFamily));

        return cloudTopology;
    }

    private EntityCloudScopeRecord createEntityCloudScopeRecord(Long entityOid, Long accountOid,
            Long regionOid, Long availabilityZoneOid, Long serviceProviderOid, Long resourceGroupOid) {
        EntityCloudScopeRecord record = new EntityCloudScopeRecord();
        record.setAccountOid(accountOid);
        record.setEntityOid(entityOid);
        record.setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        record.setRegionOid(regionOid);
        record.setAvailabilityZoneOid(availabilityZoneOid);
        record.setServiceProviderOid(serviceProviderOid);
        record.setResourceGroupOid(resourceGroupOid);
        record.setCreationTime(LocalDateTime.now());
        return record;
    }
}
