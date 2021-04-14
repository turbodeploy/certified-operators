package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.cost.component.db.Cost;
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
        store.updateEntityStates(stateSet.stream().collect(Collectors.toMap(EntityState::getEntityId, Function.identity())));

        // Get states by IDs.
        Set<Long> entityIds = ImmutableSet.of(3L, 4L, 5L, 100L);
        Map<Long, EntityState> stateMap = store.getEntityStates(entityIds);
        assertEquals(3, stateMap.size());

        // Delete 2 states.
        Set<Long> entitiesToDelete = ImmutableSet.of(1L, 2L);
        stateMap = store.getEntityStates(entitiesToDelete);
        assertEquals(2, stateMap.size());
        store.deleteEntityStates(entitiesToDelete);
        stateMap = store.getEntityStates(entitiesToDelete);
        assertEquals(0, stateMap.size());

        // Update existing state and insert new state.
        // Change state for entity id 3 to have action list = [1,2,3,4,5]
        // Add a new state for entity id 11.
        Set<EntityState> stateSetToUpdate = new HashSet<>();
        stateSetToUpdate.add(createState(11L));
        EntityState entity3 = createState(3L);
        entity3.setActionList(Arrays.asList(1d, 2d, 3d, 4d, 5d));
        stateSetToUpdate.add(entity3);
        store.updateEntityStates(stateSetToUpdate.stream().collect(Collectors.toMap(EntityState::getEntityId, Function.identity())));
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
        store.updateEntityStates(stateSetToUpdate.stream().collect(Collectors.toMap(EntityState::getEntityId, Function.identity())));
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
}
