package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.db.tables.Workflow.WORKFLOW;
import static com.vmturbo.action.orchestrator.db.tables.WorkflowOid.WORKFLOW_OID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.TARGET_ID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_1_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_1_OID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_2_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_2_OID;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;

import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.db.tables.records.WorkflowRecord;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.CachingIdentityStore;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreUpdate;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Test the class {@link PersistentWorkflowStore} persisting a list of
 * WorkflowInfo items. The WorkflowIdentityStore is mocked.
 */
public class PersistentWorkflowStoreTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Action.ACTION);

    private static final String OLD_DESCRIPTION = "This is an old description";
    private static final String NEW_DESCRIPTION = "This is a new description";

    private static final WorkflowInfo WORKFLOW_1 = WorkflowInfo.newBuilder()
        .setTargetId(TARGET_ID)
        .setName(WORKFLOW_1_NAME)
        .setDescription(OLD_DESCRIPTION)
        .build();
    private static final WorkflowInfo WORKFLOW_1_NEW = WorkflowInfo.newBuilder()
        .setTargetId(TARGET_ID)
        .setName(WORKFLOW_1_NAME)
        .setDescription(NEW_DESCRIPTION)
        .build();
    private static final WorkflowInfo WORKFLOW_2 = WorkflowInfo.newBuilder()
        .setTargetId(TARGET_ID)
        .setName(WORKFLOW_2_NAME)
        .build();

    private static final Clock CLOCK = Clock.systemUTC();

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * the jooq context for running DB operations
     */
    private DSLContext dsl = dbConfig.getDslContext();

    private IdentityStore mockIdentityStore;

    /**
     * When testing using the in memory database, you must also setup up a REAL identity store,
     * otherwise the foreign key constraint between the WORKFLOW_OID table and WORKFLOW will
     * fail.
     */
    private IdentityStore cachingIdentityStore;
    private PersistentWorkflowIdentityStore persistentWorkflowIdentityStore;

    /**
     * The object being tested.
     */
    private PersistentWorkflowStore persistentWorkflowStore;

    /**
     * Sets up the persistentWorkflowStore with mocks.
     *
     * @throws IdentityStoreException should not be thrown.
     */
    @Before
    public void setup() throws IdentityStoreException {
        // Set up a mock for the IdentityStore
        mockIdentityStore = mock(IdentityStore.class);

        persistentWorkflowIdentityStore = new PersistentWorkflowIdentityStore(dsl);
        cachingIdentityStore = new CachingIdentityStore(new WorkflowAttributeExtractor(),
            persistentWorkflowIdentityStore, new IdentityInitializer(1L));
        persistentWorkflowStore = new PersistentWorkflowStore(dsl,
            cachingIdentityStore, CLOCK);

    }

    /**
     * Test that we write the WorkflowInfo OIDs to the DB.
     * <ul>
     * <li>We prepare the IdentityStore to return the WorkflowInfo -> OID map.
     * <li>and call the WorkflowStore under test
     * <li>then we read back from the DB and verify
     * </ul>
     *
     * @throws IdentityStoreException should not happen
     * @throws WorkflowStoreException should not happen
     */
    @Test
    public void testPersistWorkflowOids() throws IdentityStoreException, WorkflowStoreException {
        // arrange
        // prepare the OID table rows for these two entries, since the OID table holds the primary keys
        dsl.insertInto(WORKFLOW_OID)
                .columns(WORKFLOW_OID.ID, WORKFLOW_OID.TARGET_ID, WORKFLOW_OID.EXTERNAL_NAME)
                .values(WORKFLOW_1_OID, TARGET_ID, WORKFLOW_1_NAME)
                .values(WORKFLOW_2_OID, TARGET_ID, WORKFLOW_2_NAME)
                .execute();

        // map to return from the mock identity store
        Map<WorkflowDTO.WorkflowInfo, Long> itemOidsMap = ImmutableMap.<WorkflowInfo, Long>builder()
                .put(WORKFLOW_1, WORKFLOW_1_OID)
                .put(WORKFLOW_2, WORKFLOW_2_OID)
                .build();
        when(mockIdentityStore.fetchOrAssignItemOids(anyList()))
                .thenReturn(new IdentityStoreUpdate(itemOidsMap, Collections.emptyMap()));
        List<WorkflowInfo> workflowInfos = Lists.newArrayList(WORKFLOW_1, WORKFLOW_2);

        // act
        persistentWorkflowStore.persistWorkflows(TARGET_ID, workflowInfos);

        // assert that the WORKFLOW table has the two rows
        List<WorkflowRecord> workflowsFromDB = dsl.selectFrom(WORKFLOW)
                .orderBy(WORKFLOW.ID.asc())
                .fetchInto(WorkflowRecord.class);
        // fetch the two rows from the DB and verify the fields
        assertThat(workflowsFromDB.size(), equalTo(2));
        final WorkflowRecord workflowRecord1 = workflowsFromDB.get(0);
        checkWorkflowRecord(workflowRecord1, WORKFLOW_1_OID, WORKFLOW_1);
        final WorkflowRecord workflowRecord2 = workflowsFromDB.get(1);
        checkWorkflowRecord(workflowRecord2, WORKFLOW_2_OID, WORKFLOW_2);
        assertThat(workflowRecord2.get(WORKFLOW.ID), equalTo(WORKFLOW_2_OID));
    }

    /**
     * Fetch two workflows from the DB.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testFetchWorkflow() throws Exception {
        // arrange
        // prepare the OID table rows for these two entries, since the OID table holds the primary keys
        dsl.insertInto(WORKFLOW_OID)
                .columns(WORKFLOW_OID.ID, WORKFLOW_OID.TARGET_ID, WORKFLOW_OID.EXTERNAL_NAME)
                .values(WORKFLOW_1_OID, TARGET_ID, WORKFLOW_1_NAME)
                .values(WORKFLOW_2_OID, TARGET_ID, WORKFLOW_2_NAME)
                .execute();
        dsl.insertInto(WORKFLOW)
                .columns(WORKFLOW.ID, WORKFLOW.WORKFLOW_INFO)
                .values(WORKFLOW_1_OID, WORKFLOW_1)
                .values(WORKFLOW_2_OID, WORKFLOW_2)
                .execute();

        // Act
        Optional<WorkflowDTO.Workflow> optResult1 = persistentWorkflowStore.fetchWorkflow(WORKFLOW_1_OID);
        Optional<WorkflowDTO.Workflow> optResult2 = persistentWorkflowStore.fetchWorkflow(WORKFLOW_2_OID);

        // Assert
        assertTrue("WORKFLOW 1 not found", optResult1.isPresent());
        assertTrue("WORKFLOW 2 not found", optResult2.isPresent());
        final WorkflowDTO.Workflow result1 = optResult1.get();
        assertThat(result1.getId(), equalTo(WORKFLOW_1_OID));
        assertThat(result1.getWorkflowInfo(), equalTo(WORKFLOW_1));
        final WorkflowDTO.Workflow result2 = optResult2.get();
        assertThat(result2.getId(), equalTo(WORKFLOW_2_OID));
        assertThat(result2.getWorkflowInfo(), equalTo(WORKFLOW_2));
    }

    /**
     * Test that the Optional<Workflow> is empty if the workflow doesn't exist in the DB.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testFetchWorkflowNotFound() throws Exception {
        // arrange
        // empty database

        // Act
        Optional<WorkflowDTO.Workflow> optResult1 = persistentWorkflowStore.fetchWorkflow(WORKFLOW_1_OID);
        Optional<WorkflowDTO.Workflow> optResult2 = persistentWorkflowStore.fetchWorkflow(WORKFLOW_2_OID);

        // Assert
        assertFalse("WORKFLOW 1 found", optResult1.isPresent());
        assertFalse("WORKFLOW 2  found", optResult2.isPresent());
    }

    /**
     * Verify the WorkflowRecord read back from the database against the expected OID and
     * different WorkflowInfo fields, including the target id, the external name, and the ByteArray
     * for the WorkflowInfo protobuf saved as a blob.
     *
     * @param workflowRecord the DB record read from the 'workflow' table
     * @param expectedWorkflowOid the OID that should have been assigned to this workflow record row
     * @param expectedWorkflowInfo the WorkflowInfo that should have been recorded
     */
    private void checkWorkflowRecord(WorkflowRecord workflowRecord, long expectedWorkflowOid,
                                     WorkflowInfo expectedWorkflowInfo) {
        assertThat(workflowRecord.get(WORKFLOW.ID),
                equalTo(expectedWorkflowOid));
        assertThat(workflowRecord.get(WORKFLOW.WORKFLOW_INFO),
                equalTo(expectedWorkflowInfo));
    }

    @Test
    public void testInsert() throws WorkflowStoreException {
        persistentWorkflowStore.insertWorkflow(WORKFLOW_1);

        Result<Record> record = dsl.select().from(WORKFLOW)
            .fetch();

        WorkflowInfo workflowInfo = WORKFLOW.WORKFLOW_INFO.from(record.get(0)).component1();
        assertEquals(WORKFLOW_1, workflowInfo);
    }

    @Test
    public void testUpdate() throws WorkflowStoreException {
        persistentWorkflowStore.insertWorkflow(WORKFLOW_1);
        Result<Record> afterInsertRecord = dsl.select().from(WORKFLOW)
            .fetch();
        long workflowId = WORKFLOW.ID.from(afterInsertRecord.get(0)).component1();

        persistentWorkflowStore.updateWorkflow(workflowId, WORKFLOW_1_NEW);
        Result<Record> afterUpdateRecord = dsl.select().from(WORKFLOW)
            .fetch();

        WorkflowInfo workflowInfo = WORKFLOW.WORKFLOW_INFO.from(afterUpdateRecord.get(0)).component1();
        assertEquals(WORKFLOW_1_NEW, workflowInfo);
    }

    @Test
    public void testDelete() throws WorkflowStoreException {
        persistentWorkflowStore.insertWorkflow(WORKFLOW_1);
        Result<Record> afterInsertRecord = dsl.select().from(WORKFLOW)
            .fetch();
        long workflowId = WORKFLOW.ID.from(afterInsertRecord.get(0)).component1();

        persistentWorkflowStore.deleteWorkflow(workflowId);

        int numOfRows = dsl.selectCount().from(WORKFLOW).fetch().get(0).component1();
        // there should be no rows in the inmemory database
        assertEquals(0, numOfRows);
    }
}