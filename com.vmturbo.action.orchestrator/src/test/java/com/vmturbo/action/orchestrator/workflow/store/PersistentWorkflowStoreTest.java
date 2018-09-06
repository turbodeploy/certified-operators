package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.db.tables.Workflow.WORKFLOW;
import static com.vmturbo.action.orchestrator.db.tables.WorkflowOid.WORKFLOW_OID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.TARGET_ID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_1_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_1_OID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_2_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_2_OID;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.action.orchestrator.db.tables.records.WorkflowRecord;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStoreUpdate;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Test the class {@link PersistentWorkflowStore} persisting a list of
 * WorkflowInfo items. The WorkflowIdentityStore is mocked.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=action"})
public class PersistentWorkflowStoreTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    /**
     * the class to run the DB migration
     */
    private Flyway flyway;

    /**
     * the jooq context for running DB operations
     */
    private DSLContext dsl;

    @Before
    public void setup() {

        // Clean the database and bring it up to the production configuration before running test
        flyway = dbConfig.flyway();
        flyway.clean();
        flyway.migrate();

        // Grab a handle for JOOQ DB operations
        dsl = dbConfig.dsl();

    }

    @After
    public void teardown() {
        // at the end, remove the test DB
        flyway.clean();
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
     * @throws PersistentWorkflowStoreTest should not happen
     */
    @Test
    public void testPersistWorkflowOids() throws IdentityStoreException, WorkflowStoreException {
        // arrange
        IdentityStore mockIdentityStore = Mockito.mock(IdentityStore.class);
        WorkflowInfo workflow1 = WorkflowInfo.newBuilder()
                .setTargetId(TARGET_ID)
                .setName(WORKFLOW_1_NAME)
                .build();
        WorkflowInfo workflow2 = WorkflowInfo.newBuilder()
                .setTargetId(TARGET_ID)
                .setName(WORKFLOW_2_NAME)
                .build();
        // prepare the OID table rows for these two entries, since the OID table holds the primary keys
        dsl.insertInto(WORKFLOW_OID)
                .columns(WORKFLOW_OID.ID, WORKFLOW_OID.TARGET_ID, WORKFLOW_OID.EXTERNAL_NAME)
                .values(WORKFLOW_1_OID, TARGET_ID, WORKFLOW_1_NAME)
                .values(WORKFLOW_2_OID, TARGET_ID, WORKFLOW_2_NAME)
                .execute();

        // map to return from the mock identity store
        Map<WorkflowDTO.WorkflowInfo, Long> itemOidsMap = ImmutableMap.<WorkflowInfo, Long>builder()
                .put(workflow1, WORKFLOW_1_OID)
                .put(workflow2, WORKFLOW_2_OID)
                .build();
        when(mockIdentityStore.fetchOrAssignItemOids(anyList()))
                .thenReturn(new IdentityStoreUpdate(itemOidsMap, Collections.emptyMap()));
        Clock clock = Clock.systemUTC();
        List<WorkflowInfo> workflowInfos = Lists.newArrayList(workflow1, workflow2);
        // workflowStore under test
        PersistentWorkflowStore workflowStoreToTest = new PersistentWorkflowStore(dsl,
                mockIdentityStore, clock);

        // act
        workflowStoreToTest.persistWorkflows(TARGET_ID, workflowInfos);

        // assert that the WORKFLOW table has the two rows
        List<WorkflowRecord> workflowsFromDB = dsl.selectFrom(WORKFLOW)
                .orderBy(WORKFLOW.ID.asc())
                .fetchInto(WorkflowRecord.class);
        // fetch the two rows from the DB and verify the fields
        assertThat(workflowsFromDB.size(), equalTo(2));
        final WorkflowRecord workflowRecord1 = workflowsFromDB.get(0);
        checkWorkflowRecord(workflowRecord1, WORKFLOW_1_OID, workflow1);
        final WorkflowRecord workflowRecord2 = workflowsFromDB.get(1);
        checkWorkflowRecord(workflowRecord2, WORKFLOW_2_OID, workflow2);
        assertThat(workflowRecord2.get(WORKFLOW.ID), equalTo(WORKFLOW_2_OID));
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
                equalTo(expectedWorkflowInfo.toByteArray()));
    }
}