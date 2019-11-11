package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.db.tables.Workflow.WORKFLOW;
import static com.vmturbo.action.orchestrator.db.tables.WorkflowOid.WORKFLOW_OID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.TARGET_ID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_1_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_1_OID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_1_TARGET_ID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_2_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_2_OID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_2_TARGET_ID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.attr1;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.attr2;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=action"})
public class PersistentWorkflowIdentityStoreTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;
    private DSLContext dsl;

    @Before
    public void setup() {

        IdentityGenerator.initPrefix(0);

        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    /**
     * Load the workflow DB with two entries, workflow-1 and workflow-2.
     * Fetch all the OID mappings into a map and verify.
     * @throws IdentityStoreException - should not happen
     */
    @Test
    public void testFetchAllOidMappings() throws IdentityStoreException {
        // arrange
        PersistentWorkflowIdentityStore testIdentityStore = new PersistentWorkflowIdentityStore(dsl);
        persistBothWorkflowOids();
        // act
        Map<IdentityMatchingAttributes, Long> attrToOidMap = testIdentityStore.fetchAllOidMappings();
        // assert
        assertThat(attrToOidMap.size(), equalTo(2));
        assertThat(attrToOidMap.keySet(), containsInAnyOrder(attr1, attr2));
        assertThat(attrToOidMap.values(), containsInAnyOrder(WORKFLOW_1_OID, WORKFLOW_2_OID));
    }


    /**
     * Load the workflow DB with one entries, workflow-1.
     * Save a new entry, workflow2
     * Fetch all the OID mappings into a map and verify.
     */
    @Test
    public void testSaveOidMappings() throws Exception {
        // arrange
        PersistentIdentityStore testIdentityStore = new PersistentWorkflowIdentityStore(dsl);
        dsl.insertInto(WORKFLOW_OID)
                .set(WORKFLOW_OID.ID, WORKFLOW_1_OID)
                .set(WORKFLOW_OID.EXTERNAL_NAME, WORKFLOW_1_NAME)
                .set(WORKFLOW_OID.TARGET_ID, WORKFLOW_1_TARGET_ID)
                .execute();
        Map<IdentityMatchingAttributes, Long> attrsToOidMap =
                ImmutableMap.<IdentityMatchingAttributes, Long>builder()
                        .put(attr2, WORKFLOW_2_OID)
                        .build();

        // act
        testIdentityStore.saveOidMappings(attrsToOidMap);
        // assert
        Map<IdentityMatchingAttributes, Long> attrToOidMap = testIdentityStore.fetchAllOidMappings();
        assertThat(attrToOidMap.size(), equalTo(2));
        assertThat(attrToOidMap.keySet(), containsInAnyOrder(attr1, attr2));
        assertThat(attrToOidMap.values(), containsInAnyOrder(WORKFLOW_1_OID, WORKFLOW_2_OID));
    }

    /**
     * Load the workflow DB with one entries, workflow-1.
     * Update the workflow1 to workflow2 with workflow oid 1
     * Fetch all the OID mappings into a map and verify.
     */
    @Test
    public void testUpdateOidMappings() throws Exception {
        // arrange
        PersistentIdentityStore testIdentityStore = new PersistentWorkflowIdentityStore(dsl);
        dsl.insertInto(WORKFLOW_OID)
                .set(WORKFLOW_OID.ID, WORKFLOW_1_OID)
                .set(WORKFLOW_OID.EXTERNAL_NAME, WORKFLOW_1_NAME)
                .set(WORKFLOW_OID.TARGET_ID, WORKFLOW_1_TARGET_ID)
                .execute();
        Map<IdentityMatchingAttributes, Long> updatedMap =
                ImmutableMap.<IdentityMatchingAttributes, Long>builder()
                        .put(attr2, WORKFLOW_1_OID)
                        .build();

        // act
        testIdentityStore.updateOidMappings(updatedMap);
        // assert
        Map<IdentityMatchingAttributes, Long> attrsToOidMap = testIdentityStore.fetchAllOidMappings();
        assertEquals(attrsToOidMap.size(), 1);
        Map.Entry<IdentityMatchingAttributes, Long> entry = attrsToOidMap.entrySet().iterator().next();
        assertEquals(entry.getKey(), attr2);
        assertEquals((long) entry.getValue(), WORKFLOW_1_OID);
    }

    /**
     * Test that deleting an OID deletes the OID row.
     *
     * @throws IdentityStoreException - should never happen
     */
    @Test
    public void testDeleteOidMappings() throws IdentityStoreException {
        PersistentWorkflowIdentityStore testIdentityStore = new PersistentWorkflowIdentityStore(dsl);
        // arrange
        persistBothWorkflowOids();
        Set<Long> oidsToRemove = Sets.newHashSet(WORKFLOW_1_OID);
        // act
        testIdentityStore.removeOidMappings(oidsToRemove);
        // assert
        List<Long> oidsFound = dsl.select()
                .from(WORKFLOW_OID)
                .fetch()
                .getValues(WORKFLOW_OID.ID, Long.class);
        assertFalse("expect OID1 to be deleted", oidsFound.contains(WORKFLOW_1_OID));
        assertTrue("expect OID2 to still remain", oidsFound.contains(WORKFLOW_2_OID));
    }

    /**
     * Test that deleting an OID cascades to delete the workflow_info row as well.
     *
     * @throws IdentityStoreException - should never happen
     */
    @Test
    public void testDeleteCascades() throws IdentityStoreException {
        // arrange
        // populate the OIDs table, creating primary keys
        persistBothWorkflowOids();
        WorkflowInfo workflow1 = WorkflowInfo.newBuilder()
                .setTargetId(TARGET_ID)
                .setName(WORKFLOW_1_NAME)
                .build();
        WorkflowInfo workflow2 = WorkflowInfo.newBuilder()
                .setTargetId(TARGET_ID)
                .setName(WORKFLOW_2_NAME)
                .build();
        dsl.insertInto(WORKFLOW)
                .set(WORKFLOW.ID, WORKFLOW_1_OID)
                .set(WORKFLOW.WORKFLOW_INFO, workflow1)
                .execute();
        dsl.insertInto(WORKFLOW)
                .set(WORKFLOW.ID, WORKFLOW_2_OID)
                .set(WORKFLOW.WORKFLOW_INFO, workflow2)
                .execute();
        PersistentWorkflowIdentityStore testIdentityStore = new PersistentWorkflowIdentityStore(dsl);

        // act
        testIdentityStore.removeOidMappings(Collections.singleton(WORKFLOW_1_OID));

        // assert
        // fetch the remaining rows from the workflow_oid table
        List<Long> oidsRemaining = dsl.select()
                .from(WORKFLOW_OID)
                .fetch()
                .getValues(WORKFLOW_OID.ID, Long.class);
        assertThat(oidsRemaining.size(), equalTo(1));
        assertThat(oidsRemaining.iterator().next(), equalTo(WORKFLOW_2_OID));
        // check that the corresponding workflow was deleted
        List<Long> workflowsRemaining = dsl.select()
                .from(WORKFLOW)
                .fetch()
                .getValues(WORKFLOW.ID, Long.class);
        assertThat(workflowsRemaining.size(), equalTo(1));
        assertThat(workflowsRemaining.iterator().next(), equalTo(WORKFLOW_2_OID));
    }

    /**
     * Test that collecting and restoring work flow identifiers diags.
     *
     * @throws DiagnosticsException - should never happen
     */
    @Test
    public void testDiagsCollectAndRestore() throws DiagnosticsException {
        PersistentWorkflowIdentityStore testIdentityStore = new PersistentWorkflowIdentityStore(dsl);
        persistBothWorkflowOids();
        final List<String> diags = testIdentityStore.collectDiagsStream()
            .collect(Collectors.toList());
        assertEquals(2, diags.size());
        // Clean up db before restore
        dsl.delete(WORKFLOW_OID).execute();
        testIdentityStore.restoreDiags(diags);
        final Result<Record> rs = dsl.select()
                .from(WORKFLOW_OID)
                .fetch();
        assertEquals(2, rs.size());
        assertThat(rs.getValues(WORKFLOW_OID.ID), containsInAnyOrder(WORKFLOW_1_OID, WORKFLOW_2_OID));
        assertThat(rs.getValues(WORKFLOW_OID.TARGET_ID),
                containsInAnyOrder(WORKFLOW_1_TARGET_ID, WORKFLOW_2_TARGET_ID));
        assertThat(rs.getValues(WORKFLOW_OID.EXTERNAL_NAME),
                containsInAnyOrder(WORKFLOW_1_NAME, WORKFLOW_2_NAME));
    }

    /**
     * Persist two rows in the WORKFLOW_OID table.
     */
    private void persistBothWorkflowOids() {
        dsl.insertInto(WORKFLOW_OID)
                .set(WORKFLOW_OID.ID, WORKFLOW_1_OID)
                .set(WORKFLOW_OID.TARGET_ID, WORKFLOW_1_TARGET_ID)
                .set(WORKFLOW_OID.EXTERNAL_NAME, WORKFLOW_1_NAME)
                .execute();
        dsl.insertInto(WORKFLOW_OID)
                .set(WORKFLOW_OID.ID, WORKFLOW_2_OID)
                .set(WORKFLOW_OID.TARGET_ID, WORKFLOW_2_TARGET_ID)
                .set(WORKFLOW_OID.EXTERNAL_NAME, WORKFLOW_2_NAME)
                .execute();
    }

}