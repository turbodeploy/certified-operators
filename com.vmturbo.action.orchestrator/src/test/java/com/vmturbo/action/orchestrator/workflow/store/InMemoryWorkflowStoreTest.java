package com.vmturbo.action.orchestrator.workflow.store;

import static com.vmturbo.action.orchestrator.db.tables.Workflow.WORKFLOW;
import static com.vmturbo.action.orchestrator.db.tables.WorkflowOid.WORKFLOW_OID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.TARGET_ID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_1_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_1_OID;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_2_NAME;
import static com.vmturbo.action.orchestrator.workflow.store.PersistentWorkflowTestConstants.WORKFLOW_2_OID;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import com.vmturbo.action.orchestrator.TestActionOrchestratorDbEndpointConfig;
import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.workflow.rpc.WorkflowFilter;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreUpdate;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Tests {@link InMemoryWorkflowStore}.
 */
@RunWith(Parameterized.class)
public class InMemoryWorkflowStoreTest extends MultiDbTestBase {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    @Spy
    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public InMemoryWorkflowStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Action.ACTION, configurableDbDialect, dialect, "action-orchestrator",
                TestActionOrchestratorDbEndpointConfig::actionOrchestratorEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    @Mock
    private IdentityStore mockIdentityStore;

    @Mock
    private PersistentWorkflowStore mockPersistentWorkflowStore;

    private InMemoryWorkflowStore inMemoryWorkflowStore;

    private static final WorkflowInfo WORKFLOW_1 = WorkflowInfo.newBuilder()
            .setTargetId(TARGET_ID)
            .setName(WORKFLOW_1_NAME)
            .build();
    private static final WorkflowInfo WORKFLOW_2 = WorkflowInfo.newBuilder()
            .setTargetId(TARGET_ID)
            .setName(WORKFLOW_2_NAME)
            .build();

    private static final Clock CLOCK = Clock.systemUTC();

    /**
     * Setup up test environment.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if thread has been interrupted
     * @throws WorkflowStoreException should not be thrown.
     */
    @Before
    public void setup() throws WorkflowStoreException, SQLException, UnsupportedDialectException,
                               InterruptedException {
        MockitoAnnotations.initMocks(this);
        when(mockPersistentWorkflowStore.fetchWorkflows(any())).thenReturn(Collections.emptySet());
        inMemoryWorkflowStore = new InMemoryWorkflowStore(mockPersistentWorkflowStore);
        reset(mockPersistentWorkflowStore);
    }

    /**
     * Tests that when we initialize inMemoryWorkflowStore we synchronize it with
     * PersistentWorkflowStore and follow-up fetch workflow requests won't interact with DB.
     *
     * @throws WorkflowStoreException if something goes wrong
     */
    @Test
    public void testCreatingOfInMemoryWorkflowStore() throws WorkflowStoreException {
        // ARRANGE
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
        final InMemoryWorkflowStore inMemoryWorkflowStore =
                new InMemoryWorkflowStore(dsl, mockIdentityStore, CLOCK);
        // Reset interactions with dsl which used for initializing InMemoryWorkflowStore
        Mockito.reset(dsl);

        // ACT - fetch workflow
        final Optional<Workflow> workflow = inMemoryWorkflowStore.fetchWorkflow(WORKFLOW_1_OID);

        // ASSERT
        // verified that we fetched workflow from in-memory cache without interactions with DB
        Mockito.verifyZeroInteractions(dsl);
        Assert.assertTrue(workflow.isPresent());
        Assert.assertEquals(workflow.get().getWorkflowInfo(), WORKFLOW_1);
    }

    /**
     * Tests that we correctly persisting discovered workflows in InMemoryWorkflowStore and can
     * get them late from in-memory cache without interacting with db.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testSyncUpInMemoryWorkflowStore() throws Exception {
        // ASSERT
        final InMemoryWorkflowStore inMemoryWorkflowStore =
                new InMemoryWorkflowStore(dsl, mockIdentityStore, CLOCK);

        // prepare the OID table rows for these two entries, since the WORKFLOW_OID table holds the
        // primary keys
        dsl.insertInto(WORKFLOW_OID)
                .columns(WORKFLOW_OID.ID, WORKFLOW_OID.TARGET_ID, WORKFLOW_OID.EXTERNAL_NAME)
                .values(WORKFLOW_1_OID, TARGET_ID, WORKFLOW_1_NAME)
                .values(WORKFLOW_2_OID, TARGET_ID, WORKFLOW_2_NAME)
                .execute();

        // map to return from the mock identity store
        final Map<WorkflowDTO.WorkflowInfo, Long> itemOidsMap =
                ImmutableMap.<WorkflowInfo, Long>builder()
                .put(WORKFLOW_1, WORKFLOW_1_OID)
                .put(WORKFLOW_2, WORKFLOW_2_OID)
                .build();
        Mockito.when(mockIdentityStore.fetchOrAssignItemOids(Mockito.anyList()))
                .thenReturn(new IdentityStoreUpdate(itemOidsMap, Collections.emptyMap()));

        // ACT - persisting two discovered workflows related to target
        inMemoryWorkflowStore.persistWorkflows(TARGET_ID, Arrays.asList(WORKFLOW_1, WORKFLOW_2));

        // ASSERT
        // checks that both workflows were persisted in InMemoryWorkflowStore
        Assert.assertTrue(inMemoryWorkflowStore.fetchWorkflow(WORKFLOW_1_OID).isPresent());
        Assert.assertTrue(inMemoryWorkflowStore.fetchWorkflow(WORKFLOW_2_OID).isPresent());
    }

    /**
     * Tests that when we initialize inMemoryWorkflowStore we synchronize it with
     * PersistentWorkflowStore and follow-up fetch workflow requests won't interact with DB.
     *
     * @throws WorkflowStoreException if something goes wrong
     */
    @Test
    public void testFetchingWorkflowsFromInMemoryWorkflowStore() throws WorkflowStoreException {
        // ARRANGE
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
        final InMemoryWorkflowStore inMemoryWorkflowStore =
                new InMemoryWorkflowStore(dsl, mockIdentityStore, CLOCK);
        // Reset interactions with dsl which used for initializing InMemoryWorkflowStore
        Mockito.reset(dsl);

        // ACT - fetch workflows related to certain target
        final Set<Workflow> workflows = inMemoryWorkflowStore.fetchWorkflows(
                new WorkflowFilter(Collections.singletonList(TARGET_ID)));

        // ASSERT
        // verified that we fetched workflows from in-memory cache without interactions with DB
        Mockito.verifyZeroInteractions(dsl);
        Assert.assertEquals(2, workflows.size());
        Assert.assertEquals(Sets.newHashSet(WORKFLOW_1, WORKFLOW_2),
                workflows.stream().map(Workflow::getWorkflowInfo).collect(Collectors.toSet()));
    }

    /**
     * Inserting a workflow should refresh the cache.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testCreateRefreshesCache() throws WorkflowStoreException {
        inMemoryWorkflowStore.insertWorkflow(WORKFLOW_1);
        verify(mockPersistentWorkflowStore, times(1)).fetchWorkflows(any());
    }

    /**
     * Updating a workflow should refresh the cache.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testUpdateRefreshesCache() throws WorkflowStoreException {
        inMemoryWorkflowStore.updateWorkflow(123L, WORKFLOW_1);
        verify(mockPersistentWorkflowStore, times(1)).fetchWorkflows(any());
    }

    /**
     * Deleting a workflow should refresh the cache.
     *
     * @throws WorkflowStoreException should not be thrown.
     */
    @Test
    public void testDeleteRefreshesCache() throws WorkflowStoreException {
        inMemoryWorkflowStore.deleteWorkflow(123L);
        verify(mockPersistentWorkflowStore, times(1)).fetchWorkflows(any());
    }
}
