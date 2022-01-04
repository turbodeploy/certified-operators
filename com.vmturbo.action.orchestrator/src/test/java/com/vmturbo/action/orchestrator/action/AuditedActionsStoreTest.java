package com.vmturbo.action.orchestrator.action;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.TimeZone;

import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.action.orchestrator.TestActionOrchestratorDbEndpointConfig;
import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Tests for {@link AuditActionsStore}.
 */
@RunWith(Parameterized.class)
public class AuditedActionsStoreTest extends MultiDbTestBase {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.DBENDPOINT_CONVERTED_PARAMS;
    }

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
    public AuditedActionsStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Action.ACTION, configurableDbDialect, dialect, "action",
                TestActionOrchestratorDbEndpointConfig::actionOrchestratorEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;
    /**
     * Rule to expect exceptions, if required.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AuditActionsPersistenceManager auditedActionsStore;

    private static final long RECOMMENDATION_ID_1 = 1L;
    private static final long RECOMMENDATION_ID_2 = 2L;
    private static final long RECOMMENDATION_ID_3 = 3L;
    private static final long WORKFLOW_ID_1 = 10L;
    private static final long WORKFLOW_ID_2 = 11L;
    private static final long TARGET_ENTITY_ID_1 = 21L;
    private static final long TARGET_ENTITY_ID_2 = 22L;
    private static final long TARGET_ENTITY_ID_3 = 23L;
    private static final String VMEM_RESIZE_UP_ONGEN = ActionSettingSpecs.getSubSettingFromActionModeSetting(
        ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds,
        ActionSettingType.ON_GEN);

    /**
     * We override the current time in milliseconds to Mon Jan 18 2021 20:25:18 EST for convenience
     * of unit testing.
     */
    private static final Long CURRENT_TIME = 1611001518345L;

    /**
     * Set up for tests.
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if thread has been interrupted
     */
    @Before
    public void setUp() throws SQLException, UnsupportedDialectException, InterruptedException {
        auditedActionsStore = new AuditActionsStore(dsl);
    }

    /**
     * Tests persisting of audited ON_GEN actions.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testPersistingAuditedActions() throws ActionStoreOperationException {
        final AuditedActionInfo auditedAction1 =
                new AuditedActionInfo(RECOMMENDATION_ID_1, WORKFLOW_ID_1, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        final AuditedActionInfo auditedAction2 =
                new AuditedActionInfo(RECOMMENDATION_ID_2, WORKFLOW_ID_2, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        final HashSet<AuditedActionInfo> actionsForAudit =
                Sets.newHashSet(auditedAction1, auditedAction2);
        auditedActionsStore.persistActions(actionsForAudit);

        final Collection<AuditedActionInfo> auditedActions =
                auditedActionsStore.getActions();
        Assert.assertEquals(2, auditedActions.size());
        Assert.assertTrue(CollectionUtils.isEqualCollection(actionsForAudit, auditedActions));
    }

    /**
     * Tests persisting of CLEARED audited actions. In this case persists cleared_timestamp.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testPersistingClearedActions() throws ActionStoreOperationException {
        final AuditedActionInfo auditedOnGenAction =
                new AuditedActionInfo(RECOMMENDATION_ID_1, WORKFLOW_ID_1, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        auditedActionsStore.persistActions(Collections.singletonList(auditedOnGenAction));

        final AuditedActionInfo clearedAction =
                new AuditedActionInfo(RECOMMENDATION_ID_1, WORKFLOW_ID_1, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN,
                        Optional.of(CURRENT_TIME));
        auditedActionsStore.persistActions(Collections.singletonList(clearedAction));

        final Collection<AuditedActionInfo> auditedActions =
                auditedActionsStore.getActions();
        Assert.assertEquals(1, auditedActions.size());
        final AuditedActionInfo auditedAction = auditedActions.iterator().next();
        Assert.assertEquals(RECOMMENDATION_ID_1, auditedAction.getRecommendationId());
        Assert.assertEquals(WORKFLOW_ID_1, auditedAction.getWorkflowId());

        // Database only saves up to seconds, so we ignore the millis by comparing
        // year, month, day, hour, minute, and second manually
        final LocalDateTime clearedTimestamp =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(CURRENT_TIME),
                        TimeZone.getDefault().toZoneId());
        final LocalDateTime clearedTimestampOfAuditedAction =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(auditedAction.getClearedTimestamp().get()),
                        TimeZone.getDefault().toZoneId());
        Assert.assertEquals(clearedTimestamp.getYear(),
                clearedTimestampOfAuditedAction.getYear());
        Assert.assertEquals(clearedTimestamp.getMonth(),
                clearedTimestampOfAuditedAction.getMonth());
        Assert.assertEquals(clearedTimestamp.getDayOfMonth(),
                clearedTimestampOfAuditedAction.getDayOfMonth());
        Assert.assertEquals(clearedTimestamp.getHour(),
                clearedTimestampOfAuditedAction.getHour());
        Assert.assertEquals(clearedTimestamp.getMinute(),
                clearedTimestampOfAuditedAction.getMinute());
        Assert.assertEquals(clearedTimestamp.getSecond(),
                clearedTimestampOfAuditedAction.getSecond());
    }

    /**
     * Tests removing audited actions.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testRemovingAuditedActions() throws ActionStoreOperationException {
        final AuditedActionInfo auditedOnGenAction =
                new AuditedActionInfo(RECOMMENDATION_ID_1, WORKFLOW_ID_1, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        auditedActionsStore.persistActions(Collections.singletonList(auditedOnGenAction));

        final AuditedActionInfo clearedAction =
                new AuditedActionInfo(RECOMMENDATION_ID_1, WORKFLOW_ID_1, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN,
                        Optional.of(CURRENT_TIME));
        auditedActionsStore.persistActions(Collections.singletonList(clearedAction));

        auditedActionsStore.removeActionWorkflows(
                Collections.singletonList(Pair.create(RECOMMENDATION_ID_1, WORKFLOW_ID_1)));

        final Collection<AuditedActionInfo> auditedActions =
                auditedActionsStore.getActions();
        Assert.assertTrue(auditedActions.isEmpty());
    }

    /**
     * Tests removing audited actions associated with certain workflow.
     *
     * @throws ActionStoreOperationException if something goes wrong while operating
     * in DAO layer
     */
    @Test
    public void testRemovingAuditedActionsRelatedToWorkflow() throws ActionStoreOperationException {
        final AuditedActionInfo auditedAction1 =
                new AuditedActionInfo(RECOMMENDATION_ID_1, WORKFLOW_ID_1, TARGET_ENTITY_ID_1, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        final AuditedActionInfo auditedAction2 =
                new AuditedActionInfo(RECOMMENDATION_ID_2, WORKFLOW_ID_2, TARGET_ENTITY_ID_2, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        final AuditedActionInfo auditedAction3 =
                new AuditedActionInfo(RECOMMENDATION_ID_3, WORKFLOW_ID_2, TARGET_ENTITY_ID_3, VMEM_RESIZE_UP_ONGEN, Optional.empty());
        final HashSet<AuditedActionInfo> actionsForAudit =
                Sets.newHashSet(auditedAction1, auditedAction2, auditedAction3);
        auditedActionsStore.persistActions(actionsForAudit);

        auditedActionsStore.deleteActionsRelatedToWorkflow(WORKFLOW_ID_2);

        final Collection<AuditedActionInfo> auditedActions =
                auditedActionsStore.getActions();
        Assert.assertEquals(1, auditedActions.size());
        final AuditedActionInfo auditedAction = auditedActions.iterator().next();
        // all actions associated with WORKFLOW_ID_2 were removed
        Assert.assertEquals(WORKFLOW_ID_1, auditedAction.getWorkflowId());
    }
}
