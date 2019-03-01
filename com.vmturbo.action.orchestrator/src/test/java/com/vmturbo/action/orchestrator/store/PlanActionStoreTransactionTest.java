package com.vmturbo.action.orchestrator.store;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockResult;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test transaction failures with the {@link PlanActionStore}.
 */
public class PlanActionStoreTransactionTest {

    private final List<ActionDTO.Action> recommendations = Arrays.asList(
        ActionOrchestratorTestUtils.createMoveRecommendation(0xfeed),
        ActionOrchestratorTestUtils.createMoveRecommendation(0xfad)
    );

    private final long initialPlanId = 1;
    private final long topologyContextId = 3;
    private final ActionPlan actionPlan = ActionPlan.newBuilder()
        .setId(initialPlanId)
        .setTopologyId(2)
        .setTopologyContextId(topologyContextId)
        .addAllAction(recommendations)
        .build();

    private final IActionFactory actionFactory = new ActionFactory();
    private PlanActionStore actionStore;

    @Test
    public void testRollbackWhenErrorDuringPopulateClean() throws Exception {
        MockDataProvider mockProvider = providerFailingOn("DELETE");
        actionStore = new PlanActionStore(actionFactory, contextFor(mockProvider), topologyContextId);

        // The first call does not clear because there is nothing in the store yet.
        assertTrue(actionStore.populateRecommendedActions(actionPlan));

        // The second call will call clear and should trigger transaction rollback.
        assertFalse(actionStore.populateRecommendedActions(actionPlan.toBuilder().setId(1234L).build()));

        // The store should have failed to populate and planId should continue to be null.
        assertEquals(initialPlanId, (long)actionStore.getActionPlanId().get());
    }

    @Test
    public void testRollbackWhenErrorDuringPopulateStore() throws Exception {
        MockDataProvider mockProvider = providerFailingOn("INSERT");
        actionStore = new PlanActionStore(actionFactory, contextFor(mockProvider), topologyContextId);

        // The attempt to store actions should fail.
        assertFalse(actionStore.populateRecommendedActions(actionPlan));

        // And the plan ID should not get set.
        assertFalse(actionStore.getActionPlanId().isPresent());
    }

    @Test
    public void testRollbackWhenErrorDuringOverwrite() throws Exception {
        MockDataProvider mockProvider = providerFailingOn("INSERT");
        actionStore = new PlanActionStore(actionFactory, contextFor(mockProvider), topologyContextId);

        // The attempt to store actions should fail.
        List<Action> actions = actionPlan.getActionList().stream()
            .map(action -> actionFactory.newAction(action, actionPlan.getId()))
            .collect(Collectors.toList());
        assertFalse(actionStore.overwriteActions(actions));

        // And the plan ID should not get set.
        assertFalse(actionStore.getActionPlanId().isPresent());
    }

    @Test
    public void testRollbackDuringClear() throws Exception {
        MockDataProvider mockProvider = providerFailingOn("DELETE");
        actionStore = new PlanActionStore(actionFactory, contextFor(mockProvider), topologyContextId);

        // The first call does not clear because there is nothing in the store yet.
        assertTrue(actionStore.populateRecommendedActions(actionPlan));

        // Calling clear will clear records, which should trigger the failure
        assertFalse(actionStore.clear());

        // The store should have failed to populate and planId should continue to be null.
        assertEquals(initialPlanId, (long)actionStore.getActionPlanId().get());
    }

    private static DSLContext contextFor(@Nonnull final MockDataProvider mockProvider) {
        return DSL.using(new MockConnection(mockProvider), SQLDialect.MARIADB);
    }

    private static MockDataProvider providerFailingOn(@Nonnull final String operationName) {
        return mockExecuteContext -> {
            if (mockExecuteContext.sql().toUpperCase().contains(operationName.toUpperCase())) {
                throw new SQLException("Failed");
            }

            return new MockResult[0];
        };
    }
}
