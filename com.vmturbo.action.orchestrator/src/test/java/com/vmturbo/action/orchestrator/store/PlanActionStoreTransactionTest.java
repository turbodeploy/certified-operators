package com.vmturbo.action.orchestrator.store;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.passthroughTranslator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

import com.google.common.collect.ImmutableMap;

/**
 * Test transaction failures with the {@link PlanActionStore}.
 */
public class PlanActionStoreTransactionTest {

    private final long vm1 = 1;
    private final long vm2 = 2;
    private final long hostA = 0xA;
    private final long hostB = 0xB;

    private final List<ActionDTO.Action> recommendations = Arrays.asList(
        ActionOrchestratorTestUtils.createMoveRecommendation(0xfeed,vm1,hostA,3,hostB,3),
        ActionOrchestratorTestUtils.createMoveRecommendation(0xfad,vm2,hostA,3,hostB,3)
    );

    private final long initialPlanId = 1;
    private final long topologyContextId = 3;
    private final long realtimeId = 777777L;
    private final ActionPlan actionPlan = ActionPlan.newBuilder()
        .setId(initialPlanId)
        .setInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(2)
                    .setTopologyContextId(topologyContextId)
                    .setTopologyType(TopologyType.REALTIME))))
        .addAllAction(recommendations)
        .build();
    private final ActionModeCalculator actionModeCalculator = mock(ActionModeCalculator.class);
    private final EntitiesAndSettingsSnapshotFactory entitiesSnapshotFactory = mock(EntitiesAndSettingsSnapshotFactory.class);
    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
    private final ActionTranslator actionTranslator = passthroughTranslator();

    private final IActionFactory actionFactory = new ActionFactory(actionModeCalculator);
    private PlanActionStore actionStore;

    @Before
    public void setup() {
        setEntitiesOIDs();
    }

    public void setEntitiesOIDs() {
        when(entitiesSnapshotFactory.newSnapshot(any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getEntityFromOid(eq(vm1)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm1,
                EntityType.VIRTUAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(vm2)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(vm2,
                EntityType.VIRTUAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostA)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostA,
                EntityType.PHYSICAL_MACHINE.getNumber()));
        when(snapshot.getEntityFromOid(eq(hostB)))
            .thenReturn(ActionOrchestratorTestUtils.createTopologyEntityDTO(hostB,
                EntityType.PHYSICAL_MACHINE.getNumber()));
    }

    @Test
    public void testRollbackWhenErrorDuringPopulateClean() throws Exception {
        MockDataProvider mockProvider = providerFailingOn("DELETE");
        actionStore = new PlanActionStore(actionFactory, contextFor(mockProvider), topologyContextId, entitiesSnapshotFactory, actionTranslator, realtimeId);

        // The first call does not clear because there is nothing in the store yet.
        assertTrue(actionStore.populateRecommendedActions(actionPlan));

        // The second call will call clear and should trigger transaction rollback.
        assertFalse(actionStore.populateRecommendedActions(actionPlan.toBuilder().setId(1234L).build()));

        // The store should have failed to populate and planId should continue to be null.
        assertEquals(initialPlanId, (long)actionStore.getActionPlanId(ActionPlanType.MARKET).get());
    }

    @Test
    public void testRollbackWhenErrorDuringPopulateStore() throws Exception {
        MockDataProvider mockProvider = providerFailingOn("INSERT");
        actionStore = new PlanActionStore(actionFactory, contextFor(mockProvider), topologyContextId, entitiesSnapshotFactory, actionTranslator, realtimeId);

        // The attempt to store actions should fail.
        assertFalse(actionStore.populateRecommendedActions(actionPlan));

        // And the plan ID should not get set.
        assertFalse(actionStore.getActionPlanId(ActionPlanType.MARKET).isPresent());
    }

    @Test
    public void testRollbackWhenErrorDuringOverwrite() throws Exception {
        MockDataProvider mockProvider = providerFailingOn("INSERT");
        actionStore = new PlanActionStore(actionFactory, contextFor(mockProvider), topologyContextId, entitiesSnapshotFactory, actionTranslator, realtimeId);

        // The attempt to store actions should fail.
        List<Action> actions = actionPlan.getActionList().stream()
            .map(action -> actionFactory.newAction(action, actionPlan.getId()))
            .collect(Collectors.toList());
        assertFalse(actionStore.overwriteActions(ImmutableMap.of(ActionPlanType.MARKET, actions)));

        // And the plan ID should not get set.
        assertFalse(actionStore.getActionPlanId(ActionPlanType.MARKET).isPresent());
    }

    @Test
    public void testRollbackDuringClear() throws Exception {
        MockDataProvider mockProvider = providerFailingOn("DELETE");
        actionStore = new PlanActionStore(actionFactory, contextFor(mockProvider), topologyContextId, entitiesSnapshotFactory, actionTranslator, realtimeId);

        // The first call does not clear because there is nothing in the store yet.
        assertTrue(actionStore.populateRecommendedActions(actionPlan));

        // Calling clear will clear records, which should trigger the failure
        assertFalse(actionStore.clear());

        // The store should have failed to populate and planId should continue to be null.
        assertEquals(initialPlanId, (long)actionStore.getActionPlanId(ActionPlanType.MARKET).get());
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
