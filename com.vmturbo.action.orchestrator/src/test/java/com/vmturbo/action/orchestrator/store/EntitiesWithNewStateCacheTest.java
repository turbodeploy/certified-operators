package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Integration tests related to the LiveActionStore.
 */
public class EntitiesWithNewStateCacheTest {

    private final long firstPlanId = 0xBEADED;

    private final long vm1 = 1;

    private final long hostA = 0xA;
    private final long hostB = 0xB;
    private final long hostC = 0xC;
    private final int vmType = 1;

    private  LiveActions actions;
    private EntitiesWithNewStateCache entitiesWithNewStateCache;
    private  ActionModeCalculator actionModeCalculator;

    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);
    private Clock clock = new MutableFixedClock(1_000_000);
    private UserSessionContext userSessionContext = mock(UserSessionContext.class);
    private final AcceptedActionsDAO acceptedActionsStore = Mockito.mock(AcceptedActionsDAO.class);
    private final RejectedActionsDAO rejectedActionsStore = Mockito.mock(RejectedActionsDAO.class);

    /**
     * Set up.
     */
    @Before
    public void setup() {
        actions =
                new LiveActions(actionHistoryDao, acceptedActionsStore, rejectedActionsStore, clock,
                        userSessionContext, Mockito.mock(InvolvedEntitiesExpander.class));
        entitiesWithNewStateCache = new EntitiesWithNewStateCache(actions);
        actionModeCalculator = new ActionModeCalculator();
        IdentityGenerator.initPrefix(0);
    }

    /**
     * Tests that the right actions get cleared for a host going into maintenance and for a host
     * going out of maintenance.
     */
    @Test
    public void testUpdateHostsWithNewState() {
        final Action moveInAction = new Action(move(vm1, hostA, vmType, hostB,
            vmType).setDeprecatedImportance(1).setExecutable(false).build(), firstPlanId,
            actionModeCalculator, 1);
        final Action moveOutAction = new Action(move(vm1, hostC, vmType, hostA,
            vmType).setDeprecatedImportance(1).setExecutable(false).build(), firstPlanId,
            actionModeCalculator, 2);
        final Action notAffectingAction = new Action(move(vm1, hostB, vmType,
            hostC, vmType).setDeprecatedImportance(1).setExecutable(false).build(), firstPlanId,
            actionModeCalculator, 3);

        EntitiesWithNewState entitiesWithNewState = EntitiesWithNewState.newBuilder()
            .setStateChangeId(3)
            .addTopologyEntity(TopologyEntityDTO
                .newBuilder().setOid(hostB).setEntityState(EntityState.MAINTENANCE)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build())
            .addTopologyEntity(TopologyEntityDTO
                .newBuilder().setOid(hostC).setEntityState(EntityState.POWERED_ON)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build())
            .build();

        actions.replaceMarketActions(Stream.of(moveInAction, moveOutAction,
            notAffectingAction));
        entitiesWithNewStateCache.updateHostsWithNewState(entitiesWithNewState);


        assertFalse(actions.getAction(moveInAction.getId()).isPresent());
        assertFalse(actions.getAction(moveOutAction.getId()).isPresent());
        assertTrue(actions.getAction(notAffectingAction.getId()).isPresent());
    }

    /**
     * Tests that the cache gets correctly cleaned once we receive a topology with a newer
     * state change id.
     */
    @Test
    public void testUpdateHostsWithNewStateWithNewTopology() {
        final int stateChangeId = 0;
        final Action moveInAction = new Action(move(vm1, hostA, vmType, hostB,
            vmType).setDeprecatedImportance(1).setExecutable(false).build(), firstPlanId,
            actionModeCalculator, 1);
        final Action moveOutAction = new Action(move(vm1, hostC, vmType, hostA,
            vmType).setDeprecatedImportance(1).setExecutable(false).build(), firstPlanId,
            actionModeCalculator, 2);
        final Action notAffectingAction = new Action(move(vm1, hostB, vmType,
            hostC, vmType).setDeprecatedImportance(1).setExecutable(false).build(), firstPlanId,
            actionModeCalculator, 3);

        EntitiesWithNewState entitiesWithNewState = EntitiesWithNewState.newBuilder()
            .setStateChangeId(stateChangeId)
            .addTopologyEntity(TopologyEntityDTO
                .newBuilder().setOid(hostB).setEntityState(EntityState.MAINTENANCE)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build())
            .addTopologyEntity(TopologyEntityDTO
                .newBuilder().setOid(hostC).setEntityState(EntityState.POWERED_ON)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build())
            .build();

        entitiesWithNewStateCache.updateHostsWithNewState(entitiesWithNewState);

        actions.replaceMarketActions(Stream.of(moveInAction, moveOutAction,
            notAffectingAction));

        entitiesWithNewStateCache.clearActionsAndUpdateCache(stateChangeId + 1);


        assertTrue(actions.getAction(moveInAction.getId()).isPresent());
        assertTrue(actions.getAction(moveOutAction.getId()).isPresent());
        assertTrue(actions.getAction(notAffectingAction.getId()).isPresent());
    }

    private static ActionDTO.Action.Builder move(long targetId,
                                                 long sourceId, int sourceType,
                                                 long destinationId, int destinationType) {
        return ActionOrchestratorTestUtils.createMoveRecommendation(IdentityGenerator.next(),
            targetId, sourceId, sourceType, destinationId, destinationType).toBuilder();
    }
}
