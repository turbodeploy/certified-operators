package com.vmturbo.action.orchestrator.store.pipeline;

import static org.mockito.Mockito.mock;

import java.time.Clock;

import org.junit.Test;

import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.identity.IdentityService;

/**
 * Tests for action pipeline factories.
 */
public class ActionPipelineFactoryTest {

    private final ActionStorehouse storehouse = mock(ActionStorehouse.class);
    private final ActionAutomationManager automationManager = mock(ActionAutomationManager.class);
    private final AtomicActionFactory atomicActionFactory = mock(AtomicActionFactory.class);
    private final EntitiesAndSettingsSnapshotFactory entitiesAndSettingsSnapshotFactory =
        mock(EntitiesAndSettingsSnapshotFactory.class);
    private final long maxWaitTimeMinutes = 10;
    private final ProbeCapabilityCache probeCapabilityCache = mock(ProbeCapabilityCache.class);
    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);
    private final IActionFactory actionFactory = mock(IActionFactory.class);
    private final Clock clock = Clock.systemUTC();
    private final int queryTimeWindowForLastExecutedActionsMins = 10;
    @SuppressWarnings("unchecked")
    private final IdentityService<ActionInfo> actionIdentityService = (IdentityService<ActionInfo>)mock(IdentityService.class);
    private final ActionTargetSelector actionTargetSelector = mock(ActionTargetSelector.class);
    private final ActionTranslator actionTranslator = mock(ActionTranslator.class);
    private final LiveActionsStatistician actionsStatistician = mock(LiveActionsStatistician.class);
    private final ActionAuditSender actionAuditSender = mock(ActionAuditSender.class);
    private final AuditedActionsManager auditedActionsManager = mock(AuditedActionsManager.class);

    private final ActionPlan actionPlan = ActionPlan.newBuilder()
        .setId(1)
        .setInfo(ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(123)
                    .setTopologyContextId(1234L))))
        .build();

    private final ActionPlan riBuyPlan = ActionPlan.newBuilder()
        .setId(1)
        .setInfo(ActionPlanInfo.newBuilder()
            .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                .setTopologyContextId(1234L)
                    .build()))
        .build();

    /**
     * Ensure that live actions pipeline context members are set up properly.
     */
    @Test
    public void testLiveActionsPipeline() {
        final LiveActionPipelineFactory factory = new LiveActionPipelineFactory(storehouse,
            automationManager, atomicActionFactory, entitiesAndSettingsSnapshotFactory,
            maxWaitTimeMinutes, probeCapabilityCache, actionHistoryDao,
            actionFactory, clock, queryTimeWindowForLastExecutedActionsMins,
            actionIdentityService, actionTargetSelector, actionTranslator, actionsStatistician,
            actionAuditSender, auditedActionsManager, mock(ActionTopologyStore.class), 777777L, 100);

        // If there is an issue with pipeline context members, attempting to create a pipeline
        // will result in an exception and fail the test.
        factory.actionPipeline(actionPlan);
    }


    /**
     * Ensure that live actions pipeline context members are set up properly for buy RI.
     */
    @Test
    public void testBuyRILiveActionsPipeline() {
        final LiveActionPipelineFactory factory = new LiveActionPipelineFactory(storehouse,
            automationManager, atomicActionFactory, entitiesAndSettingsSnapshotFactory,
            maxWaitTimeMinutes, probeCapabilityCache, actionHistoryDao,
            actionFactory, clock, queryTimeWindowForLastExecutedActionsMins,
            actionIdentityService, actionTargetSelector, actionTranslator, actionsStatistician,
            actionAuditSender, auditedActionsManager, mock(ActionTopologyStore.class), 777777L, 100);

        // If there is an issue with pipeline context members, attempting to create a pipeline
        // will result in an exception and fail the test.
        factory.actionPipeline(riBuyPlan);
    }

    /**
     * Ensure that plan actions pipeline context members are set up properly.
     */
    @Test
    public void testPlanActionsPipeline() {
        final PlanActionPipelineFactory factory = new PlanActionPipelineFactory(storehouse, atomicActionFactory,
                Clock.systemUTC());

        // If there is an issue with pipeline context members, attempting to create a pipeline
        // will result in an exception and fail the test.
        factory.actionPipeline(actionPlan);
    }
}
