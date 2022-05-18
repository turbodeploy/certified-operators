package com.vmturbo.action.orchestrator.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockedByRelation;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockedByRelation.BlockedByResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockingRelation;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockingRelation.BlockingResize;
import com.vmturbo.common.protobuf.action.ActionDTO.CausedByRelation;
import com.vmturbo.common.protobuf.action.ActionDTO.CausedByRelation.CausedByProvision;
import com.vmturbo.common.protobuf.action.ActionDTO.MarketRelatedAction;
import com.vmturbo.common.protobuf.action.ActionDTO.RelatedAction;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test {@link RelatedActionsInjector}.
 */
public class RelatedActionsInjectorTest {

    private static final long POD_PROVISION_ID = 11;
    private static final long RE_RECOMMENDED_POD_PROVISION_ID = 12;
    private static final String POD_NAME = "POD";
    private static final long POD_OID = 111;
    private static final long VM_PROVISION_ID = 21;
    private static final long RE_RECOMMENDED_VM_PROVISION_ID = 22;
    private static final long VM_OID = 222;
    private static final String VM_NAME = "VM";
    private static final long WC_ATOMIC_RESIZE_ID = 31;
    private static final long RE_RECOMMENDED_WC_ATOMIC_RESIZE_ID = 32;
    private static final long WC_OID = 333;
    private static final String WC_NAME = "WORKLOAD_CONTROLLER";
    private static final long NS_RESIZE_ID = 41;
    private static final long NS_RESIZE_ID1 = 41;
    private static final long NS_RESIZE_ID2 = 42;
    private static final long RE_RECOMMENDED_NS_RESIZE_ID1 = 51;
    private static final long RE_RECOMMENDED_NS_RESIZE_ID2 = 52;
    private static final long NS_OID = 444;
    private static final String NS_NAME = "NAMESPACE";

    private final long actionPlanId = 1L;
    private final LiveActionStore liveActionStore = Mockito.mock(LiveActionStore.class);
    private final EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot =
            Mockito.mock(EntitiesAndSettingsSnapshot.class);

    /**
     * Static tests initialization.
     */
    @BeforeClass
    public static void initClass() {
        IdentityGenerator.initPrefix(0);
    }

    /**
     * Test {@link RelatedActionsInjector#injectSymmetricRelatedActions()} for related market actions.
     */
    @Test
    public void testInjectSymmetricRelatedMarketActions() {
        Action podProvision = ActionOrchestratorTestUtils.createProvisionAction(POD_PROVISION_ID,
                POD_OID, EntityType.CONTAINER_POD_VALUE);
        Action vmProvision = ActionOrchestratorTestUtils.createProvisionAction(VM_PROVISION_ID,
                VM_OID, EntityType.VIRTUAL_MACHINE_VALUE);
        Map<Long, Action> actionsMap = ImmutableMap.of(
                podProvision.getId(), podProvision,
                vmProvision.getId(), vmProvision);
        when(liveActionStore.getActions()).thenReturn(actionsMap);
        when(entitiesAndSettingsSnapshot.getEntityFromOid(POD_OID)).thenReturn(mockActionPartialEntityOpt(
                POD_OID, POD_NAME));
        when(entitiesAndSettingsSnapshot.getEntityFromOid(VM_OID)).thenReturn(mockActionPartialEntityOpt(
                VM_OID, VM_NAME));

        Map<Long, List<MarketRelatedAction>> marketActionsRelationsMap = ImmutableMap.of(
                POD_PROVISION_ID, Collections.singletonList(
                        mockCausedByMarketRelatedAction(VM_PROVISION_ID, VM_OID, EntityType.VIRTUAL_MACHINE_VALUE)));
        RelatedActionsInjector relatedActionsInjector =
                new RelatedActionsInjector(actionPlanId, marketActionsRelationsMap, Collections.emptyMap(),
                        Collections.emptyMap(), liveActionStore, entitiesAndSettingsSnapshot);

        relatedActionsInjector.injectSymmetricRelatedActions();

        // Verify pod provision action has related VM provision action with CAUSED_BY relation.
        assertNotNull(podProvision.getRelatedActions());
        assertEquals(1, podProvision.getRelatedActions().size());
        RelatedAction podRelatedAction = podProvision.getRelatedActions().get(0);
        assertTrue(podRelatedAction.hasActionEntity());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, podRelatedAction.getActionEntity().getType());
        assertEquals(VM_OID, podRelatedAction.getActionEntity().getId());
        assertTrue(podRelatedAction.hasRecommendationId());
        assertTrue(podRelatedAction.hasCausedByRelation());
        assertTrue(podRelatedAction.getCausedByRelation().hasProvision());

        // Verify VM provision action has related POD provision action with CAUSING relation.
        assertNotNull(vmProvision.getRelatedActions());
        assertEquals(1, vmProvision.getRelatedActions().size());
        RelatedAction vmRelatedAction = vmProvision.getRelatedActions().get(0);
        assertTrue(vmRelatedAction.hasActionEntity());
        assertEquals(EntityType.CONTAINER_POD_VALUE, vmRelatedAction.getActionEntity().getType());
        assertEquals(POD_OID, vmRelatedAction.getActionEntity().getId());
        assertTrue(vmRelatedAction.hasRecommendationId());
        assertTrue(vmRelatedAction.hasCausingRelation());
        assertTrue(vmRelatedAction.getCausingRelation().hasProvision());
    }

    /**
     * Test {@link RelatedActionsInjector#injectSymmetricRelatedActions()} for related market actions
     * with re-recommended action ID.
     */
    @Test
    public void testInjectSymmetricRelatedMarketActionsWithReRecommendedAction() {
        Action podProvision = ActionOrchestratorTestUtils.createProvisionAction(POD_PROVISION_ID,
                POD_OID, EntityType.CONTAINER_POD_VALUE);
        Action vmProvision = ActionOrchestratorTestUtils.createProvisionAction(VM_PROVISION_ID,
                VM_OID, EntityType.VIRTUAL_MACHINE_VALUE);
        Map<Long, Action> actionsMap = ImmutableMap.of(
                podProvision.getId(), podProvision,
                vmProvision.getId(), vmProvision);
        when(liveActionStore.getActions()).thenReturn(actionsMap);
        when(entitiesAndSettingsSnapshot.getEntityFromOid(POD_OID)).thenReturn(mockActionPartialEntityOpt(
                POD_OID, POD_NAME));
        when(entitiesAndSettingsSnapshot.getEntityFromOid(VM_OID)).thenReturn(mockActionPartialEntityOpt(
                VM_OID, VM_NAME));

        // MarketActionsRelationsMap has re-recommended action ID.
        Map<Long, List<MarketRelatedAction>> marketActionsRelationsMap = ImmutableMap.of(
                RE_RECOMMENDED_POD_PROVISION_ID, Collections.singletonList(
                        mockCausedByMarketRelatedAction(RE_RECOMMENDED_VM_PROVISION_ID, VM_OID, EntityType.VIRTUAL_MACHINE_VALUE)));
        Map<Long, Long> reRecommendedIdMap = ImmutableMap.of(
                RE_RECOMMENDED_POD_PROVISION_ID, POD_PROVISION_ID,
                RE_RECOMMENDED_VM_PROVISION_ID, VM_PROVISION_ID);
        when(liveActionStore.getReRecommendedIdMap()).thenReturn(reRecommendedIdMap);

        RelatedActionsInjector relatedActionsInjector =
                new RelatedActionsInjector(actionPlanId, marketActionsRelationsMap, Collections.emptyMap(),
                        Collections.emptyMap(), liveActionStore, entitiesAndSettingsSnapshot);
        relatedActionsInjector.injectSymmetricRelatedActions();

        // Verify pod provision action has related VM provision action with CAUSED_BY relation.
        assertNotNull(podProvision.getRelatedActions());
        assertEquals(1, podProvision.getRelatedActions().size());
        RelatedAction podRelatedAction = podProvision.getRelatedActions().get(0);
        assertTrue(podRelatedAction.hasActionEntity());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, podRelatedAction.getActionEntity().getType());
        assertEquals(VM_OID, podRelatedAction.getActionEntity().getId());
        assertTrue(podRelatedAction.hasRecommendationId());
        assertTrue(podRelatedAction.hasCausedByRelation());
        assertTrue(podRelatedAction.getCausedByRelation().hasProvision());

        // Verify VM provision action has related POD provision action with CAUSING relation.
        assertNotNull(vmProvision.getRelatedActions());
        assertEquals(1, vmProvision.getRelatedActions().size());
        RelatedAction vmRelatedAction = vmProvision.getRelatedActions().get(0);
        assertTrue(vmRelatedAction.hasActionEntity());
        assertEquals(EntityType.CONTAINER_POD_VALUE, vmRelatedAction.getActionEntity().getType());
        assertEquals(POD_OID, vmRelatedAction.getActionEntity().getId());
        assertTrue(vmRelatedAction.hasRecommendationId());
        assertTrue(vmRelatedAction.hasCausingRelation());
        assertTrue(vmRelatedAction.getCausingRelation().hasProvision());
    }

    /**
     * Test {@link RelatedActionsInjector#injectSymmetricRelatedActions()} for related market actions
     * with re-recommended action ID.
     */
    @Test
    public void testInjectSymmetricRelatedMarketActionsWithMissingReRecommendedAction() {
        Action podProvision = ActionOrchestratorTestUtils.createProvisionAction(POD_PROVISION_ID,
                POD_OID, EntityType.CONTAINER_POD_VALUE);
        Action vmProvision = ActionOrchestratorTestUtils.createProvisionAction(VM_PROVISION_ID,
                VM_OID, EntityType.VIRTUAL_MACHINE_VALUE);
        Map<Long, Action> actionsMap = ImmutableMap.of(
                podProvision.getId(), podProvision,
                vmProvision.getId(), vmProvision);
        when(liveActionStore.getActions()).thenReturn(actionsMap);
        when(entitiesAndSettingsSnapshot.getEntityFromOid(POD_OID)).thenReturn(mockActionPartialEntityOpt(
                POD_OID, POD_NAME));
        when(entitiesAndSettingsSnapshot.getEntityFromOid(VM_OID)).thenReturn(mockActionPartialEntityOpt(
                VM_OID, VM_NAME));

        // MarketActionsRelationsMap has re-recommended action ID.
        Map<Long, List<MarketRelatedAction>> marketActionsRelationsMap = ImmutableMap.of(
                RE_RECOMMENDED_POD_PROVISION_ID, Collections.singletonList(
                        mockCausedByMarketRelatedAction(RE_RECOMMENDED_VM_PROVISION_ID, VM_OID, EntityType.VIRTUAL_MACHINE_VALUE)));
        Map<Long, Long> reRecommendedIdMap = ImmutableMap.of(
                RE_RECOMMENDED_POD_PROVISION_ID, POD_PROVISION_ID);
        when(liveActionStore.getReRecommendedIdMap()).thenReturn(reRecommendedIdMap);

        RelatedActionsInjector relatedActionsInjector =
                new RelatedActionsInjector(actionPlanId, marketActionsRelationsMap, Collections.emptyMap(),
                        Collections.emptyMap(), liveActionStore, entitiesAndSettingsSnapshot);
        relatedActionsInjector.injectSymmetricRelatedActions();

        // Verify pod provision action has related VM provision action with CAUSED_BY relation.
        assertNotNull(podProvision.getRelatedActions());
        assertEquals(0, podProvision.getRelatedActions().size());

        // Verify VM provision action has related POD provision action with CAUSING relation.
        assertNotNull(vmProvision.getRelatedActions());
        assertEquals(0, vmProvision.getRelatedActions().size());
    }

    /**
     * Test {@link RelatedActionsInjector#injectSymmetricRelatedActions()} for related atomic actions.
     */
    @Test
    public void testInjectSymmetricRelatedAtomicActions() {
        Action wcAtomicResize = ActionOrchestratorTestUtils.createAtomicResizeAction(
                WC_ATOMIC_RESIZE_ID, WC_OID);
        Action nsResize = ActionOrchestratorTestUtils.createResizeAction(NS_RESIZE_ID, NS_OID, EntityType.NAMESPACE_VALUE);
        Map<Long, Action> actionsMap = ImmutableMap.of(
                wcAtomicResize.getId(), wcAtomicResize,
                nsResize.getId(), nsResize);
        when(liveActionStore.getActions()).thenReturn(actionsMap);
        when(entitiesAndSettingsSnapshot.getEntityFromOid(WC_OID)).thenReturn(mockActionPartialEntityOpt(
                WC_OID, WC_NAME));
        when(entitiesAndSettingsSnapshot.getEntityFromOid(NS_OID)).thenReturn(mockActionPartialEntityOpt(
                NS_OID, NS_NAME));

        Map<Long, List<MarketRelatedAction>> atomicActionsRelationsMap = ImmutableMap.of(
                WC_ATOMIC_RESIZE_ID, Collections.singletonList(
                        mockBlockedByMarketRelatedAction(NS_RESIZE_ID, NS_OID, EntityType.NAMESPACE_VALUE,
                                CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE)));
        Map<Long, Map<Long, RelatedAction>> atomicActionsReverseRelations = ImmutableMap.of(
                NS_RESIZE_ID, ImmutableMap.of(WC_ATOMIC_RESIZE_ID,
                        mockBlockingRelatedAction(WC_OID, EntityType.NAMESPACE_VALUE,
                                CommodityDTO.CommodityType.VCPU_VALUE))
        );
        RelatedActionsInjector relatedActionsInjector =
                new RelatedActionsInjector(actionPlanId, Collections.emptyMap(), atomicActionsRelationsMap,
                        atomicActionsReverseRelations, liveActionStore, entitiesAndSettingsSnapshot);

        relatedActionsInjector.injectSymmetricRelatedActions();

        // Verify WorkloadController atomic resize action has related namespace resize action with BLOCKED_BY relation.
        assertNotNull(wcAtomicResize.getRelatedActions());
        assertEquals(1, wcAtomicResize.getRelatedActions().size());
        RelatedAction wcRelatedAction = wcAtomicResize.getRelatedActions().get(0);
        assertTrue(wcRelatedAction.hasActionEntity());
        assertEquals(EntityType.NAMESPACE_VALUE, wcRelatedAction.getActionEntity().getType());
        assertEquals(NS_OID, wcRelatedAction.getActionEntity().getId());
        assertTrue(wcRelatedAction.hasRecommendationId());
        assertTrue(wcRelatedAction.hasBlockedByRelation());
        assertTrue(wcRelatedAction.getBlockedByRelation().hasResize());
        assertEquals(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE, wcRelatedAction.getBlockedByRelation().getResize().getCommodityType().getType());

        // Verify namespace resize action has related WorkloadController resize action with BLOCKING relation.
        assertNotNull(nsResize.getRelatedActions());
        assertEquals(1, nsResize.getRelatedActions().size());
        RelatedAction nsRelatedAction = nsResize.getRelatedActions().get(0);
        assertTrue(nsRelatedAction.hasActionEntity());
        assertEquals(EntityType.WORKLOAD_CONTROLLER_VALUE, nsRelatedAction.getActionEntity().getType());
        assertEquals(WC_OID, nsRelatedAction.getActionEntity().getId());
        assertTrue(nsRelatedAction.hasRecommendationId());
        assertTrue(nsRelatedAction.hasBlockingRelation());
        assertTrue(nsRelatedAction.getBlockingRelation().hasResize());
        assertEquals(CommodityDTO.CommodityType.VCPU_VALUE, nsRelatedAction.getBlockingRelation().getResize().getCommodityType().getType());
    }

    /**
     * Test {@link RelatedActionsInjector#injectSymmetricRelatedActions()} for related atomic actions.
     */
    @Test
    public void testInjectSymmetricRelatedAtomicActionsWithMultipleBlockingActions() {
        Action wcAtomicResize = ActionOrchestratorTestUtils.createAtomicResizeAction(
                WC_ATOMIC_RESIZE_ID, WC_OID);
        Action nsResize1 = ActionOrchestratorTestUtils.createResizeAction(NS_RESIZE_ID1,
                    CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE, NS_OID, EntityType.NAMESPACE_VALUE);
        Action nsResize2 = ActionOrchestratorTestUtils.createResizeAction(NS_RESIZE_ID2,
                    CommodityDTO.CommodityType.VMEM_LIMIT_QUOTA_VALUE, NS_OID, EntityType.NAMESPACE_VALUE);

        Map<Long, Action> actionsMap = ImmutableMap.of(
                wcAtomicResize.getId(), wcAtomicResize,
                nsResize1.getId(), nsResize1,
                nsResize2.getId(), nsResize2);
        when(liveActionStore.getActions()).thenReturn(actionsMap);
        when(entitiesAndSettingsSnapshot.getEntityFromOid(WC_OID)).thenReturn(mockActionPartialEntityOpt(
                WC_OID, WC_NAME));
        when(entitiesAndSettingsSnapshot.getEntityFromOid(NS_OID)).thenReturn(mockActionPartialEntityOpt(
                NS_OID, NS_NAME));

        // MarketActionsRelationsMap has re-recommended action ID.
        Map<Long, Long> reRecommendedIdMap = ImmutableMap.of(
                RE_RECOMMENDED_WC_ATOMIC_RESIZE_ID, WC_ATOMIC_RESIZE_ID,
                RE_RECOMMENDED_NS_RESIZE_ID1, NS_RESIZE_ID1,
                RE_RECOMMENDED_NS_RESIZE_ID2, NS_RESIZE_ID2);
        when(liveActionStore.getReRecommendedIdMap()).thenReturn(reRecommendedIdMap);

        MarketRelatedAction blockingAction1 = mockBlockedByMarketRelatedAction(RE_RECOMMENDED_NS_RESIZE_ID1, NS_OID,
                EntityType.NAMESPACE_VALUE, CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE);
        MarketRelatedAction blockingAction2 = mockBlockedByMarketRelatedAction(RE_RECOMMENDED_NS_RESIZE_ID2, NS_OID,
                EntityType.NAMESPACE_VALUE, CommodityDTO.CommodityType.VMEM_LIMIT_QUOTA_VALUE);
        List<MarketRelatedAction> blockingActions = new ArrayList<>();
        blockingActions.add(blockingAction1);
        blockingActions.add(blockingAction2);

        Map<Long, List<MarketRelatedAction>> atomicActionsRelationsMap = ImmutableMap.of(
                RE_RECOMMENDED_WC_ATOMIC_RESIZE_ID, blockingActions);
        Map<Long, Map<Long, RelatedAction>> atomicActionsReverseRelations = ImmutableMap.of(
                RE_RECOMMENDED_NS_RESIZE_ID1, ImmutableMap.of(RE_RECOMMENDED_WC_ATOMIC_RESIZE_ID,
                        mockBlockingRelatedAction(WC_OID, EntityType.NAMESPACE_VALUE,
                                CommodityDTO.CommodityType.VCPU_VALUE)),
                RE_RECOMMENDED_NS_RESIZE_ID2, ImmutableMap.of(RE_RECOMMENDED_WC_ATOMIC_RESIZE_ID,
                        mockBlockingRelatedAction(WC_OID, EntityType.NAMESPACE_VALUE,
                                CommodityDTO.CommodityType.VMEM_VALUE))
        );

        RelatedActionsInjector relatedActionsInjector =
                new RelatedActionsInjector(actionPlanId, Collections.emptyMap(), atomicActionsRelationsMap,
                        atomicActionsReverseRelations, liveActionStore, entitiesAndSettingsSnapshot);

        relatedActionsInjector.injectSymmetricRelatedActions();

        // Verify WorkloadController atomic resize action has related namespace resize action with BLOCKED_BY relation.
        assertNotNull(wcAtomicResize.getRelatedActions());
        assertEquals(2, wcAtomicResize.getRelatedActions().size());
        RelatedAction wcRelatedAction = wcAtomicResize.getRelatedActions().get(0);
        assertTrue(wcRelatedAction.hasActionEntity());
        assertEquals(EntityType.NAMESPACE_VALUE, wcRelatedAction.getActionEntity().getType());
        assertEquals(NS_OID, wcRelatedAction.getActionEntity().getId());
        assertTrue(wcRelatedAction.hasRecommendationId());
        assertTrue(wcRelatedAction.hasBlockedByRelation());
        assertTrue(wcRelatedAction.getBlockedByRelation().hasResize());
        int commType = wcRelatedAction.getBlockedByRelation().getResize().getCommodityType().getType();
        assertTrue((commType == CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE)
                || (commType == CommodityDTO.CommodityType.VMEM_LIMIT_QUOTA_VALUE));

        // Verify namespace resize action has related WorkloadController resize action with BLOCKING relation.
        assertNotNull(nsResize1.getRelatedActions());
        assertEquals(1, nsResize1.getRelatedActions().size());
        RelatedAction nsRelatedAction1 = nsResize1.getRelatedActions().get(0);
        assertTrue(nsRelatedAction1.hasActionEntity());
        assertEquals(EntityType.WORKLOAD_CONTROLLER_VALUE, nsRelatedAction1.getActionEntity().getType());
        assertEquals(WC_OID, nsRelatedAction1.getActionEntity().getId());
        assertTrue(nsRelatedAction1.hasRecommendationId());
        assertTrue(nsRelatedAction1.hasBlockingRelation());
        assertTrue(nsRelatedAction1.getBlockingRelation().hasResize());
        assertTrue(nsRelatedAction1.getBlockingRelation().getResize().getCommodityType().getType()
                == CommodityDTO.CommodityType.VCPU_VALUE);

        assertNotNull(nsResize2.getRelatedActions());
        assertEquals(1, nsResize2.getRelatedActions().size());
        RelatedAction nsRelatedAction2 = nsResize2.getRelatedActions().get(0);
        assertTrue(nsRelatedAction2.hasActionEntity());
        assertEquals(EntityType.WORKLOAD_CONTROLLER_VALUE, nsRelatedAction2.getActionEntity().getType());
        assertEquals(WC_OID, nsRelatedAction2.getActionEntity().getId());
        assertTrue(nsRelatedAction2.hasRecommendationId());
        assertTrue(nsRelatedAction2.hasBlockingRelation());
        assertTrue(nsRelatedAction2.getBlockingRelation().hasResize());
        assertTrue(nsRelatedAction2.getBlockingRelation().getResize().getCommodityType().getType()
                == CommodityDTO.CommodityType.VMEM_VALUE);
    }

    /**
     * Test {@link RelatedActionsInjector#injectSymmetricRelatedActions()} for related atomic actions.
     */
    @Test
    public void testInjectSymmetricRelatedAtomicActionsWithMissingReRecommendedAction() {
        Action wcAtomicResize = ActionOrchestratorTestUtils.createAtomicResizeAction(
                WC_ATOMIC_RESIZE_ID, WC_OID);
        Action nsResize1 = ActionOrchestratorTestUtils.createResizeAction(NS_RESIZE_ID1, NS_OID, EntityType.NAMESPACE_VALUE);
        Action nsResize2 = ActionOrchestratorTestUtils.createResizeAction(NS_RESIZE_ID2, NS_OID, EntityType.NAMESPACE_VALUE);

        Map<Long, Action> actionsMap = ImmutableMap.of(
                wcAtomicResize.getId(), wcAtomicResize,
                nsResize1.getId(), nsResize1,
                nsResize2.getId(), nsResize2);
        when(liveActionStore.getActions()).thenReturn(actionsMap);
        when(entitiesAndSettingsSnapshot.getEntityFromOid(WC_OID))
                .thenReturn(mockActionPartialEntityOpt(WC_OID, WC_NAME));
        when(entitiesAndSettingsSnapshot.getEntityFromOid(NS_OID))
                .thenReturn(mockActionPartialEntityOpt(NS_OID, NS_NAME));

        // MarketActionsRelationsMap has re-recommended action ID.
        Map<Long, Long> reRecommendedIdMap = ImmutableMap.of(
                RE_RECOMMENDED_WC_ATOMIC_RESIZE_ID, WC_ATOMIC_RESIZE_ID,
                //RE_RECOMMENDED_NS_RESIZE_ID1, NS_RESIZE_ID1,
                RE_RECOMMENDED_NS_RESIZE_ID2, NS_RESIZE_ID2);
        when(liveActionStore.getReRecommendedIdMap()).thenReturn(reRecommendedIdMap);

        MarketRelatedAction blockingAction1 = mockBlockedByMarketRelatedAction(RE_RECOMMENDED_NS_RESIZE_ID1, NS_OID,
                EntityType.NAMESPACE_VALUE, CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE);
        MarketRelatedAction blockingAction2 = mockBlockedByMarketRelatedAction(RE_RECOMMENDED_NS_RESIZE_ID2, NS_OID,
                EntityType.NAMESPACE_VALUE, CommodityDTO.CommodityType.VMEM_LIMIT_QUOTA_VALUE);
        List<MarketRelatedAction> blockingActions = new ArrayList<>();
        blockingActions.add(blockingAction1);
        blockingActions.add(blockingAction2);

        Map<Long, List<MarketRelatedAction>> atomicActionsRelationsMap = ImmutableMap.of(
                RE_RECOMMENDED_WC_ATOMIC_RESIZE_ID, blockingActions);
        Map<Long, Map<Long, RelatedAction>> atomicActionsReverseRelations = ImmutableMap.of(
                RE_RECOMMENDED_NS_RESIZE_ID1, ImmutableMap.of(RE_RECOMMENDED_WC_ATOMIC_RESIZE_ID,
                        mockBlockingRelatedAction(WC_OID, EntityType.NAMESPACE_VALUE,
                                CommodityDTO.CommodityType.VCPU_VALUE)),
                RE_RECOMMENDED_NS_RESIZE_ID2, ImmutableMap.of(RE_RECOMMENDED_WC_ATOMIC_RESIZE_ID,
                        mockBlockingRelatedAction(WC_OID, EntityType.NAMESPACE_VALUE,
                                CommodityDTO.CommodityType.VCPU_VALUE))
        );

        RelatedActionsInjector relatedActionsInjector =
                new RelatedActionsInjector(actionPlanId, Collections.emptyMap(), atomicActionsRelationsMap,
                        atomicActionsReverseRelations, liveActionStore, entitiesAndSettingsSnapshot);

        relatedActionsInjector.injectSymmetricRelatedActions();

        // Verify WorkloadController atomic resize action has related namespace resize action with BLOCKED_BY relation.
        assertNotNull(wcAtomicResize.getRelatedActions());
        assertEquals(1, wcAtomicResize.getRelatedActions().size());
        RelatedAction wcRelatedAction = wcAtomicResize.getRelatedActions().get(0);
        assertTrue(wcRelatedAction.hasActionEntity());
        assertEquals(EntityType.NAMESPACE_VALUE, wcRelatedAction.getActionEntity().getType());
        assertEquals(NS_OID, wcRelatedAction.getActionEntity().getId());
        assertTrue(wcRelatedAction.hasRecommendationId());
        assertTrue(wcRelatedAction.hasBlockedByRelation());
        assertTrue(wcRelatedAction.getBlockedByRelation().hasResize());

        // Verify namespace resize action has related WorkloadController resize action with BLOCKING relation.
        assertTrue(nsResize1.getRelatedActions().isEmpty());

        assertNotNull(nsResize2.getRelatedActions());
        assertEquals(1, nsResize2.getRelatedActions().size());
        RelatedAction nsRelatedAction2 = nsResize2.getRelatedActions().get(0);
        assertTrue(nsRelatedAction2.hasActionEntity());
        assertEquals(EntityType.WORKLOAD_CONTROLLER_VALUE, nsRelatedAction2.getActionEntity().getType());
        assertEquals(WC_OID, nsRelatedAction2.getActionEntity().getId());
        assertTrue(nsRelatedAction2.hasRecommendationId());
        assertTrue(nsRelatedAction2.hasBlockingRelation());
        assertTrue(nsRelatedAction2.getBlockingRelation().hasResize());
    }

    private Optional<ActionPartialEntity> mockActionPartialEntityOpt(final long entityOID, final String entityName) {
        return Optional.of(ActionPartialEntity.newBuilder()
                .setOid(entityOID)
                .setDisplayName(entityName)
                .build());
    }

    private MarketRelatedAction mockCausedByMarketRelatedAction(final long actionID, final long targetEntityID,
                                                                final int entityType) {
        return MarketRelatedAction.newBuilder()
                .setActionId(actionID)
                .setActionEntity(ActionEntity.newBuilder()
                        .setId(targetEntityID)
                        .setType(entityType)
                        .build())
                .setCausedByRelation(CausedByRelation.newBuilder()
                        .setProvision(CausedByProvision.newBuilder()))
                .build();
    }

    private MarketRelatedAction mockBlockedByMarketRelatedAction(final long actionID, final long targetEntityID,
                                                                 final int entityType, final int commodityType) {
        return MarketRelatedAction.newBuilder()
                .setActionId(actionID)
                .setActionEntity(ActionEntity.newBuilder()
                        .setId(targetEntityID)
                        .setType(entityType)
                        .build())
                .setBlockedByRelation(BlockedByRelation.newBuilder()
                        .setResize(BlockedByResize.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(commodityType))))
                .build();
    }

    private RelatedAction mockBlockingRelatedAction(final long targetEntityID, final int entityType,
                                                    final int commodityType) {
        return RelatedAction.newBuilder()
                .setRecommendationId(IdentityGenerator.next())
                .setActionEntity(ActionEntity.newBuilder()
                        .setId(targetEntityID)
                        .setType(entityType)
                        .build())
                .setBlockingRelation(BlockingRelation.newBuilder()
                        .setResize(BlockingResize.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(commodityType))))
                .build();
    }
}