package com.vmturbo.market.topology.conversions.action;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.MarketRelatedActionsList;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.MarketRelatedAction;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.utils.GuestLoadFilters;
import com.vmturbo.market.runner.postprocessor.NamespaceQuotaAnalysisResult;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO.BlockedByRelation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO.BlockedByRelation.BlockedByResize;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO.CausedByRelation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO.CausedByRelation.CausedBySuspension;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 * RelatedActionInterpreter test.
 */
public class RelatedActionInterpreterTest {

    private static final long POD_OID = 11;
    private static final long VM_OID = 22;
    private static final long GUEST_LOAD_OID = 33;
    private static final long CONTAINER_OID = 44;
    private static final long NAMESPACE_OID = 55;
    private static final long POD_SUSPENSION_ACTION_ID = 111;
    private static final long VM_SUSPENSION_ACTION_ID = 222;
    private static final long GUEST_LOAD_APP_SUSPENSION_ACTION_ID = 333;
    private static final long CONTAINER_RESIZE_ACTION_ID = 444;
    private static final long NAMESPACE_RESIZE_ACTION_ID = 555;

    private static final ProjectedTopologyEntity guestLoadApp = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                    .setOid(GUEST_LOAD_OID)
                    .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                    .setEnvironmentType(EnvironmentType.ON_PREM)
                    .putEntityPropertyMap(GuestLoadFilters.APPLICATION_TYPE_PATH, SupplyChainConstants.GUEST_LOAD))
            .build();
    private static final ProjectedTopologyEntity pod = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                    .setOid(POD_OID)
                    .setEntityType(EntityType.CONTAINER_POD_VALUE)
                    .setEnvironmentType(EnvironmentType.ON_PREM))
            .build();
    private static final ProjectedTopologyEntity vm = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                    .setOid(VM_OID)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setEnvironmentType(EnvironmentType.ON_PREM))
            .build();
    private static final ProjectedTopologyEntity container = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                    .setOid(CONTAINER_OID)
                    .setEntityType(EntityType.CONTAINER_VALUE)
                    .setEnvironmentType(EnvironmentType.ON_PREM))
            .build();
    private static final ProjectedTopologyEntity namespace = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.NAMESPACE_VALUE)
                    .setOid(NAMESPACE_OID)
                    .setEnvironmentType(EnvironmentType.ON_PREM))
            .build();

    private static final ActionTO guestLoadAppSuspendActionTO = ActionTO.newBuilder()
            .setId(GUEST_LOAD_APP_SUSPENSION_ACTION_ID)
            .setDeactivate(DeactivateTO.newBuilder().setTraderToDeactivate(GUEST_LOAD_OID))
            .setImportance(-1)
            .addRelatedActions(RelatedActionTO.newBuilder()
                    .setRelatedActionId(VM_SUSPENSION_ACTION_ID)
                    .setTargetTrader(VM_OID)
                    .setCausedByRelation(CausedByRelation.newBuilder()
                            .setSuspension(CausedBySuspension.newBuilder())))
            .setIsNotExecutable(true)
            .build();
    private static final ActionTO podSuspendActionTO = ActionTO.newBuilder()
            .setId(POD_SUSPENSION_ACTION_ID)
            .setDeactivate(DeactivateTO.newBuilder().setTraderToDeactivate(POD_OID))
            .setImportance(-1)
            .addRelatedActions(RelatedActionTO.newBuilder()
                    .setRelatedActionId(VM_SUSPENSION_ACTION_ID)
                    .setTargetTrader(VM_OID)
                    .setCausedByRelation(CausedByRelation.newBuilder()
                            .setSuspension(CausedBySuspension.newBuilder())))
            .setIsNotExecutable(true)
            .build();
    private static final ActionTO vmSuspendActionTO = ActionTO.newBuilder()
            .setId(VM_SUSPENSION_ACTION_ID)
            .setDeactivate(DeactivateTO.newBuilder().setTraderToDeactivate(VM_OID))
            .setImportance(-1)
            .setIsNotExecutable(true)
            .build();
    private static final ActionTO containerResizeActionTO = ActionTO.newBuilder()
            .setId(CONTAINER_RESIZE_ACTION_ID)
            .setResize(ResizeTO.newBuilder().setSellingTrader(CONTAINER_OID))
            .setImportance(-1)
            .addRelatedActions(RelatedActionTO.newBuilder()
                    .setTargetTrader(NAMESPACE_OID)
                    .setBlockedByRelation(BlockedByRelation.newBuilder()
                            .setResize(BlockedByResize.newBuilder()
                                    .setCommodityType(CommoditySpecificationTO.newBuilder()
                                            .setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE)
                                            .setBaseType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE)))))
            .setIsNotExecutable(true)
            .build();

    private static final Action guestLoadAppSuspendAction = Action.newBuilder()
            .setId(GUEST_LOAD_APP_SUSPENSION_ACTION_ID)
            .setInfo(ActionInfo.newBuilder()
                    .setDeactivate(Deactivate.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(GUEST_LOAD_OID)
                                    .setType(EntityType.APPLICATION_COMPONENT_VALUE)
                                    .setEnvironmentType(EnvironmentType.ON_PREM))))
            .setDeprecatedImportance(-1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();
    private static final Action podSuspendAction = Action.newBuilder()
            .setId(POD_SUSPENSION_ACTION_ID)
            .setInfo(ActionInfo.newBuilder()
                    .setDeactivate(Deactivate.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(POD_OID)
                                    .setType(EntityType.CONTAINER_POD_VALUE)
                                    .setEnvironmentType(EnvironmentType.ON_PREM))))
            .setDeprecatedImportance(-1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();
    private static final Action vmSuspendAction = Action.newBuilder()
            .setId(VM_SUSPENSION_ACTION_ID)
            .setInfo(ActionInfo.newBuilder()
                    .setDeactivate(Deactivate.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(VM_OID)
                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                    .setEnvironmentType(EnvironmentType.ON_PREM))))
            .setDeprecatedImportance(-1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();
    private static final Action containerResizeAction = Action.newBuilder()
            .setId(CONTAINER_RESIZE_ACTION_ID)
            .setInfo(ActionInfo.newBuilder()
                    .setResize(Resize.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(CONTAINER_OID)
                                    .setType(EntityType.CONTAINER_VALUE)
                                    .setEnvironmentType(EnvironmentType.ON_PREM))))
            .setDeprecatedImportance(-1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();
    private static final Action namespaceResizeAction = Action.newBuilder()
            .setId(NAMESPACE_RESIZE_ACTION_ID)
            .setInfo(ActionInfo.newBuilder()
                    .setResize(Resize.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setId(NAMESPACE_OID)
                                    .setType(EntityType.NAMESPACE_VALUE)
                                    .setEnvironmentType(EnvironmentType.ON_PREM))
                            .setCommodityType(CommodityType.newBuilder()
                                    .setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))))
            .setDeprecatedImportance(-1)
            .setExplanation(Explanation.getDefaultInstance())
            .build();

    private static final Map<Long, ProjectedTopologyEntity> projectedTopologyEntityMap =
            ImmutableMap.of(GUEST_LOAD_OID, guestLoadApp, POD_OID, pod, VM_OID, vm,
                    NAMESPACE_OID, namespace, CONTAINER_OID, container);

    /**
     * Test interpret related actions.
     */
    @Test
    public void interpretRelatedActions() {
        final ImmutableList<ActionTO> actionTOs =
                ImmutableList.of(podSuspendActionTO, guestLoadAppSuspendActionTO, vmSuspendActionTO);
        final ImmutableList<Action> actions =
                ImmutableList.of(guestLoadAppSuspendAction, podSuspendAction, vmSuspendAction);

        final RelatedActionInterpreter relatedActionInterpreter = new RelatedActionInterpreter(
                TopologyInfo.newBuilder().build());
        Map<Long, MarketRelatedActionsList> relatedActionsListMap =
                relatedActionInterpreter.interpret(
                        actionTOs, actions, projectedTopologyEntityMap, null);

        final MarketRelatedAction expectedCausedByRelatedSuspendAction = MarketRelatedAction.newBuilder()
                .setActionId(VM_SUSPENSION_ACTION_ID)
                .setActionEntity(ActionEntity.newBuilder()
                        .setId(VM_OID)
                        .setType(vm.getEntity().getEntityType())
                        .setEnvironmentType(vm.getEntity().getEnvironmentType()))
                .setCausedByRelation(ActionDTO.CausedByRelation.newBuilder()
                        .setSuspension(ActionDTO.CausedByRelation.CausedBySuspension.newBuilder()))
                .build();

        // GuestLoad App action is skipped, so only pod suspension action has related action which is
        // vm suspension.
        assertEquals(1, relatedActionsListMap.size());
        MarketRelatedActionsList relatedVMSuspensions = relatedActionsListMap.get(POD_SUSPENSION_ACTION_ID);
        assertNotNull(relatedVMSuspensions);
        assertEquals(1, relatedVMSuspensions.getRelatedActionsCount());
        assertEquals(expectedCausedByRelatedSuspendAction, relatedVMSuspensions.getRelatedActions(0));
    }

    /**
     * Test interpret related namespace action on container resizing action in real time.
     */
    @Test
    public void testInterpretContainerNSRelatedActionInRealTime() {
        final ImmutableList<ActionTO> actionTOs = ImmutableList.of(containerResizeActionTO);
        final ImmutableList<Action> actions = ImmutableList.of(containerResizeAction, namespaceResizeAction);

        final NamespaceQuotaAnalysisResult namespaceQuotaAnalysisResult = new NamespaceQuotaAnalysisResult();
        namespaceQuotaAnalysisResult.addNamespaceResizeAction(namespaceResizeAction);

        final RelatedActionInterpreter relatedActionInterpreter = new RelatedActionInterpreter(
                TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME).build());
        Map<Long, MarketRelatedActionsList> relatedActionsListMap =
                relatedActionInterpreter.interpret(
                        actionTOs, actions, projectedTopologyEntityMap, namespaceQuotaAnalysisResult);

        final MarketRelatedAction expectedBlockedByNSResizeAction = MarketRelatedAction.newBuilder()
                .setActionId(NAMESPACE_RESIZE_ACTION_ID)
                .setActionEntity(ActionEntity.newBuilder()
                        .setId(NAMESPACE_OID)
                        .setType(namespace.getEntity().getEntityType())
                        .setEnvironmentType(namespace.getEntity().getEnvironmentType()))
                .setBlockedByRelation(ActionDTO.BlockedByRelation.newBuilder()
                        .setResize(ActionDTO.BlockedByRelation.BlockedByResize.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE))))
                .build();

        assertEquals(1, relatedActionsListMap.size());
        MarketRelatedActionsList relatedNSResizes = relatedActionsListMap.get(CONTAINER_RESIZE_ACTION_ID);
        assertNotNull(relatedNSResizes);
        assertEquals(1, relatedNSResizes.getRelatedActionsCount());
        assertEquals(expectedBlockedByNSResizeAction, relatedNSResizes.getRelatedActions(0));
    }

    /**
     * Test interpret related namespace action on container resizing action in plan.
     */
    @Test
    public void testInterpretContainerNSRelatedActionInPlan() {
        final ImmutableList<ActionTO> actionTOs = ImmutableList.of(containerResizeActionTO);
        final ImmutableList<Action> actions = ImmutableList.of(containerResizeAction, namespaceResizeAction);

        final NamespaceQuotaAnalysisResult namespaceQuotaAnalysisResult = new NamespaceQuotaAnalysisResult();
        namespaceQuotaAnalysisResult.addNamespaceResizeAction(namespaceResizeAction);

        final RelatedActionInterpreter relatedActionInterpreter = new RelatedActionInterpreter(
                TopologyInfo.newBuilder().setTopologyType(TopologyType.PLAN).build());
        Map<Long, MarketRelatedActionsList> relatedActionsListMap =
                relatedActionInterpreter.interpret(
                        actionTOs, actions, projectedTopologyEntityMap, namespaceQuotaAnalysisResult);

        // No blockedBy namespace related actions set on container resize actions in plan.
        assertEquals(0, relatedActionsListMap.size());
    }
}