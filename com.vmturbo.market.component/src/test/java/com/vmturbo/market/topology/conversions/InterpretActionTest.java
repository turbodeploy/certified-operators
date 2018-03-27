package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.InitialPlacement;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Test various actions.
 */
public class InterpretActionTest {

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =  TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .build();

    private static final Map<Long, Integer> entityIdToEntityTypeMap =
        Collections.emptyMap();

    private CommodityDTOs.CommoditySpecificationTO economyCommodity1;
    private CommodityType topologyCommodity1;
    private CommodityDTOs.CommoditySpecificationTO economyCommodity2;
    private CommodityType topologyCommodity2;

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        // The commodity types in topologyCommodity
        // map to the base type in economy commodity.
        topologyCommodity1 = CommodityType.newBuilder()
                        .setType(1)
                        .setKey("blah")
                        .build();
        topologyCommodity2 = CommodityType.newBuilder()
                        .setType(2)
                        .setKey("blahblah")
                        .build();
        economyCommodity1 = CommodityDTOs.CommoditySpecificationTO.newBuilder()
                        .setType(0)
                        .setBaseType(1)
                        .build();
        economyCommodity2 = CommodityDTOs.CommoditySpecificationTO.newBuilder()
                        .setType(1)
                        .setBaseType(2)
                        .build();
    }

    @Test
    public void testCommodityIdsAreInvertible() {
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true);

        final CommodityType segmentationFoo = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .setKey("foo")
            .build();
        final CommodityType segmentationBar = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .setKey("bar")
            .build();
        final CommodityType cpu = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .build();
        final CommodityType cpuRed = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
            .setKey("red")
            .build();

        int segmentationFooId = converter.toMarketCommodityId(segmentationFoo);
        int segmentationBarId = converter.toMarketCommodityId(segmentationBar);
        int cpuId = converter.toMarketCommodityId(cpu);
        int cpuRedId = converter.toMarketCommodityId(cpuRed);

        assertEquals(segmentationFoo, converter.commodityIdToCommodityType(segmentationFooId));
        assertEquals(segmentationBar, converter.commodityIdToCommodityType(segmentationBarId));
        assertEquals(cpu, converter.commodityIdToCommodityType(cpuId));
        assertEquals(cpuRed, converter.commodityIdToCommodityType(cpuRedId));
    }

    @Test
    public void testExecutableFlag() {
        long modelSeller = 1234;
        int modelType = 1;
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(modelSeller, modelType);
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true);

        final ActionTO executableActionTO = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                // Using ProvisionBySupply because it requires the least setup, and all
                // we really care about is the executable flag.
                .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(modelSeller)
                        .build())
                .build();

        final ActionTO notExecutableActionTO = ActionTO.newBuilder(executableActionTO)
                .setIsNotExecutable(true)
                .build();

        assertTrue(converter.interpretAction(executableActionTO, entityIdToTypeMap).get().getExecutable());
        assertFalse(converter.interpretAction(notExecutableActionTO, entityIdToTypeMap).get().getExecutable());
    }

    @Test
    public void testInterpretMoveAction() throws IOException, InvalidTopologyException {
        long srcId = 1234;
        long destId = 5678;
        int srcType = 0;
        int destType = 1;
        TopologyDTO.TopologyEntityDTO entityDTO =
                TopologyConverterTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");

        Map<Long, Integer> entityIdTypeMap =
            ImmutableMap.of(srcId, srcType,
                            destId, destType,
                            entityDTO.getOid(), entityDTO.getEntityType());

        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true);

        Set<TraderTO> traderTOs = converter.convertToMarket(Lists.newArrayList(entityDTO));
        final TraderTO vmTraderTO = TopologyConverterTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setCompoundMove(CompoundMoveTO.newBuilder()
                    .addMoves(MoveTO.newBuilder()
                        .setShoppingListToMove(shoppingList.getOid())
                        .setSource(srcId)
                        .setDestination(destId)
                        .setMoveExplanation(MoveExplanation.getDefaultInstance())))
                .build(), entityIdTypeMap).get().getInfo();
        ActionInfo actionInfoWithOutSource = converter.interpretAction(
                ActionTO.newBuilder()
                        .setImportance(0.1)
                        .setIsNotExecutable(false)
                        .setCompoundMove(CompoundMoveTO.newBuilder()
                                .addMoves(MoveTO.newBuilder()
                                        .setShoppingListToMove(shoppingList.getOid())
                                        .setDestination(destId)
                                        .setMoveExplanation(MoveExplanation.newBuilder()
                                                .setInitialPlacement(InitialPlacement
                                                        .getDefaultInstance()))))
                        .build(), entityIdTypeMap)
                .get().getInfo();

        assertEquals(ActionTypeCase.MOVE, actionInfo.getActionTypeCase());
        assertEquals(1, actionInfo.getMove().getChangesList().size());
        assertEquals(srcId, actionInfo.getMove().getChanges(0).getSource().getId());
        assertEquals(srcType, actionInfo.getMove().getChanges(0).getSource().getType());
        assertEquals(destId, actionInfo.getMove().getChanges(0).getDestination().getId());
        assertEquals(destType, actionInfo.getMove().getChanges(0).getDestination().getType());

        assertEquals(ActionTypeCase.MOVE, actionInfoWithOutSource.getActionTypeCase());
        assertEquals(1, actionInfoWithOutSource.getMove().getChangesList().size());
        assertFalse(actionInfoWithOutSource.getMove().getChanges(0).hasSource());
        assertEquals(destId, actionInfo.getMove().getChanges(0).getDestination().getId());
        assertEquals(destType, actionInfo.getMove().getChanges(0).getDestination().getType());
    }

    @Test
    public void testInterpretReconfigureAction() throws IOException, InvalidTopologyException {
        long reconfigureSourceId = 1234;
        int reconfigureSourceType = 1;

        TopologyDTO.TopologyEntityDTO topologyDTO =
                        TopologyConverterTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(
                reconfigureSourceId, reconfigureSourceType,
                topologyDTO.getOid(), topologyDTO.getEntityType());
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true);

        Set<TraderTO> traderTOs = converter.convertToMarket(Lists.newArrayList(topologyDTO));
        final TraderTO vmTraderTO = TopologyConverterTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setReconfigure(ReconfigureTO.newBuilder()
                    .setShoppingListToReconfigure(shoppingList.getOid())
                    .setSource(reconfigureSourceId))
                .build(), entityIdToTypeMap).get().getInfo();

        assertEquals(ActionTypeCase.RECONFIGURE, actionInfo.getActionTypeCase());
        assertEquals(reconfigureSourceId, actionInfo.getReconfigure().getSource().getId());
    }

    @Test
    public void testInterpretReconfigureActionWithoutSource() throws IOException, InvalidTopologyException {
        TopologyDTO.TopologyEntityDTO topologyDTO =
                        TopologyConverterTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(
                topologyDTO.getOid(), topologyDTO.getEntityType());
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true);

        Set<TraderTO> traderTOs = converter.convertToMarket(Lists.newArrayList(topologyDTO));
        final TraderTO vmTraderTO = TopologyConverterTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setReconfigure(ReconfigureTO.newBuilder()
                    .setShoppingListToReconfigure(shoppingList.getOid()))
                .build(), entityIdToTypeMap).get().getInfo();

        assertEquals(ActionTypeCase.RECONFIGURE, actionInfo.getActionTypeCase());
        assertFalse(actionInfo.getReconfigure().hasSource());
    }

    @Test
    public void testInterpretProvisionBySupplyAction() throws Exception {
        long modelSeller = 1234;
        int modelType = 1;
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(modelSeller, modelType);
        final TopologyConverter converter =
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true);

        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                    .setImportance(0.)
                    .setIsNotExecutable(false)
                    .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(modelSeller))
                    .build(), entityIdToTypeMap).get().getInfo();

        assertEquals(ActionTypeCase.PROVISION, actionInfo.getActionTypeCase());
        assertEquals(-1, actionInfo.getProvision().getProvisionedSeller());
        assertEquals(modelSeller, actionInfo.getProvision().getEntityToClone().getId());
    }

    @Test
    public void testInterpretResizeAction() throws Exception {
        long entityToResize = 1;
        int  entityType = 1;
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(entityToResize, entityType);
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true));
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity1));

        final long oldCapacity = 10;
        final long newCapacity = 9;
        final ActionTO resizeAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setResize(ResizeTO.newBuilder()
                    .setSellingTrader(entityToResize)
                    .setNewCapacity(newCapacity)
                    .setOldCapacity(oldCapacity)
                    .setSpecification(economyCommodity1))
                .build();
        final ActionInfo actionInfo =
                converter.interpretAction(resizeAction, entityIdToTypeMap).get().getInfo();

        assertEquals(ActionTypeCase.RESIZE, actionInfo.getActionTypeCase());
        assertEquals(entityToResize, actionInfo.getResize().getTarget().getId());
        assertEquals(topologyCommodity1, actionInfo.getResize().getCommodityType());
        assertEquals(oldCapacity, actionInfo.getResize().getOldCapacity(), 0);
        assertEquals(newCapacity, actionInfo.getResize().getNewCapacity(), 0);
    }

    @Test
    public void testInterpretActivateAction() throws Exception {
        long entityToActivate = 1;
        int  entityType = 1;
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(entityToActivate, entityType);
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true));
        // Insert the commodity type into the converter's mapping
        final CommodityType expectedCommodityType = CommodityType.newBuilder()
            .setType(12)
            .setKey("Foo")
            .build();

        final int marketCommodityId = converter.toMarketCommodityId(expectedCommodityType);
        final  CommodityDTOs.CommoditySpecificationTO economyCommodity =
            CommodityDTOs.CommoditySpecificationTO.newBuilder()
                .setType(marketCommodityId)
                .setBaseType(expectedCommodityType.getType())
                .build();

        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity2));

        final ActionTO activateAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setActivate(ActivateTO.newBuilder()
                    .setTraderToActivate(entityToActivate)
                    .setModelSeller(2)
                    .setMostExpensiveCommodity(economyCommodity.getBaseType())
                    .addTriggeringBasket(economyCommodity)
                    .addTriggeringBasket(economyCommodity2))
                .build();
        final Action action = converter.interpretAction(activateAction, entityIdToTypeMap).get();
        final ActionInfo actionInfo = action.getInfo();

        assertEquals(ActionTypeCase.ACTIVATE, actionInfo.getActionTypeCase());
        assertEquals(entityToActivate, actionInfo.getActivate().getTarget().getId());
        assertThat(actionInfo.getActivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
        assertEquals(expectedCommodityType.getType(),
            action.getExplanation().getActivate().getMostExpensiveCommodity());
    }

    @Test
    public void testInterpretDeactivateAction() throws Exception {
        long entityToDeactivate = 1;
        int  entityType = 1;
        final Map<Long, Integer> entityIdToTypeMap =
            ImmutableMap.of(entityToDeactivate, entityType);
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true));
        Mockito.doReturn(Optional.of(topologyCommodity1))
                .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity2));

        final ActionTO deactivateAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setDeactivate(DeactivateTO.newBuilder()
                        .setTraderToDeactivate(entityToDeactivate)
                        .addTriggeringBasket(economyCommodity1)
                        .addTriggeringBasket(economyCommodity2)
                        .build())
                .build();
        final ActionInfo actionInfo =
                converter.interpretAction(deactivateAction, entityIdToTypeMap).get().getInfo();

        assertEquals(ActionTypeCase.DEACTIVATE, actionInfo.getActionTypeCase());
        assertEquals(entityToDeactivate, actionInfo.getDeactivate().getTarget().getId());
        assertThat(actionInfo.getDeactivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
    }
}
