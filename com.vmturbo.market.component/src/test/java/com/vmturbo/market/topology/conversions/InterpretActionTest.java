package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;

/**
 * Test various actions.
 */
public class InterpretActionTest {

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
    public void testExecutableFlag() {
        final TopologyConverter converter = new TopologyConverter(true, TopologyType.REALTIME);

        final ActionTO executableActionTO = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                // Using ProvisionBySupply because it requires the least setup, and all
                // we really care about is the executable flag.
                .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(1234)
                        .build())
                .build();

        final ActionTO notExecutableActionTO = ActionTO.newBuilder(executableActionTO)
                .setIsNotExecutable(true)
                .build();

        assertTrue(converter.interpretAction(executableActionTO).get().getExecutable());
        assertFalse(converter.interpretAction(notExecutableActionTO).get().getExecutable());
    }

    @Test
    public void testInterpretMoveAction() throws IOException, InvalidTopologyException {
        TopologyConverter converter = new TopologyConverter(true, TopologyType.REALTIME);
        TopologyDTO.TopologyEntityDTO entityDTO =
                TopologyConverterTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");

        Set<TraderTO> traderTOs = converter.convertToMarket(Lists.newArrayList(entityDTO));
        final TraderTO vmTraderTO = TopologyConverterTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                    .setImportance(0.)
                    .setIsNotExecutable(false)
                    .setMove(MoveTO.newBuilder()
                        .setShoppingListToMove(shoppingList.getOid())
                        .setSource(1234)
                        .setDestination(5678)
                            // set the moveExplanation only for
                            // compilation purpose
                        .setMoveExplanation(MoveExplanation
                            .newBuilder().build())
                        .build())
                    .build()).get().getInfo();

        assertEquals(ActionTypeCase.MOVE, actionInfo.getActionTypeCase());
        assertEquals(1234, actionInfo.getMove().getSourceId());
        assertEquals(5678, actionInfo.getMove().getDestinationId());
    }

    @Test
    public void testInterpretReconfigureAction() throws IOException, InvalidTopologyException {
        TopologyConverter converter = new TopologyConverter(true, TopologyType.REALTIME);
        TopologyDTO.TopologyEntityDTO topologyDTO =
                        TopologyConverterTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");

        Set<TraderTO> traderTOs = converter.convertToMarket(Lists.newArrayList(topologyDTO));
        final TraderTO vmTraderTO = TopologyConverterTest.getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setReconfigure(ReconfigureTO.newBuilder()
                    .setShoppingListToReconfigure(shoppingList.getOid())
                    .setSource(1234)
                    .build())
                .build()).get().getInfo();

        assertEquals(ActionTypeCase.RECONFIGURE, actionInfo.getActionTypeCase());
        assertEquals(1234, actionInfo.getReconfigure().getSourceId());
    }

    @Test
    public void testInterpretProvisionBySupplyAction() throws Exception {
        TopologyConverter converter = new TopologyConverter(true, TopologyType.REALTIME);

        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                    .setImportance(0.)
                    .setIsNotExecutable(false)
                    .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(1234)
                        .build())
                    .build()).get().getInfo();

        assertEquals(ActionTypeCase.PROVISION, actionInfo.getActionTypeCase());
        assertEquals(-1, actionInfo.getProvision().getProvisionedSeller());
        assertEquals(1234, actionInfo.getProvision().getEntityToCloneId());
    }

    @Test
    public void testInterpretResizeAction() throws Exception {
        final TopologyConverter converter = Mockito.spy(new TopologyConverter(true, TopologyType.REALTIME));
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity1));

        final long entityToResize = 1;
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
        final ActionInfo actionInfo = converter.interpretAction(resizeAction).get().getInfo();

        assertEquals(ActionTypeCase.RESIZE, actionInfo.getActionTypeCase());
        assertEquals(entityToResize, actionInfo.getResize().getTargetId());
        assertEquals(topologyCommodity1, actionInfo.getResize().getCommodityType());
        assertEquals(oldCapacity, actionInfo.getResize().getOldCapacity(), 0);
        assertEquals(newCapacity, actionInfo.getResize().getNewCapacity(), 0);
    }

    @Test
    public void testInterpretActivateAction() throws Exception {
        final TopologyConverter converter = Mockito.spy(new TopologyConverter(true, TopologyType.REALTIME));
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity2));

        final long entityToActivate = 1;
        final ActionTO activateAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setActivate(ActivateTO.newBuilder()
                    .setTraderToActivate(entityToActivate)
                    .setModelSeller(2)
                    .setMostExpensiveCommodity(economyCommodity1.getBaseType())
                    .addTriggeringBasket(economyCommodity1)
                    .addTriggeringBasket(economyCommodity2))
                .build();
        final ActionInfo actionInfo = converter.interpretAction(activateAction).get().getInfo();

        assertEquals(ActionTypeCase.ACTIVATE, actionInfo.getActionTypeCase());
        assertEquals(entityToActivate, actionInfo.getActivate().getTargetId());
        assertThat(actionInfo.getActivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
    }

    @Test
    public void testInterpretDeactivateAction() throws Exception {
        final TopologyConverter converter = Mockito.spy(new TopologyConverter(true, TopologyType.REALTIME));
        Mockito.doReturn(Optional.of(topologyCommodity1))
                .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity2));

        final long entityToDeactivate = 1;
        final ActionTO deactivateAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setDeactivate(DeactivateTO.newBuilder()
                        .setTraderToDeactivate(entityToDeactivate)
                        .addTriggeringBasket(economyCommodity1)
                        .addTriggeringBasket(economyCommodity2)
                        .build())
                .build();
        final ActionInfo actionInfo = converter.interpretAction(deactivateAction).get().getInfo();

        assertEquals(ActionTypeCase.DEACTIVATE, actionInfo.getActionTypeCase());
        assertEquals(entityToDeactivate, actionInfo.getDeactivate().getTargetId());
        assertThat(actionInfo.getDeactivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
    }


}
