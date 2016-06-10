package com.vmturbo.platform.analysis.translators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.function.ToLongFunction;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.topology.Topology;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link AnalysisToProtobuf} class.
 */
@RunWith(JUnitParamsRunner.class)
public class AnalysisToProtobufTest {
    // Fields
    private static final Economy e = new Economy();
    private static final CommoditySpecification CPU = new CommoditySpecification(100);
    private static final Basket empty = new Basket();
    private static final Basket basketBought = new Basket(CPU);

    private static final Trader vm1 = e.addTrader(0, TraderState.INACTIVE, empty, basketBought);
    private static final Trader vm2 = e.addTrader(0, TraderState.ACTIVE, empty, basketBought);
    private static final Trader pm1 = e.addTrader(1, TraderState.ACTIVE, basketBought, empty);
    private static final Trader pm2 = e.addTrader(1, TraderState.ACTIVE, basketBought, empty);

    // Methods for converting PriceFunctionDTOs.

    @Test
    @Ignore
    public final void testPriceFunctionTO() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting UpdatingFunctionDTOs.

    @Test
    @Ignore
    public final void testUpdatingFunction() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting EconomyDTOs.

    @Test
    @Ignore
    public final void testCommoditySpecificationTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testCommodityBoughtTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testCommoditySoldSettingsTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testCommoditySoldTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testShoppingListTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testTraderSettingsTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testTraderStateTO() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testTraderTO() {
        fail("Not yet implemented"); // TODO
    }

    // Methods for converting ActionDTOs.

    @Test
    @Ignore
    public final void testSpecifications() {
        fail("Not yet implemented"); // TODO
    }

    @SuppressWarnings("unused")
    private static Object[] parametersForTestActionTO() {
        pm1.getCommoditySold(CPU).setCapacity(15);
        pm2.getCommoditySold(CPU).setCapacity(25);

        BiMap<@NonNull Trader, @NonNull Long> traderOids = HashBiMap.create();
        traderOids.put(vm1, 0l);
        traderOids.put(vm2, 1l);
        traderOids.put(pm1, 2l);
        traderOids.put(pm2, 3l);
        ToLongFunction<@NonNull Trader> func1 = traderOids::get;

        ShoppingList shop1 = e.addBasketBought(vm1, basketBought);
        shop1.move(pm1);
        ShoppingList shop2 = e.addBasketBought(vm2, basketBought);
        shop2.move(pm2);
        BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOids = HashBiMap.create();
        shoppingListOids.put(shop1, 10l);
        shoppingListOids.put(shop2, 20l);
        ToLongFunction<@NonNull ShoppingList> func2 = shoppingListOids::get;

        Action activate = new Activate(e, vm1, e.getMarket(basketBought), vm2);
        ActionTO activateTO = ActionTO.newBuilder().setActivate(ActivateTO.newBuilder()
                        .setTraderToActivate(0l).setModelSeller(1l)
                        .addTriggeringBasket(CommoditySpecificationTO.newBuilder().setType(100)
                                        .setQualityLowerBound(0)
                                        .setQualityUpperBound(Integer.MAX_VALUE).build()))
                        .build();

        Action deactivate = new Deactivate(e, vm2, e.getMarket(basketBought));
        ActionTO deActionTO = ActionTO.newBuilder().setDeactivate(DeactivateTO.newBuilder()
                        .setTraderToDeactivate(1l)
                        .addTriggeringBasket(CommoditySpecificationTO.newBuilder().setType(100)
                                                        .setQualityLowerBound(0)
                                                        .setQualityUpperBound(Integer.MAX_VALUE)
                                                        .build())
                        .build()).build();

        Action move = new Move(e, shop2, pm1);
        ActionTO moveTO = ActionTO.newBuilder().setMove(
                        MoveTO.newBuilder().setShoppingListToMove(20l).setDestination(2l)
                                        .setSource(3l).build())
                        .build();

        Action provisionByDemand = new ProvisionByDemand(e, shop2, pm2);
        ActionTO provisionByDemanTO = ActionTO.newBuilder()
                        .setProvisionByDemand(
                                        ProvisionByDemandTO.newBuilder().setModelBuyer(20l)
                                                        .setModelSeller(3l)
                                                        .build())
                        .build();

        Action provisionBySupply = new ProvisionBySupply(e, pm1);

        ActionTO provisionBySupplyTO = ActionTO.newBuilder()
                        .setProvisionBySupply(
                                        ProvisionBySupplyTO.newBuilder().setModelSeller(2l)
                                                        .build())
                        .build();

        Action resize = new Resize(pm1, CPU, 500);
        ActionTO resizeTO = ActionTO.newBuilder().setResize(ResizeTO.newBuilder()
                        .setSellingTrader(2l).setNewCapacity(500).setOldCapacity(15)
                        .setSpecification(
                                        CommoditySpecificationTO.newBuilder().setType(100)
                                                        .setQualityLowerBound(0)
                                                        .setQualityUpperBound(Integer.MAX_VALUE)
                                                        .build()))
                        .build();

        Action reconfigure = new Reconfigure(e, shop1);
        ActionTO reconfigureTO = ActionTO
                        .newBuilder().setReconfigure(ReconfigureTO.newBuilder()
                                        .setShoppingListToReconfigure(10l).setSource(2l).build())
                        .build();
        return new Object[][] {{activate, func1, func2, new Topology(), activateTO},
                        {deactivate, func1, func2, new Topology(), deActionTO},
                        {move, func1, func2, new Topology(), moveTO},
                        {provisionByDemand, func1, func2, new Topology(), provisionByDemanTO},
                        {provisionBySupply, func1, func2, new Topology(), provisionBySupplyTO},
                        {resize, func1, func2, new Topology(), resizeTO},
                        {reconfigure, func1, func2, new Topology(), reconfigureTO}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: AnalysisToProtobuf().actionTO({0},{1},{2},{3}) == {4}")
    public final void testActionTO(@NonNull Action input,
                    @NonNull ToLongFunction<@NonNull Trader> traderOid,
                    @NonNull ToLongFunction<@NonNull ShoppingList> shoppingListOid, Topology topo,
                    ActionTO expect) {
        ActionTO actionTO =
                        AnalysisToProtobuf.actionTO(input, traderOid, shoppingListOid, topo);
        assertEquals(expect, actionTO);
    }

    // Methods for converting CommunicationDTOs.

    @Test
    @Ignore
    public final void testAnalysisResults() {
        fail("Not yet implemented"); // TODO
    }

} // end AnalysisToProtobufTest class
