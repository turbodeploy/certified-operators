package com.vmturbo.platform.analysis.translators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Set;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.BiMap;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.CompoundMove;
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
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.CompoundMoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Performance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO.CommodityMaxAmountAvailableEntry;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO.CommodityNewCapacityEntry;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
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
    private static final CommoditySpecification CPU = new CommoditySpecification(100, 1000, 0, Integer.MAX_VALUE);
    private static final CommoditySpecification MEM = new CommoditySpecification(101, 1001, 0, Integer.MAX_VALUE);
    private static final CommoditySpecification STORAGE_AMOUNT = new CommoditySpecification(200);
    private static final Basket empty = new Basket();
    private static final Basket basketBought1 = new Basket(CPU, MEM);
    private static final Basket basketBought2 = new Basket(STORAGE_AMOUNT);

    private static final Trader vm1 = e.addTrader(0, TraderState.INACTIVE, empty, basketBought1);
    private static final Trader vm2 = e.addTrader(0, TraderState.ACTIVE, empty, basketBought1);
    private static final Trader vm3 =
                    e.addTrader(0, TraderState.ACTIVE, empty, basketBought1, basketBought2);
    private static final Trader pm1 = e.addTrader(1, TraderState.ACTIVE, basketBought1, empty);
    private static final Trader pm2 = e.addTrader(1, TraderState.ACTIVE, basketBought1, empty);
    private static final Trader st1 = e.addTrader(2, TraderState.ACTIVE, basketBought2, empty);
    private static final Trader st2 = e.addTrader(2, TraderState.ACTIVE, basketBought2, empty);
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

    @SuppressWarnings({"unused", "unchecked"})
    private static Object[] parametersForTestActionTO() {
        IdentityGenerator.initPrefix(0);
        pm1.getCommoditySold(CPU).setCapacity(15).setQuantity(10);
        pm1.getCommoditySold(MEM).setCapacity(100).setQuantity(10);
        pm2.getCommoditySold(CPU).setCapacity(25).setQuantity(10);
        pm2.getCommoditySold(MEM).setCapacity(200).setQuantity(10);

        Topology topo = new Topology();
        BiMap<@NonNull Trader, @NonNull Long> traderOids = null;
        BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOids = null;
        ShoppingList shop1 = null;
        ShoppingList shop2 = null;
        try {
            // set the traderOid_ to be accessible so that we could get manually assign
            // oid to a trader
            Field traderOidField = Topology.class.getDeclaredField("traderOids_");
            traderOidField.setAccessible(true);
            traderOids = (BiMap<@NonNull Trader, @NonNull Long>)traderOidField.get(topo);
            traderOids.put(vm1, 0l);
            traderOids.put(vm2, 1l);
            traderOids.put(pm1, 2l);
            traderOids.put(pm2, 3l);
            traderOids.put(st1, 4l);
            traderOids.put(st2, 5l);
            traderOids.put(vm3, 6l);

            shop1 = e.addBasketBought(vm1, basketBought1);
            shop1.move(pm1);
            shop2 = e.addBasketBought(vm2, basketBought1);
            shop2.move(pm2);

            Field shoppingListOidField = Topology.class.getDeclaredField("shoppingListOids_");
            shoppingListOidField.setAccessible(true);
            shoppingListOids = (BiMap<@NonNull ShoppingList, @NonNull Long>)shoppingListOidField
                            .get(topo);
            shoppingListOids.put(shop1, 10l);
            shoppingListOids.put(shop2, 20l);
        } catch (Exception e) {

        }
        Action activate = new Activate(e, pm1, e.getMarket(basketBought1), pm2, TestUtils.CPU);
        ActionTO activateTO = ActionTO.newBuilder().setActivate(ActivateTO.newBuilder()
                        .setTraderToActivate(2l).setModelSeller(3l)
                        .setMostExpensiveCommodity(((Activate)activate)
                                        .getReason().getBaseType())
                        .addTriggeringBasket(CommoditySpecificationTO.newBuilder().setType(100)
                                        .setBaseType(1000).setQualityLowerBound(0)
                                        .setQualityUpperBound(Integer.MAX_VALUE)
                                        .build())
                        .addTriggeringBasket(CommoditySpecificationTO.newBuilder().setType(101)
                                        .setBaseType(1001).setQualityLowerBound(0)
                                        .setQualityUpperBound(Integer.MAX_VALUE)
                                        .build()))
                        .setImportance((float)((ActionImpl)activate).getImportance())
                        .setIsNotExecutable(false)
                        .build();

        Action deactivate = new Deactivate(e, vm2, e.getMarket(basketBought1));
        ActionTO deActionTO = ActionTO.newBuilder().setDeactivate(DeactivateTO.newBuilder()
                        .setTraderToDeactivate(1l)
                        .addTriggeringBasket(CommoditySpecificationTO.newBuilder().setType(100)
                                                        .setBaseType(1000)
                                                        .setQualityLowerBound(0)
                                                        .setQualityUpperBound(Integer.MAX_VALUE)
                                                        .build())
                        .addTriggeringBasket(CommoditySpecificationTO.newBuilder().setType(101)
                                                        .setBaseType(1001)
                                                        .setQualityLowerBound(0)
                                                        .setQualityUpperBound(Integer.MAX_VALUE)
                                                        .build()).build()).setImportance((float)(
                                                        (ActionImpl)deactivate).getImportance())
                        .setIsNotExecutable(false)
                        .build();

        Action move = new Move(e, shop2, pm1);
        ActionTO moveTO = ActionTO.newBuilder().setMove(
                        MoveTO.newBuilder().setShoppingListToMove(20l).setDestination(2l)
                                .setCouponId(2l).setCouponDiscount(0.0)
                                        .setSource(3l).setMoveExplanation(MoveExplanation.newBuilder()
                                                        .setPerformance(Performance.newBuilder()
                                                                        .build()).build()).build())
                        .setImportance((float)((ActionImpl)move).getImportance())
                        .setIsNotExecutable(false)
                        .build();
        Action provisionByDemand = new ProvisionByDemand(e, shop2, pm2);
        provisionByDemand.take(); // we call take to create provisionedSeller
        // assign -1 as oid for the newly provisioned seller
        traderOids.put(((ProvisionByDemand)provisionByDemand).getProvisionedSeller(), -1l);
        ActionTO provisionByDemanTO = ActionTO.newBuilder()
                        .setProvisionByDemand(
                                        ProvisionByDemandTO.newBuilder().setModelBuyer(20l)
                                        .setProvisionedSeller(-1l)
                                        .setModelSeller(3l)
                                        .addCommodityNewCapacityEntry(CommodityNewCapacityEntry
                                                        .newBuilder().setCommodityBaseType(1000)
                                                        .setNewCapacity(25).build())
                                        .addCommodityNewCapacityEntry(CommodityNewCapacityEntry
                                                        .newBuilder().setCommodityBaseType(1001)
                                                        .setNewCapacity(200).build())
                                        .addCommodityMaxAmountAvailable(
                                                        CommodityMaxAmountAvailableEntry
                                                        .newBuilder().setCommodityBaseType(1000)
                                                        .setMaxAmountAvailable(25).setRequestedAmount(0)
                                                        .build())
                                        .addCommodityMaxAmountAvailable(
                                                        CommodityMaxAmountAvailableEntry
                                                        .newBuilder().setCommodityBaseType(1001)
                                                        .setMaxAmountAvailable(200).setRequestedAmount(0)
                                                        .build()))
                        .setImportance((float)((ActionImpl)provisionByDemand).getImportance())
                        .setIsNotExecutable(false)
                        .build();

        Action provisionBySupply = new ProvisionBySupply(e, pm1, TestUtils.CPU);
        provisionBySupply.take(); // we call take to create provisionedSeller
        // assign -2 as oid for the newly povisioned seller
        traderOids.put(((ProvisionBySupply)provisionBySupply).getProvisionedSeller(), -2l);
        ActionTO provisionBySupplyTO = ActionTO.newBuilder()
                        .setProvisionBySupply(
                                        ProvisionBySupplyTO.newBuilder().setModelSeller(2l)
                                        .setProvisionedSeller(-2l)
                                                        .setMostExpensiveCommodity(
                                                                        ((ProvisionBySupply)provisionBySupply)
                                                                                        .getReason()
                                                                                        .getBaseType())
                                        .build()).setImportance((float)(
                                        (ActionImpl)provisionBySupply).getImportance())
                        .setIsNotExecutable(false)
                        .build();

        Action resize = new Resize(e, pm1, CPU, 500);
        ActionTO resizeTO = ActionTO.newBuilder().setResize(ResizeTO.newBuilder()
                        .setSellingTrader(2l).setNewCapacity(500).setOldCapacity(15)
                        .setStartUtilization(0).setEndUtilization(0)
                        .setSpecification(
                                        CommoditySpecificationTO.newBuilder().setType(100)
                                                        .setBaseType(1000)
                                                        .setQualityLowerBound(0)
                                                        .setQualityUpperBound(Integer.MAX_VALUE)
                                                        .build())).setImportance((float)
                                                        ((ActionImpl)resize).getImportance())
                        .setIsNotExecutable(false)
                        .build();

        Action reconfigure = new Reconfigure(e, shop1);
        ActionTO reconfigureTO = ActionTO
                        .newBuilder().setReconfigure(ReconfigureTO.newBuilder()
                                        .setShoppingListToReconfigure(10l).setSource(2l).build())
                        .setImportance((float)((ActionImpl)reconfigure).getImportance())
                        .setIsNotExecutable(false)
                        .build();

        ShoppingList[] twoShoppingLists = Arrays.copyOf(e.getMarketsAsBuyer(vm3).keySet().toArray(),
                                                        2, ShoppingList[].class);
        // assign and oid for each shoppingList and make shoppinglist buy from supplier
        shoppingListOids.put(twoShoppingLists[0], 30l);
        shoppingListOids.put(twoShoppingLists[1], 40l);
        twoShoppingLists[0].move(pm1);
        twoShoppingLists[1].move(st1);

        Action compoundMove = CompoundMove.createAndCheckCompoundMoveWithImplicitSources(
                                    e, Arrays.asList(twoShoppingLists), Arrays.asList(pm2, st2));
        CompoundMoveTO.Builder compoundMoveTO = CompoundMoveTO.newBuilder();

        ActionTO compoundMoveActionTO = ActionTO.newBuilder()
                        .setCompoundMove(compoundMoveTO
                                        .addMoves(MoveTO.newBuilder().setShoppingListToMove(30l)
                                                        .setDestination(3l).setCouponId(3l)
                                                .setCouponDiscount(0.0).setSource(2l)
                                                        .setMoveExplanation(MoveExplanation
                                                                        .newBuilder()
                                                                        .setPerformance(Performance
                                                                                        .newBuilder()
                                                                                        .build())
                                                                        .build())
                                                      .build())
                                        .addMoves(MoveTO.newBuilder().setShoppingListToMove(40l)
                                                        .setDestination(5l).setCouponId(5l)
                                                .setCouponDiscount(0.0).setSource(4l)
                                                        .setMoveExplanation(MoveExplanation
                                                                        .newBuilder()
                                                                        .setPerformance(Performance
                                                                                        .newBuilder()
                                                                                        .build())
                                                                        .build())
                                                        .build())
                                        .build()).setImportance((float)(
                                                        (ActionImpl)compoundMove).getImportance())
                        .setIsNotExecutable(false)
                        .build();
        return new Object[][] {{activate, traderOids, shoppingListOids, topo, activateTO},
                        {deactivate, traderOids, shoppingListOids, topo, deActionTO},
                        {move, traderOids, shoppingListOids, topo, moveTO},
                        {provisionByDemand, traderOids, shoppingListOids, topo,
                                        provisionByDemanTO},
                        {provisionBySupply, traderOids, shoppingListOids, topo,
                                        provisionBySupplyTO},
                        {resize, traderOids, shoppingListOids, topo, resizeTO},
                        {reconfigure, traderOids, shoppingListOids, topo, reconfigureTO},
                        {compoundMove, traderOids, shoppingListOids, topo,
                                        compoundMoveActionTO}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: AnalysisToProtobuf().actionTO({0},{1},{2},{3}) == {4}")
    public final void testActionTO(@NonNull Action input,
                    @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOid,
                    @NonNull BiMap<@NonNull ShoppingList, @NonNull Long> shoppingListOid,
                    Topology topo,
                    ActionTO expect) {
        ActionTO actionTO =
                        AnalysisToProtobuf.actionTO(input, traderOid, shoppingListOid, topo);
        // we dont know the oid of he provisioned seller because the oid is based on time
        // so comparing other fields one by one
        if (input instanceof ProvisionByDemand) {
            ProvisionByDemandTO output = expect.getProvisionByDemand();
            assertEquals(output.getModelBuyer(), actionTO.getProvisionByDemand().getModelBuyer());
            assertEquals(output.getModelSeller(), actionTO.getProvisionByDemand().getModelSeller());
        }  else if (input instanceof ProvisionBySupply) {
            ProvisionBySupplyTO output = expect.getProvisionBySupply();
            assertEquals(output.getModelSeller(), actionTO.getProvisionBySupply().getModelSeller());
            assertEquals(output.getMostExpensiveCommodity(),
                            actionTO.getProvisionBySupply().getMostExpensiveCommodity());
        } else if (input instanceof Move) {
            final MoveTO output = expect.getMove();
            final MoveTO actionTOMove = actionTO.getMove();
            assertEquals(output.getCouponId(), actionTOMove.getCouponId());
            assertEquals(output.getCouponDiscount(), actionTOMove.getCouponDiscount(), 0.001);
        } else if (input instanceof CompoundMove) {
            final CompoundMoveTO output = expect.getCompoundMove();
            final CompoundMoveTO actionTOMove = actionTO.getCompoundMove();
            final MoveTO firstActionTOMove = actionTOMove.getMoves(0);
            final MoveTO firstMove = output.getMoves(0);
            assertEquals(firstMove.getCouponId(), firstActionTOMove.getCouponId());
            assertEquals(firstMove.getCouponDiscount(), firstActionTOMove.getCouponDiscount(), 0.001);
            final MoveTO secondActionTOMove = actionTOMove.getMoves(1);
            final MoveTO secondMove = output.getMoves(1);
            assertEquals(secondMove.getCouponId(), secondActionTOMove.getCouponId());
            assertEquals(secondMove.getCouponDiscount(), secondActionTOMove.getCouponDiscount(), 0.001);
        } else {
            assertEquals(expect, actionTO);
        }
    }

    @Test
    public final void testActionToNewShoppingLists() throws Exception{
        Topology topology = new Topology();
        // Use reflection to add vm1 to the traders OID map.
        Field traderOidField = Topology.class.getDeclaredField("traderOids_");
        traderOidField.setAccessible(true);
        @SuppressWarnings("unchecked")
        BiMap<Trader, Long> traderOids = (BiMap<Trader, Long>)traderOidField.get(topology);
        traderOids.put(pm1, 100l); // needed when construction the ActionTO
        // Provision
        ProvisionBySupply prov = new ProvisionBySupply(e, pm1, TestUtils.CPU);
        prov.take();
        BiMap<ShoppingList, Long> shoppingListOids = topology.getShoppingListOids();
        assertTrue(shoppingListOids.isEmpty());
        AnalysisToProtobuf.actionTO(prov, traderOids, shoppingListOids, topology);
        Set<ShoppingList> provisionedShoppingLists =
                topology.getEconomy().getMarketsAsBuyer(prov.getProvisionedSeller()).keySet();
        assertTrue(shoppingListOids.keySet().containsAll(provisionedShoppingLists));
        // OIDs of provisioned shopping lists should all be negative
        shoppingListOids.values().stream().forEach(oid -> assertTrue(oid < 0));
    }

    // Methods for converting CommunicationDTOs.

    @Test
    @Ignore
    public final void testAnalysisResults() {
        fail("Not yet implemented"); // TODO
    }

} // end AnalysisToProtobufTest class
