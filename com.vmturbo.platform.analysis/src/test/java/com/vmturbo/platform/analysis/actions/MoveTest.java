package com.vmturbo.platform.analysis.actions;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link Move} class.
 */
@RunWith(JUnitParamsRunner.class)
public final class MoveTest {
    // Fields
    private static final ToDoubleFunction<List<Double>> MAX_DOUBLE_LIST =
            quantities -> quantities.isEmpty() ? 0.0 : Collections.max(quantities);
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification DRS = new CommoditySpecification(1);
    private static final CommoditySpecification LAT1 = new CommoditySpecification(2);
    private static final CommoditySpecification MEM = new CommoditySpecification(3);
    private static final CommoditySpecification LAT2 = new CommoditySpecification(4);

    private static final Basket EMPTY = new Basket();
    private static final Basket PM = new Basket(CPU, MEM);
    private static final Basket PM_EXT = new Basket(CPU, DRS, MEM);
    private static final Basket PM_ALL = new Basket(CPU, DRS, LAT1, MEM, LAT2);

    private static final Basket BASKET1 = new Basket(CPU, LAT1, MEM, LAT2);
    private static final Basket BASKET2 = new Basket(CPU, DRS, LAT1, MEM);

    // Methods

    @Test
    @Parameters
    @TestCaseName("Test #{index}: new Move({0},{1},{2})")
    public final void testMove(@NonNull Economy economy, @NonNull BuyerParticipation target, @Nullable Trader destination) {
        Move move = new Move(economy,target,destination);

        assertSame(economy, move.getEconomy());
        assertSame(target, move.getTarget());
        assertSame(target.getSupplier(), move.getSource());
        assertSame(destination, move.getDestination());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestMove() {
        Economy e1 = new Economy();
        BuyerParticipation p1 = e1.addBasketBought(e1.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);

        Economy e2 = new Economy();
        BuyerParticipation p2 = e2.addBasketBought(e2.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        p2.move(e2.addTrader(1, TraderState.ACTIVE, EMPTY));

        Economy e3 = new Economy();
        BuyerParticipation p3 = e3.addBasketBought(e3.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        Trader t3 = e3.addTrader(0, TraderState.ACTIVE, EMPTY);

        Economy e4 = new Economy();
        BuyerParticipation p4 = e4.addBasketBought(e4.addTrader(0, TraderState.ACTIVE, EMPTY), EMPTY);
        p4.move(e4.addTrader(0, TraderState.ACTIVE, EMPTY));
        Trader t4 = e4.addTrader(0, TraderState.ACTIVE, EMPTY);

        return new Object[][]{{e1,p1,null},{e2,p2,null},{e3,p3,t3},{e4,p4,t4}};
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.serialize({1}) == {2}")
    public final void testSerialize(@NonNull Move move,
            @NonNull Function<@NonNull Trader, @NonNull String> oid, @NonNull String serialized) {
        assertEquals(move.serialize(oid), serialized);
    }

    // TODO (Vaptistis): add more tests once semantics are clear.
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSerialize() {
        @NonNull Map<@NonNull Trader, @NonNull String> oids = new HashMap<>();
        @NonNull Function<@NonNull Trader, @NonNull String> oid = oids::get;

        Economy e1 = new Economy();
        Trader t11 = e1.addTrader(0, TraderState.ACTIVE, EMPTY);
        BuyerParticipation bp1 = e1.addBasketBought(t11, EMPTY);
        Trader t12 = e1.addTrader(0, TraderState.ACTIVE, EMPTY);
        bp1.move(t12);
        Trader t21 = e1.addTrader(0, TraderState.INACTIVE, EMPTY);
        BuyerParticipation bp2 = e1.addBasketBought(t21, EMPTY);
        Trader t22 = e1.addTrader(0, TraderState.INACTIVE, EMPTY);
        bp2.move(t22);

        oids.put(t11, "id1");
        oids.put(t12, "id2");
        oids.put(t21, "id3");
        oids.put(t22, "id4");

        return new Object[][]{
            {new Move(e1,bp1,t21),oid,"<action type=\"move\" target=\"id1\" source=\"id2\" destination=\"id3\" />"},
            {new Move(e1,bp2,t11),oid,"<action type=\"move\" target=\"id3\" source=\"id4\" destination=\"id1\" />"},
        };
    }

    // TODO: refactor as parameterized test
    @Test // No current supplier case
    public final void testMoveTake_NoSupplier() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
        Trader pm1 = economy.addTrader(0, TraderState.ACTIVE, PM);

        BuyerParticipation participation = economy.addBasketBought(vm, PM);
        participation.setQuantity(0, 10);
        participation.setQuantity(1, 20);
        pm1.getCommoditySold(CPU).setQuantity(3);
        pm1.getCommoditySold(MEM).setQuantity(6);

        new Move(economy,participation,pm1).take();

        assertEquals(10, participation.getQuantities()[0], 0f);
        assertEquals(20, participation.getQuantities()[1], 0f);
        assertEquals(13, pm1.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(26, pm1.getCommoditySold(MEM).getQuantity(), 0f);
    }

    @Test // Case where the supplier sells the exact basket requested
    public final void testMoveTake_ExactBasket() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
        Trader pm1 = economy.addTrader(0, TraderState.ACTIVE, PM);
        Trader pm2 = economy.addTrader(0, TraderState.ACTIVE, PM);

        BuyerParticipation participation = economy.addBasketBought(vm, PM);
        participation.setQuantity(0, 10);
        participation.setQuantity(1, 20);
        pm1.getCommoditySold(CPU).setQuantity(12);
        pm1.getCommoditySold(MEM).setQuantity(25);
        pm2.getCommoditySold(CPU).setQuantity(3);
        pm2.getCommoditySold(MEM).setQuantity(6);

        participation.move(pm1);

        new Move(economy,participation,pm2).take();

        assertEquals(10, participation.getQuantities()[0], 0f);
        assertEquals(20, participation.getQuantities()[1], 0f);
        assertEquals(2, pm1.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(5, pm1.getCommoditySold(MEM).getQuantity(), 0f);
        assertEquals(13, pm2.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(26, pm2.getCommoditySold(MEM).getQuantity(), 0f);
    }

    @Test // Case where the current supplier sells a subset or requested basket
    public final void testMoveTake_SubsetBasket() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
        Trader pm1 = economy.addTrader(0, TraderState.ACTIVE, PM);
        Trader pm2 = economy.addTrader(0, TraderState.ACTIVE, PM_EXT);

        BuyerParticipation participation = economy.addBasketBought(vm, PM_EXT);
        participation.setQuantity(0, 10);
        participation.setQuantity(1, 20);
        participation.setQuantity(2, 30);
        pm1.getCommoditySold(CPU).setQuantity(12);
        pm1.getCommoditySold(MEM).setQuantity(37);
        pm2.getCommoditySold(CPU).setQuantity(3);
        pm2.getCommoditySold(DRS).setQuantity(6);
        pm2.getCommoditySold(MEM).setQuantity(9);

        participation.move(pm1);

        new Move(economy,participation,pm2).take();

        assertEquals(10, participation.getQuantities()[0], 0f);
        assertEquals(20, participation.getQuantities()[1], 0f);
        assertEquals(30, participation.getQuantities()[2], 0f);
        assertEquals(2, pm1.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(7, pm1.getCommoditySold(MEM).getQuantity(), 0f);
        assertEquals(13, pm2.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(26, pm2.getCommoditySold(DRS).getQuantity(), 0f);
        assertEquals(39, pm2.getCommoditySold(MEM).getQuantity(), 0f);
    }

    @Test // Case where the current supplier sells a superset of requested basket
    public final void testMoveTake_SupersetBasket() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
        Trader pm1 = economy.addTrader(0, TraderState.ACTIVE, PM_EXT);
        Trader pm2 = economy.addTrader(0, TraderState.ACTIVE, PM_EXT);

        BuyerParticipation participation = economy.addBasketBought(vm, PM);
        participation.setQuantity(0, 10);
        participation.setQuantity(1, 30);
        pm1.getCommoditySold(CPU).setQuantity(12);
        pm1.getCommoditySold(DRS).setQuantity(25);
        pm1.getCommoditySold(MEM).setQuantity(37);
        pm2.getCommoditySold(CPU).setQuantity(3);
        pm2.getCommoditySold(DRS).setQuantity(6);
        pm2.getCommoditySold(MEM).setQuantity(9);

        participation.move(pm1);

        new Move(economy,participation,pm2).take();

        assertEquals(10, participation.getQuantities()[0], 0f);
        assertEquals(30, participation.getQuantities()[1], 0f);
        assertEquals(2, pm1.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(25, pm1.getCommoditySold(DRS).getQuantity(), 0f);
        assertEquals(7, pm1.getCommoditySold(MEM).getQuantity(), 0f);
        assertEquals(13, pm2.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(6, pm2.getCommoditySold(DRS).getQuantity(), 0f);
        assertEquals(39, pm2.getCommoditySold(MEM).getQuantity(), 0f);
    }

    @Test // non-additive commodities
    public final void testMoveTake_NonAdditive() {
        Economy economy = new Economy();
        economy.getQuantityFunctions().put(LAT1, MAX_DOUBLE_LIST);
        economy.getQuantityFunctions().put(LAT2, MAX_DOUBLE_LIST);
        Trader vm1 = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
        Trader vm2 = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
        Trader vm3 = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
        Trader pm1 = economy.addTrader(0, TraderState.ACTIVE, PM_ALL);
        Trader pm2 = economy.addTrader(0, TraderState.ACTIVE, PM_ALL);

        BuyerParticipation part1 = economy.addBasketBought(vm1, BASKET1);
        BuyerParticipation part2 = economy.addBasketBought(vm2, BASKET1);
        BuyerParticipation part3 = economy.addBasketBought(vm3, BASKET1);
        BuyerParticipation part4 = economy.addBasketBought(vm3, BASKET2);
        BuyerParticipation part5 = economy.addBasketBought(vm3, PM_EXT);

        // LAT1
        int lat1InBasket1 = BASKET1.indexOf(LAT1);
        int lat1InBasket2 = BASKET2.indexOf(LAT1);
        part1.setQuantity(lat1InBasket1, 10);
        part2.setQuantity(lat1InBasket1, 30);
        part3.setQuantity(lat1InBasket1, 20);
        part4.setQuantity(lat1InBasket2, 15);
        // LAT2
        int lat2InBasket1 = BASKET1.indexOf(LAT2);
        part1.setQuantity(lat2InBasket1, 150);
        part2.setQuantity(lat2InBasket1, 100);
        part3.setQuantity(lat2InBasket1, 120);

        pm1.getCommoditySold(LAT1).setQuantity(12);
        pm2.getCommoditySold(LAT1).setQuantity(3);

        // Initial placement
        part1.move(pm1);
        part2.move(pm1);
        part3.move(pm1);
        part4.move(pm2);
        part5.move(pm2);

        Action moveToPm2 = new Move(economy, part2, pm2);

        assertSame(moveToPm2, moveToPm2.take());
        assertEquals(20, pm1.getCommoditySold(LAT1).getQuantity(), 0f);
        assertEquals(30, pm2.getCommoditySold(LAT1).getQuantity(), 0f);
        assertEquals(150, pm1.getCommoditySold(LAT2).getQuantity(), 0f);
        assertEquals(100, pm2.getCommoditySold(LAT2).getQuantity(), 0f);

        assertSame(moveToPm2, moveToPm2.rollback());
        assertEquals(30, pm1.getCommoditySold(LAT1).getQuantity(), 0f);
        assertEquals(15, pm2.getCommoditySold(LAT1).getQuantity(), 0f);
        assertEquals(150, pm1.getCommoditySold(LAT2).getQuantity(), 0f);
        assertEquals(0, pm2.getCommoditySold(LAT2).getQuantity(), 0f);
    }

    @Test
    @Ignore
    public final void testDebugDescription() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore
    public final void testDebugReason() {
        fail("Not yet implemented"); // TODO
    }

} // end MoveTest class
