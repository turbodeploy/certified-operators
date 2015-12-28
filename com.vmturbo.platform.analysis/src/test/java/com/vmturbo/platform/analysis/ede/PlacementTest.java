package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

import junitparams.JUnitParamsRunner;

/**
 * A test case for the {@link Placement} class.
 */
@RunWith(JUnitParamsRunner.class)
public final class PlacementTest {
    // Fields
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification DRS = new CommoditySpecification(1);
    private static final CommoditySpecification MEM = new CommoditySpecification(2);

    private static final Basket EMPTY = new Basket();
    private static final Basket PM = new Basket(CPU,MEM);
    private static final Basket PM_EXT = new Basket(CPU,DRS,MEM);

    // Methods

    @Test
    @Ignore
    public final void testPlacementDecisions() {
        fail("Not yet implemented"); // TODO
    }

    // TODO: refactor as parameterized test
    @Test // No current supplier case
    public final void testMoveTraderAndUpdateQuantitiesSold_NoSupplier() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(0, TraderState.ACTIVE, EMPTY);
        Trader pm1 = economy.addTrader(0, TraderState.ACTIVE, PM);

        BuyerParticipation participation = economy.addBasketBought(vm, PM);
        participation.setQuantity(0, 10);
        participation.setQuantity(1, 20);
        pm1.getCommoditySold(CPU).setQuantity(3);
        pm1.getCommoditySold(MEM).setQuantity(6);

        Placement.moveTraderAndUpdateQuantitiesSold(participation, pm1, PM, economy);

        assertEquals(10, participation.getQuantities()[0], 0f);
        assertEquals(20, participation.getQuantities()[1], 0f);
        assertEquals(13, pm1.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(26, pm1.getCommoditySold(MEM).getQuantity(), 0f);
    }

    @Test // Case where the supplier sells the exact basket requested
    public final void testMoveTraderAndUpdateQuantitiesSold_ExactBasket() {
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

        economy.moveTrader(participation, pm1);

        Placement.moveTraderAndUpdateQuantitiesSold(participation, pm2, PM, economy);

        assertEquals(10, participation.getQuantities()[0], 0f);
        assertEquals(20, participation.getQuantities()[1], 0f);
        assertEquals(2, pm1.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(5, pm1.getCommoditySold(MEM).getQuantity(), 0f);
        assertEquals(13, pm2.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(26, pm2.getCommoditySold(MEM).getQuantity(), 0f);
    }

    @Test // Case where the current supplier sells a subset or requested basket
    public final void testMoveTraderAndUpdateQuantitiesSold_SubsetBasket() {
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

        economy.moveTrader(participation, pm1);

        Placement.moveTraderAndUpdateQuantitiesSold(participation, pm2, PM_EXT, economy);

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
    public final void testMoveTraderAndUpdateQuantitiesSold_SupersetBasket() {
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

        economy.moveTrader(participation, pm1);

        Placement.moveTraderAndUpdateQuantitiesSold(participation, pm2, PM, economy);

        assertEquals(10, participation.getQuantities()[0], 0f);
        assertEquals(30, participation.getQuantities()[1], 0f);
        assertEquals(2, pm1.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(25, pm1.getCommoditySold(DRS).getQuantity(), 0f);
        assertEquals(7, pm1.getCommoditySold(MEM).getQuantity(), 0f);
        assertEquals(13, pm2.getCommoditySold(CPU).getQuantity(), 0f);
        assertEquals(6, pm2.getCommoditySold(DRS).getQuantity(), 0f);
        assertEquals(39, pm2.getCommoditySold(MEM).getQuantity(), 0f);
    }

} // end PlacementTest class
