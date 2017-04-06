package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ledger.Ledger;

public class SuspensionTest {
    private static final int VM_TYPE = 0;
    private static final int PM_TYPE = 1;
    private static final double CAPACITY = 111;
    private static final double UTILIZATION_UPPER_BOUND = 0.9;

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);

    // Baskets to use in tests
    private static final Basket EMPTY = new Basket();
    private static final Basket PM_SMALL = new Basket(CPU,MEM);


    @Test
    public void testDontSuspendNonSuspendableTraders () {
        Economy economy = new Economy();
        // adding 2 non-suspendable traders
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));

        economy.populateMarketsWithSellers();
        Ledger ledger = new Ledger(economy);
        Ede ede = new Ede();

        Suspension suspension = new Suspension();
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger, ede, false);

        // verify that we do not suspend non-suspendaable traders
        assertTrue(suspAxns.isEmpty());
    }


    @Test
    public void testDontSuspendWhenNoActiveSellers() {
        Economy economy = new Economy();
        // adding 2 non-suspendable traders
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);

        Suspension suspension = new Suspension();
        // check if returned action list is empty
        assertTrue(suspension.suspensionDecisions(economy, new Ledger(economy), new Ede(), false).isEmpty());
    }

    @Test
    public void testSuspensionActions () {
        Economy economy = new Economy();
        // adding 2 non-suspendable traders
        economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        Trader seller = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        seller.getCommoditiesSold().stream().forEach(cs -> cs.setCapacity(CAPACITY).getSettings().setUtilizationUpperBound(UTILIZATION_UPPER_BOUND));
        seller.getSettings().setSuspendable(true);

        seller.getSettings().setMinDesiredUtil(0.6);

        Trader vm = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        ShoppingList vmSl = economy.addBasketBought(vm, new Basket(CPU)).setMovable(true);
        for (int index = 0; index<vmSl.getBasket().size(); index++) {
            vmSl.setQuantity(index, 10);
        }
        // placing the VM on the 2nd PM. This leaves the 1st PM empty without customers and 
        // hence will subsequently be suspended
        for (ShoppingList buyer : economy.getMarkets().iterator().next().getBuyers()) {
            Action axn = (new Move(economy, buyer, seller));
            axn.take();
        }

        economy.populateMarketsWithSellers();
        Ledger ledger = new Ledger(economy);
        Ede ede = new Ede();

        Suspension suspension = new Suspension();
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger, ede, false);

        // verify that the we suspend extra seller
        assertTrue(!suspAxns.isEmpty());

    }

    @Test
    public void testSuspensionNoActions() {
        Economy economy = new Economy();
        Trader seller  = economy.addTrader(PM_TYPE, TraderState.ACTIVE, PM_SMALL);
        seller.getSettings().setSuspendable(true);
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        economy.getMarkets().forEach(mkt -> mkt.getBuyers().stream().forEach(buyer -> buyer.move(seller)));
        economy.populateMarketsWithSellers();
        Ledger ledger = new Ledger(economy);
        Ede ede = new Ede();

        Suspension suspension = new Suspension();
        List<Action> suspAxns = suspension.suspensionDecisions(economy, ledger, ede, false);

        // verify that we do not suspend the only seller
        assertTrue(suspAxns.isEmpty());
    }

    @Test
    public void testAdjustUtilThreshold() {
        Economy economy = new Economy();
        Trader seller = economy.addTrader(VM_TYPE, TraderState.ACTIVE, PM_SMALL);

        CommoditySoldSettings commSoldSett = seller.getCommoditiesSold().get(0).getSettings();
        commSoldSett.setUtilizationUpperBound(0.7).setOrigUtilizationUpperBound(0.7);

        seller.getSettings().setMaxDesiredUtil(0.5);

        Suspension suspension = new Suspension();
        suspension.adjustUtilThreshold(economy, true);

        // verify that the utilUpperBound has changed to maxDesiredUtil
        assertTrue(commSoldSett.getUtilizationUpperBound() == seller.getSettings().getMaxDesiredUtil());
        suspension.adjustUtilThreshold(economy, false);

        // verify that the utilUpperBound has changed to origUtilDesiredUtil
        assertTrue(commSoldSett.getUtilizationUpperBound() == commSoldSett.getOrigUtilizationUpperBound());
    }

    @Test
    public void testSuspendTrader () {
        Economy economy = new Economy();
        // add buyer and seller to create a market
        economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY, new Basket(CPU));
        Trader seller = economy.addTrader(VM_TYPE, TraderState.ACTIVE, PM_SMALL);
        economy.populateMarketsWithSellers();

        List<Action> actions = new ArrayList<>();
        Suspension suspension = new Suspension();
        suspension.suspendTrader(economy, economy.getMarketsAsSeller(seller).iterator().next(), seller, actions);

        // verify that we have 1 deactivate action when suspendTrader is called
        assertTrue(actions.size() == 1);
        assertTrue(actions.get(0) instanceof Deactivate);
    }
}
