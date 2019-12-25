package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;
import com.vmturbo.platform.analysis.utilities.PlacementResults;

public class PlacementUnitTest {

    /**
     * Test the quote computation on current supplier when it is active or inactive.
     */
    @Test
    public void testCurrentQuote() {
        Economy economy = new Economy();
        Trader vm = economy.addTrader(1, TraderState.ACTIVE, new Basket(TestUtils.VCPU),
                new Basket(TestUtils.CPU));
        Trader pm1 = TestUtils.createPM(economy, new ArrayList<>(), 100, 200, true);
        List<Entry<ShoppingList, Market>> movableSlByMarket = economy.getMarketsAsBuyer(vm).entrySet()
                .stream().collect(Collectors.toList());
        ShoppingList sl = movableSlByMarket.get(0).getKey();
        sl.setQuantity(0, 50);
        sl.move(pm1);
        pm1.getCommoditiesSold().get(0).setQuantity(50);
        assertEquals(2, Placement.computeCurrentQuote(economy, movableSlByMarket), TestUtils.FLOATING_POINT_DELTA);

        Trader pm2 = economy.addTrader(TestUtils.PM_TYPE, TraderState.INACTIVE, new Basket(TestUtils.CPU),
                                       new HashSet<>());
        pm2.getCommoditiesSold().get(0).setCapacity(100);
        pm2.getCommoditiesSold().get(0).setQuantity(50);
        sl.move(pm2);
        assertTrue(Double.isInfinite(Placement.computeCurrentQuote(economy, movableSlByMarket)));
    }

    public void testGroupLeaderAlwaysMoves_ShopAlone() {
        testGroupLeaderAlwaysMoves(false);
    }

    public void testGroupLeaderAlwaysMoves_ShopTogether() {
        testGroupLeaderAlwaysMoves(true);
    }

    private void testGroupLeaderAlwaysMoves(boolean isShopTogether) {
        Economy economy = new Economy();
        Trader pm1 = TestUtils.createPM(economy, Arrays.asList(0l), 100, 100, false);
        pm1.setDebugInfoNeverUseInCode("PM1");
        Trader st1 = TestUtils.createStorage(economy, Arrays.asList(0l), 300, false);
        st1.setDebugInfoNeverUseInCode("DS1");
        Trader vm1 = TestUtils.createVM(economy);
        ShoppingList sl1 = TestUtils.createAndPlaceShoppingList(economy,
            Arrays.asList(TestUtils.CPU, TestUtils.MEM), vm1, new double[]{60, 0}, pm1);
        sl1.setGroupFactor(2);
        ShoppingList sl2 = TestUtils.createAndPlaceShoppingList(economy,
            Arrays.asList(TestUtils.ST_AMT), vm1, new double[]{100}, st1);
        sl2.setGroupFactor(1);
        vm1.setDebugInfoNeverUseInCode("VM1");
        vm1.getSettings().setIsShopTogether(isShopTogether);
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();

        PlacementResults results = Placement.generatePlacementDecisions(economy, Arrays.asList(sl1, sl2));

        Assert.assertEquals(1, results.getActions().size());
        Action action = results.getActions().get(0);
        Move move = (Move)action;
        assertTrue(move.getSource() == pm1);
        assertTrue(move.getDestination() == pm1);
    }
}
