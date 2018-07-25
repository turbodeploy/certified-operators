package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

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
}
