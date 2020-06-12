package com.vmturbo.platform.analysis.economy;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.platform.analysis.topology.Topology;

/**
 * Unit tests for scoped topology.
 */
public class EconomyScopedTopologyTest {
    private static final int TRADER_TYPE_A = 10;
    private static final int TRADER_TYPE_B = 20;
    private static final int TRADER_TYPE_C = 30;
    private static final int TRADER_TYPE_D = 40;

    private static final Basket BASKET_1 = newBasket();
    private static final Basket BASKET_2 = newBasket();
    private static final Basket BASKET_3 = newBasket();
    private static final Basket BASKET_4 = newBasket();
    private static long oid = 0;
    private static final Long CLIQUE_1 = 2001L;
    private static final Long CLIQUE_2 = 2002L;
    private static final Long CLIQUE_3 = 2003L;

    private static Topology topology;
    private static Economy economy;
    private static Trader buyer;
    private static Set<Trader> expectedPotentialSellers;

    /**
     * Construct a topology with one buyer that shops in 3 markets:
     * Market 1 has 2 sellers, 2 associated with clique #1 and one associated with clique #3
     * Market 2 has 1 seller associated with cliques #1 and #2
     * => the common clique is clique #1
     * Market 3 has 1 seller not associated with cliques.
     */
    @Before
    public void setUp() {
        topology = new Topology();
        final Trader t1 = addTrader(TRADER_TYPE_A, BASKET_1, CLIQUE_1);
        final Trader t2 = addTrader(TRADER_TYPE_A, BASKET_1, CLIQUE_1);
        @SuppressWarnings("unused")
        final Trader t3 = addTrader(TRADER_TYPE_A, BASKET_1, CLIQUE_3);
        final Trader t4 = addTrader(TRADER_TYPE_B, BASKET_2, CLIQUE_1, CLIQUE_2);
        final Trader t5 = addTrader(TRADER_TYPE_C, BASKET_3);
        final Trader t6 = addTrader(TRADER_TYPE_C, BASKET_3).setState(TraderState.INACTIVE);
        buyer = addTrader(TRADER_TYPE_D, BASKET_4);
        topology.addBasketBought(3001, buyer, BASKET_1, oid(t1)).setMovable(true);
        topology.addBasketBought(3002, buyer, BASKET_2, oid(t4)).setMovable(true);
        topology.addBasketBought(3003, buyer, BASKET_3, oid(t5)).setMovable(true);
        economy = (Economy)topology.getEconomy();
        economy.populateMarketsWithSellersAndMergeConsumerCoverage();
        // The only common clique is #1, therefore sellers that are associated with clique #1
        // (t1, t2 and t4) are potential sellers, while trader t3 is not a potential seller.
        // Trader t5 is a potential seller because it is in a market with no cliques and it
        // is active. Similarly, t6 is a potential seller because it is inactive.
        expectedPotentialSellers = Sets.newHashSet(t1, t2, t4, t5, t6);
    }

    private static Trader addTrader(int sellerType, Basket basket, Long... cliques) {
        return topology
                .addTrader(oid++, sellerType, TraderState.ACTIVE, basket, Arrays.asList(cliques))
                .setDebugInfoNeverUseInCode("Trader#" + oid); // helps debugging
    }

    /**
     * Obtain the oid of a trader from the topology object.
     *
     * @param trader the trader which oid to obtain
     * @return the oid of the trader
     */
    private static long oid(Trader trader) {
        return trader.getOid();
    }

    private static int commType = 1000;

    /**
     * Every call to newBasket() returns a basket with a distinct commodity specification.
     *
     * @return a basket that is different from those generated in previous calls to this method
     */
    private static Basket newBasket() {
        return new Basket(new CommoditySpecification(commType++));
    }

    /**
     * Test the methods {@link Economy#getCommonCliques(Trader)}
     * and {@link Economy#getPotentialSellers(Trader)}.
     */
    @Test
    public void testPotentialSellers() {
        Set<Long> commonCliquesNonEmpty = economy.getCommonCliquesNonEmpty(buyer);
        assertEquals(1, commonCliquesNonEmpty.size());
        assertEquals(CLIQUE_1, commonCliquesNonEmpty.iterator().next());
        // The market for BASKET_3 has no cliques, so common cliques
        // for all markets must be empty.
        Set<Long> commonCliques = economy.getCommonCliques(buyer);
        assertTrue(commonCliques.isEmpty());
        Set<Trader> potentialSellers = economy.getPotentialSellers(buyer);
        assertEquals(expectedPotentialSellers, potentialSellers);
    }

}
