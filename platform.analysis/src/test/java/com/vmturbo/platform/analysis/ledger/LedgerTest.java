package com.vmturbo.platform.analysis.ledger;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.topology.LegacyTopology;
import com.vmturbo.platform.analysis.utility.ListTests;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * Tests for the {@link IncomeStatement} class.
 */
@RunWith(JUnitParamsRunner.class)
public class LedgerTest {
    // Fields

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU_ANY = new CommoditySpecification(0, 1, Integer.MAX_VALUE);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);
    private static final CommoditySpecification V_CPU = new CommoditySpecification(2);
    private static final CommoditySpecification V_MEM = new CommoditySpecification(3);
    private static final CommoditySpecification CLUSTER_A = new CommoditySpecification(4, 0, 0);

    // Baskets to use in tests
    private static final Basket PM_ANY = new Basket(CPU_ANY, MEM);
    private static final Basket PM_A = new Basket(CPU_ANY, MEM, CLUSTER_A);
    private static final Basket VM = new Basket(V_CPU, V_MEM);

    // Methods

    @Test
    public final void testLedger() {
        @NonNull String[] ids = {"id1","id2","id3"};
        @NonNull String[] names = {"name1", "name2", "name2"};
        @NonNull String[] tTypes = {"type1", "type2", "type2"};
        @NonNull TraderState[] states = {TraderState.ACTIVE,TraderState.INACTIVE,TraderState.ACTIVE};
        @NonNull String[][] cTypeGroups = {{"a","b","c"},{"d","e"},{"b","d","f"}};
        @NonNull Trader[] traders = new Trader[ids.length];

        @NonNull LegacyTopology topology = new LegacyTopology();
        for (int i = 0 ; i < ids.length ; ++i) {
            traders[i] = topology.addTrader(ids[i], names[i], tTypes[i], states[i], Arrays.asList(cTypeGroups[i]));
        }
        Economy economy = (Economy)topology.getEconomy();
        Ledger ledger = new Ledger(economy);

        assertTrue(ledger.getTraderIncomeStatements().size() == economy.getTraders().size());

        economy.getTraders().forEach(trader->{
            assertTrue(ledger.getCommodityIncomeStatements(trader).size() == trader.getCommoditiesSold().size());
        });

    }

    @Test
    public final void testGetUnmodifiableTraderIncomeStatements() {
        Economy economy = new Economy();
        economy.addTrader(1, TraderState.ACTIVE, new Basket(new CommoditySpecification(0,4,8)));
        Ledger ledger = new Ledger(economy);
        List<IncomeStatement> isList = ledger.getTraderIncomeStatements();
        ListTests.verifyUnmodifiableValidOperations(isList, new IncomeStatement());
        ListTests.verifyUnmodifiableInvalidOperations(isList, new IncomeStatement());

    }

    @Parameters
    @TestCaseName("Test #{index}: Ledger({0}).calculateCommExpensesAndRevenues() == {1}")
    public final void testCalculateExpensesAndRevenues(Basket bought, Basket sold) {
        Economy economy = new Economy();
        Trader supplier = economy.addTrader(0, TraderState.ACTIVE, sold); // u can create a trader only by adding it to the market

        ShoppingList consumerShoppingList = economy.addBasketBought(economy.addTrader(1, TraderState.ACTIVE, VM, bought), bought);
        consumerShoppingList.setQuantity(0, 5).setPeakQuantity(0, 7);
        consumerShoppingList.setQuantity(1, 5).setPeakQuantity(1, 7);

        Ledger ledger = new Ledger(economy);
        consumerShoppingList.move(supplier);
        // TODO: populate rawMaterialMap
        economy.getModifiableRawCommodityMap().put(new Long(0), new Long(5));
        economy.getModifiableRawCommodityMap().put(new Long(1), new Long(5));
        economy.getModifiableRawCommodityMap().put(new Long(2), new Long(0));
        economy.getModifiableRawCommodityMap().put(new Long(3), new Long(1));

        ledger.calculateAllTraderExpensesAndRevenues(economy);
        List<IncomeStatement> traderIncomeStmts = ledger.getTraderIncomeStatements();
        assertTrue(traderIncomeStmts.get(0).getRevenues() == traderIncomeStmts.get(1).getExpenses());

        economy.getTraders().forEach(trader->{
            assertTrue(ledger.getCommodityIncomeStatements(trader).size() == trader.getCommoditiesSold().size());
        });
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCalculateExpensesAndRevenues() {
        return new Object[][] {
            {PM_ANY, PM_ANY},
            {PM_ANY, PM_A},
        };
    }

} // end LedgerTest class
