package com.vmturbo.platform.analysis.ede;

import static com.vmturbo.platform.analysis.ede.Placement.mergeContextSets;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.utilities.PlacementResults;

/**
 * A test case for the {@link Placement} class.
 */
@RunWith(Enclosed.class)
public class PlacementTest {
    // Numerical constants for use in tests
    private static final int VM_TYPE = 0;
    private static final int PM_TYPE = 1;
    private static final int ST_TYPE = 2;
    private static final double CAPACITY = 111;
    private static final double UTILIZATION_UPPER_BOUND = 0.9;

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);
    private static final CommoditySpecification STA = new CommoditySpecification(2);
    private static final CommoditySpecification LAT = new CommoditySpecification(3);

    private static final CommoditySpecification SEGMENT_0 = new CommoditySpecification(4,1004);

    // Baskets to use in tests
    private static final Basket EMPTY = new Basket();
    private static final Basket PM_SMALL = new Basket(CPU,MEM);
    private static final Basket PM_LARGE = new Basket(CPU,MEM,SEGMENT_0);
    private static final Basket ST_SMALL = new Basket(STA,LAT);
    private static final Basket ST_LARGE = new Basket(STA,LAT,SEGMENT_0);
    private static final Basket STORAGE = new Basket(STA);

    // Frequently used parameter combinations for tests.
    // ('PM' for Physical Machine | 'ST' for STorage)_('S' for Small | 'L' for Large)
    private static final Object[] PM_S = {PM_SMALL,PM_TYPE};
    private static final Object[] PM_L = {PM_LARGE,PM_TYPE};
    private static final Object[] ST_L = {ST_LARGE,ST_TYPE};
    private static final Object[] ST_S = {ST_SMALL,ST_TYPE};
    // 'L' for Low, 'M' for Medium, 'H' for High
    private static final Object[] LL = { 20.0, 20.0};
    private static final Object[] LM = { 20.0, 50.0};
    private static final Object[] LH = { 20.0,110.0};
    private static final Object[] ML = { 50.0, 20.0};
    private static final Object[] MM = { 50.0, 50.0};
    private static final Object[] MH = { 50.0,110.0};
    private static final Object[] HL = {110.0, 20.0};
    private static final Object[] HM = {110.0, 50.0};
    private static final Object[] HH = {110.0,110.0};

    private static final Object[] PM_S_M_P_LL = {PM_SMALL, true, 1, 20.0, 20.0};
    private static final Object[] PM_L_M_P_LL = {PM_LARGE, true, 1, 20.0, 20.0};
    private static final Object[] PM_L_M_P_LM = {PM_LARGE, true, 1, 20.0, 50.0};
    private static final Object[] PM_L_M_P_ML = {PM_LARGE, true, 1, 50.0, 20.0};
    private static final Object[] PM_L_M_P_MM = {PM_LARGE, true, 1, 50.0, 50.0};
    private static final Object[] PM_S_M_U_LL = {PM_SMALL, true, -1, 20.0, 20.0};
    private static final Object[] PM_L_M_U_LL = {PM_LARGE, true, -1, 20.0, 20.0};
    private static final Object[] PM_L_M_U_LM = {PM_LARGE, true, -1, 20.0, 50.0};
    private static final Object[] PM_L_M_U_ML = {PM_LARGE, true, -1, 50.0, 20.0};
    private static final Object[] PM_L_M_U_MM = {PM_LARGE, true, -1, 50.0, 50.0};

    // Arrays for use in tests
    private static final Action[] NO_ACTIONS = {};

    // Temporary, for shop together tests that generate placements
    // TODO btc - need to add this information to the test array, and also verify
    // the content of the reconfigure action.
    private static int testNumber = 0;
    public static final Set<Integer> testsExpectingReconfigure =
            new HashSet<>(Arrays.asList(1, 14, 15, 16, 17, 28, 29, 30, 42, 43, 44, 45, 56, 57, 58,
                    110, 111, 160, 161));

    @RunWith(Parameterized.class)
    public static class ShopTogetherPlacementDecisions {
        // Fields needed by parameterized runner
        @Parameter(value = 0) public @NonNull Economy economy;
        @Parameter(value = 1) public @NonNull Action @NonNull [] actions;

        @Test
        public final void testShopTogetherPlacementDecisions() {
            final Object[] actuals = Placement.shopTogetherDecisions(economy).getActions().toArray();
            if (testsExpectingReconfigure.contains(testNumber++)) {
                // Verify that there is a single Reconfigure at the end, and the rest of the
                // list matches
                boolean lastIsReconfigure = (actuals.length > 0) &&
                        (Arrays.copyOfRange(actuals, actuals.length - 1, actuals.length)[0]
                                instanceof Reconfigure);
                assertTrue("Reconfigure expected", lastIsReconfigure);
                assertArrayEquals(actions,
                        Arrays.copyOfRange(actuals, 0, actuals.length-1));
            } else {
                assertArrayEquals(actions, actuals);
            }
        }

        // TODO: add tests with inactive traders
        // TODO: add tests with partially immovable traders
        @Parameters(name = "Test #{index}: placementActions({0}) == {1}")
        public static Collection<Object[]> shopTogetherTestCases() {
            final List<@NonNull Object @NonNull []> output = new ArrayList<>();
            /*
             * This is a 5 level array of arrays:
             *
             * level 1: all test cases (size may vary)
             * level 2: one test case (size should be 3)
             *
             * level 3a: all buyers (size may vary)
             * level 4a: all shopping lists for one buyer (size may vary)
             * level 5a: the basket bought, whether it's movable and
             *           the economy index of the provider of this shopping list (negative means unplaced)
             *           followed buy the quantities bought for indices 0, 1,... (size should be >= 3)
             *
             * level 3b: all sellers (size may vary)
             * level 4b: one seller (size should be 4)
             * level 5b.1: the basket sold and trader type in that order (size should be 2)
             * level 5b.2: the quantities sold index to index (size may vary)
             * level 5b.3: the utilization upper bounds index to index. (size may vary)
             * level 5b.4: the bicliques sold by this trader (size may vary)
             *
             * level 3c: all actions (size may vary)
             * level 4c: one action (size should be 2)
             * level 5c.1: the economy index for the buyer that should move, followed by the indices
             *             of the its shopping lists that should move (size should be >= 1)
             *             you should be able to build the results with just the moves array and the economy.
             * level 5c.2: the economy indices for the sellers the buyer should move to (size may vary)
             *
             */
            final Object[][][][][] shopTogetherTestCases = {
                // TODO: also test error conditions like:
                // {{{{PM_SMALL, true, true, 20.0,20.0}}},{{PM_S,LL,{}}},{}},

                // 0 VMs, 0 PMs, 0 STs, 0 edges
                {{},{},{}},
                // 1 VM, 0 PMs, 0 STs, 1 edge
                {{{{PM_SMALL,true ,-1}}},{},{}}, // movable
/* PBD? RCF? */ {{{{PM_SMALL,false,-1}}},{},{}}, // immovable
                // 0 VMs, 1 PM, 0 STs, 0 edges
   /* error? */ {{},{{PM_S,{},{},{}}},{}}, // no bicliques
                {{},{{PM_S,{},{},{0L}}},{}}, // empty host
                {{},{{PM_S,{10.0,10.0},{},{0L}}},{}}, // empty host with overhead
                // 1 VM, 1 PM, 0 STs, 1 edge
                // movable and initially placed
                // baskets match
                {{{PM_S_M_P_LL}},{{PM_S,LL,{},{0L}}},{}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, 1, 90.0,20.0}}},{{PM_S,HL,{},{0L}}},{}}, // can't fit CPU
     /* PBD? */ {{{{PM_SMALL, true, 1,105.0,20.0}}},{{PM_S,HL,{},{0L}}},{}}, // not enough capacity for CPU
     /* PBD? */ {{{{PM_SMALL, true, 1,105.0,20.0}}},{{PM_S,ML,{},{0L}}},{}}, // not enough capacity for CPU but host seems fine!
                // basket subset
                {{{PM_S_M_P_LL}},{{PM_L,LL,{},{0L}}},{}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, 1, 20.0,90.0}}},{{PM_L,LH,{},{0L}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_SMALL, true, 1,105.0,90.0}}},{{PM_L,HH,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU
/* PBD !PBS? */ {{{{PM_SMALL, true, 1,105.0,90.0}}},{{PM_L,MH,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU, but host seems fine for CPU!
                // basket superset
     /* RCF? */ {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0L}}},{}}, // fits
/* RCF !PBS? */ {{{{PM_LARGE, true, 1, 90.0,20.0}}},{{PM_S,HL,{},{0L}}},{}}, // can't fit CPU
/* RCF !PBD? */ {{{{PM_LARGE, true, 1,105.0,20.0}}},{{PM_S,HL,{},{0L}}},{}}, // not enough capacity for CPU
/* RCF !PBD? */ {{{{PM_LARGE, true, 1,105.0,20.0}}},{{PM_S,ML,{},{0L}}},{}}, // not enough capacity for CPU but host seems fine!
                // baskets match (but are bigger)
                {{{PM_L_M_P_LL}},{{PM_L,LL,{},{0L}}},{}}, // fits
     /* PBS? */ {{{{PM_LARGE, true, 1, 20.0,90.0}}},{{PM_L,LH,{},{0L}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_LARGE, true, 1,105.0,90.0}}},{{PM_L,HH,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU
/* PBD !PBS? */ {{{{PM_LARGE, true, 1,105.0,90.0}}},{{PM_L,MH,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU, but host seems fine for CPU!
                // movable and initially unplaced
                // baskets match
                {{{{PM_SMALL, true, -1, 20.0,20.0}}},{{PM_S,LL,{},{0L}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, -1, 90.0,20.0}}},{{PM_S,LL,{},{0L}}},{}}, // can't fit CPU
     /* PBD? */ {{{{PM_SMALL, true, -1,105.0,20.0}}},{{PM_S,LL,{},{0L}}},{}}, // not enough capacity for CPU
                // basket subset
                {{{{PM_SMALL, true, -1, 20.0,20.0}}},{{PM_L,LL,{},{0L}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, -1, 20.0,90.0}}},{{PM_L,LL,{},{0L}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_SMALL, true, -1,105.0,90.0}}},{{PM_L,LL,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU
                // basket superset
     /* RCF? */ {{{{PM_LARGE, true, -1, 20.0,20.0}}},{{PM_S,LL,{},{0L}}},{}}, // fits
/* RCF !PBS? */ {{{{PM_LARGE, true, -1, 90.0,20.0}}},{{PM_S,LL,{},{0L}}},{}}, // can't fit CPU
/* RCF !PBD? */ {{{{PM_LARGE, true, -1,105.0,20.0}}},{{PM_S,LL,{},{0L}}},{}}, // not enough capacity for CPU
                // baskets match (but are bigger)
                {{{{PM_LARGE, true, -1, 20.0,20.0}}},{{PM_L,LL,{},{0L}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_LARGE, true, -1, 20.0,90.0}}},{{PM_L,LL,{},{0L}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_LARGE, true, -1,105.0,90.0}}},{{PM_L,LL,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU

                // 1 VM, 1 PM, 1 ST, 1 edge
                // (Same as above. Just checking that the results won't be changed by additional trader)
                // movable and initially placed
                // baskets match
                {{{PM_S_M_P_LL}},{{PM_S,LL,{},{0L}},{ST_S,LL,{},{0L}}},{}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, 1, 90.0,20.0}}},{{PM_S,HL,{},{0L}},{ST_S,ML,{},{0L}}},{}}, // can't fit CPU
     /* PBD? */ {{{{PM_SMALL, true, 1,105.0,20.0}}},{{PM_S,HL,{},{0L}},{ST_S,LL,{},{1L}}},{}}, // not enough capacity for CPU
     /* PBD? */ {{{{PM_SMALL, true, 1,105.0,20.0}}},{{PM_S,ML,{},{0L}},{ST_S,HM,{},{0L}}},{}}, // not enough capacity for CPU but host seems fine!
                // basket subset
                {{{PM_S_M_P_LL}},{{PM_L,LL,{},{0L}},{ST_L,LL,{},{0L}}},{}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, 1, 20.0,90.0}}},{{PM_L,LH,{},{0L}},{ST_S,ML,{},{1L}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_SMALL, true, 1,105.0,90.0}}},{{PM_L,HH,{},{0L}},{ST_S,LL,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU
/* PBD !PBS? */ {{{{PM_SMALL, true, 1,105.0,90.0}}},{{PM_L,MH,{},{0L}},{ST_S,HM,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU, but host seems fine for CPU!
                // basket superset
     /* RCF? */ {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0L}},{ST_S,LL,{},{1L}}},{}}, // fits
/* RCF !PBS? */ {{{{PM_LARGE, true, 1, 90.0,20.0}}},{{PM_S,HL,{},{0L}},{ST_L,ML,{},{0L}}},{}}, // can't fit CPU
/* RCF !PBD? */ {{{{PM_LARGE, true, 1,105.0,20.0}}},{{PM_S,HL,{},{0L}},{ST_S,LL,{},{0L}}},{}}, // not enough capacity for CPU
/* RCF !PBD? */ {{{{PM_LARGE, true, 1,105.0,20.0}}},{{PM_S,ML,{},{0L}},{ST_S,HM,{},{1L}}},{}}, // not enough capacity for CPU but host seems fine!
                // baskets match (but are bigger)
                {{{PM_L_M_P_LL}},{{PM_L,LL,{},{0L}},{ST_S,LL,{},{0L}}},{}}, // fits
     /* PBS? */ {{{{PM_LARGE, true, 1, 20.0,90.0}}},{{PM_L,LH,{},{0L}},{ST_S,ML,{},{0L}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_LARGE, true, 1,105.0,90.0}}},{{PM_L,HH,{},{0L}},{ST_L,LL,{},{1L}}},{}}, // can't fit MEM & not enough capacity for CPU
/* PBD !PBS? */ {{{{PM_LARGE, true, 1,105.0,90.0}}},{{PM_L,MH,{},{0L}},{ST_S,HM,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU, but host seems fine for CPU!
                // movable and initially unplaced
                // baskets match
                {{{{PM_SMALL, true,-1, 20.0,20.0}}},{{PM_S,LL,{},{0L}},{ST_S,LL,{},{0L}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_SMALL, true,-1, 90.0,20.0}}},{{PM_S,LL,{},{0L}},{ST_S,ML,{},{1L}}},{}}, // can't fit CPU
     /* PBD? */ {{{{PM_SMALL, true,-1,105.0,20.0}}},{{PM_S,LL,{},{0L}},{ST_S,LL,{},{0L}}},{}}, // not enough capacity for CPU
                // basket subset
                {{{{PM_SMALL, true,-1, 20.0,20.0}}},{{PM_L,LL,{},{0L}},{ST_L,HM,{},{0L}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_SMALL, true,-1, 20.0,90.0}}},{{PM_L,LL,{},{0L}},{ST_S,LL,{},{1L}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_SMALL, true,-1,105.0,90.0}}},{{PM_L,LL,{},{0L}},{ST_S,ML,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU
                // basket superset
     /* RCF? */ {{{{PM_LARGE, true,-1, 20.0,20.0}}},{{PM_S,LL,{},{0L}},{ST_S,LL,{},{0L}}},{}}, // fits
/* RCF !PBS? */ {{{{PM_LARGE, true,-1, 90.0,20.0}}},{{PM_S,LL,{},{0L}},{ST_S,HM,{},{1L}}},{}}, // can't fit CPU
/* RCF !PBD? */ {{{{PM_LARGE, true,-1,105.0,20.0}}},{{PM_S,LL,{},{0L}},{ST_L,LL,{},{0L}}},{}}, // not enough capacity for CPU
                // baskets match (but are bigger)
                {{{{PM_LARGE, true,-1, 20.0,20.0}}},{{PM_L,LL,{},{0L}},{ST_S,ML,{},{0L}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_LARGE, true,-1, 20.0,90.0}}},{{PM_L,LL,{},{0L}},{ST_S,LL,{},{1L}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_LARGE, true,-1,105.0,90.0}}},{{PM_L,LL,{},{0L}},{ST_S,HM,{},{0L}}},{}}, // can't fit MEM & not enough capacity for CPU

                // 1 VM, 2 PMs, 0 STs, 1 edge
                // movable and initially placed
                // 1st fits
                // 2nd fits (moves to distribute load)
                {{{PM_S_M_P_LL}},{{PM_S,LL,{},{0L}},{PM_S,MM,{},{0L}}},{}}, // best CPU,MEM, same biclique
                {{{PM_S_M_P_LL}},{{PM_S,LL,{},{0L}},{PM_S,MM,{},{1L}}},{}}, // best CPU,MEM, different biclique
                {{{PM_S_M_P_LL}},{{PM_S,MM,{},{0L}},{PM_S,LL,{},{0L}}},{{{0,0},{2}}}}, // improve CPU,MEM, same biclique
                {{{PM_S_M_P_LL}},{{PM_S,MM,{},{0L}},{PM_S,LL,{},{1L}}},{{{0,0},{2}}}}, // improve CPU,MEM, different biclique
                {{{{PM_SMALL, true, 1, 10.0,20.0}}},{{PM_S,{60.0,40.0},{},{0L}},{PM_S,LM,{},{0L}}},{}}, // MEM driven best, same biclique
                {{{{PM_SMALL, true, 1, 10.0,20.0}}},{{PM_S,{60.0,40.0},{},{0L}},{PM_S,LM,{},{1L}}},{}}, // MEM driven best, different biclique
                {{{{PM_SMALL, true, 1, 20.0,10.0}}},{{PM_S,{70.0,30.0},{},{0L}},{PM_S,LM,{},{0L}}},{{{0,0},{2}}}}, // CPU driven improve, same biclique
                {{{{PM_SMALL, true, 1, 20.0,10.0}}},{{PM_S,{70.0,30.0},{},{0L}},{PM_S,LM,{},{1L}}},{{{0,0},{2}}}}, // CPU driven improve, different biclique
                // 2nd doesn't have enough leftover effective capacity
                {{{PM_L_M_P_LM}},{{PM_L,MM,{},{0L}},{PM_L,LM,{},{0L}}},{}}, // same biclique
                {{{PM_L_M_P_LM}},{{PM_L,MM,{},{0L}},{PM_L,LM,{},{1L}}},{}}, // different biclique
                // 2nd doens't have enough effective capacity
                {{{PM_L_M_P_ML}},{{PM_L,ML,{},{0L}},{PM_L,LL,{0.4},{0L}}},{}}, // same biclique
                {{{PM_L_M_P_ML}},{{PM_L,ML,{},{0L}},{PM_L,LL,{0.4},{1L}}},{}}, // different biclique
                // 2nd doesn't sell all required commodities
                {{{PM_L_M_P_LL}},{{PM_L,MM,{},{0L}},{PM_S,{},{},{0L}}},{}}, // cheaper, same biclique
                {{{PM_L_M_P_LL}},{{PM_L,MM,{},{0L}},{PM_S,{},{},{1L}}},{}}, // cheaper, different biclique
                {{{PM_L_M_P_LL}},{{PM_L,MM,{},{0L}},{PM_S,MM,{},{0L}}},{}}, // more expensive, same biclique
                {{{PM_L_M_P_LL}},{{PM_L,MM,{},{0L}},{PM_S,MM,{},{1L}}},{}}, // more expensive, different biclique
                // 1st doesn't have enough leftover effective capacity
                // 2nd fits
                {{{PM_L_M_P_LM}},{{PM_L,LH,{},{0L}},{PM_L,ML,{},{0L}}},{{{0,0},{2}}}}, // same biclique
                {{{PM_L_M_P_LM}},{{PM_L,LH,{},{0L}},{PM_L,ML,{},{1L}}},{{{0,0},{2}}}}, // different biclique
                // 2nd doesn't have enough leftover effective capacity
     /* PBS? */ {{{PM_L_M_P_LM}},{{PM_L,LH,{},{0L}},{PM_L,LM,{},{0L}}},{}}, // MEM both, same biclique
     /* PBS? */ {{{PM_L_M_P_LM}},{{PM_L,LH,{},{0L}},{PM_L,LM,{},{1L}}},{}}, // MEM both, different biclique
     /* PBS? */ {{{PM_L_M_P_MM}},{{PM_L,MH,{},{0L}},{PM_L,ML,{},{0L}}},{}}, // one MEM one CPU, same biclique
     /* PBS? */ {{{PM_L_M_P_MM}},{{PM_L,MH,{},{0L}},{PM_L,ML,{},{1L}}},{}}, // one MEM one CPU, different biclique
                // 2nd doesn't have enough effective capacity
/* PBS !PBD? */ {{{PM_L_M_P_ML}},{{PM_L,HL,{},{0L}},{PM_L,{},{0.4},{0L}}},{}}, // CPU both, same biclique
/* PBS !PBD? */ {{{PM_L_M_P_ML}},{{PM_L,HL,{},{0L}},{PM_L,{},{0.4},{1L}}},{}}, // CPU both, different biclique
/* PBS !PBD? */ {{{PM_L_M_P_MM}},{{PM_L,HL,{},{0L}},{PM_L,{},{1.0,0.4},{0L}}},{}}, // one CPU one MEM, same biclique
/* PBS !PBD? */ {{{PM_L_M_P_MM}},{{PM_L,HL,{},{0L}},{PM_L,{},{1.0,0.4},{1L}}},{}}, // one CPU one MEM, different biclique
                // 2nd doesn't sell all required commodities
/* PBS !RCF? */ {{{PM_L_M_P_ML}},{{PM_L,HM,{},{0L}},{PM_S,LL,{},{0L}}},{}}, // cheaper, same biclique
/* PBS !RCF? */ {{{PM_L_M_P_ML}},{{PM_L,HM,{},{0L}},{PM_S,LL,{},{1L}}},{}}, // cheaper, different biclique
                // 1st doesn't have enough effective capacity
                // 2nd fits
                {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0L}},{PM_L,LL,{},{0L}}},{{{0,0},{2}}}}, // same biclique
                {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0L}},{PM_L,LL,{},{1L}}},{{{0,0},{2}}}}, // different biclique
                // 2nd doesn't have enough leftover effective capacity
/* PBS !PBD? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0L}},{PM_L,MM,{},{0L}}},{}}, // CPU both, same biclique
/* PBS !PBD? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0L}},{PM_L,MM,{},{1L}}},{}}, // CPU both, different biclique
/* PBS !PBD? */ {{{PM_L_M_P_MM}},{{PM_L,ML,{0.4},{0L}},{PM_L,LM,{},{0L}}},{}}, // one CPU one MEM, same biclique
/* PBS !PBD? */ {{{PM_L_M_P_MM}},{{PM_L,ML,{0.4},{0L}},{PM_L,LM,{},{1L}}},{}}, // one CPU one MEM, different biclique
                // 2nd doesn't have enough effective capacity
     /* PBD? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0L}},{PM_L,{},{0.4},{0L}}},{}}, // CPU both, same biclique
     /* PBD? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0L}},{PM_L,{},{0.4},{1L}}},{}}, // CPU both, different biclique
     /* PBD? */ {{{PM_L_M_P_MM}},{{PM_L,ML,{0.4},{0L}},{PM_L,{},{1.0,0.4},{0L}}},{}}, // one CPU one MEM, same biclique
     /* PBD? */ {{{PM_L_M_P_MM}},{{PM_L,ML,{0.4},{0L}},{PM_L,{},{1.0,0.4},{1L}}},{}}, // one CPU one MEM, different biclique
                // 2nd doesn't sell all required commodities
/* PBD !RCF? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0L}},{PM_S,{},{},{0L}}},{}}, // cheaper, same biclique
/* PBD !RCF? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0L}},{PM_S,{},{},{1L}}},{}}, // cheaper, different biclique
                // 1st doesn't sell all required commodities
                // 2nd fits
                {{{PM_L_M_P_LL}},{{PM_S,MM,{},{0L}},{PM_L,{},{},{0L}}},{{{0,0},{2}}}}, // cheaper, same biclique
                {{{PM_L_M_P_LL}},{{PM_S,MM,{},{0L}},{PM_L,{},{},{1L}}},{{{0,0},{2}}}}, // cheaper, different biclique
                {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0L}},{PM_L,MM,{},{0L}}},{{{0,0},{2}}}}, // more expensive, same biclique
                {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0L}},{PM_L,MM,{},{1L}}},{{{0,0},{2}}}}, // more expensive, different biclique
                // 2nd doesn't have enough leftover effective capacity
/* PBS !RCF? */ {{{PM_L_M_P_LM}},{{PM_S,LM,{},{0L}},{PM_L,MM,{},{0L}}},{}}, // MEM, same biclique
/* PBS !RCF? */ {{{PM_L_M_P_LM}},{{PM_S,LM,{},{0L}},{PM_L,MM,{},{1L}}},{}}, // MEM, different biclique
                // 2nd doesn't have enough effective capacity
/* PBD !RCF? */ {{{PM_L_M_P_LM}},{{PM_S,LM,{},{0L}},{PM_L,{},{1.0,0.4},{0L}}},{}}, // MEM, same biclique
/* PBD !RCF? */ {{{PM_L_M_P_LM}},{{PM_S,LM,{},{0L}},{PM_L,{},{1.0,0.4},{1L}}},{}}, // MEM, different biclique
                // 2nd doesn't sell all required commodities
     /* RCF? */ {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0L}},{PM_S,LL,{},{0L}}},{}}, // same biclique
     /* RCF? */ {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0L}},{PM_S,LL,{},{1L}}},{}}, // different biclique
                // movable and initially unplaced
                // 1st fits
                // 2nd fits (moves to distribute load)
                {{{PM_S_M_U_LL}},{{PM_S,LL,{},{0L}},{PM_S,MM,{},{0L}}},{{{0,0},{1}}}}, // 1st is best CPU,MEM, same biclique
                {{{PM_S_M_U_LL}},{{PM_S,LL,{},{0L}},{PM_S,MM,{},{1L}}},{{{0,0},{1}}}}, // 1st is best CPU,MEM, different biclique
                {{{PM_S_M_U_LL}},{{PM_S,MM,{},{0L}},{PM_S,LL,{},{0L}}},{{{0,0},{2}}}}, // 2nd is best CPU,MEM, same biclique
                {{{PM_S_M_U_LL}},{{PM_S,MM,{},{0L}},{PM_S,LL,{},{1L}}},{{{0,0},{2}}}}, // 2nd is best CPU,MEM, different biclique
                {{{{PM_SMALL, true,-1, 10.0,20.0}}},{{PM_S,ML,{},{0L}},{PM_S,LM,{},{0L}}},{{{0,0},{1}}}}, // MEM driven 1st best, same biclique
                {{{{PM_SMALL, true,-1, 10.0,20.0}}},{{PM_S,ML,{},{0L}},{PM_S,LM,{},{1L}}},{{{0,0},{1}}}}, // MEM driven 1st best, different biclique
                {{{{PM_SMALL, true,-1, 20.0,10.0}}},{{PM_S,ML,{},{0L}},{PM_S,LM,{},{0L}}},{{{0,0},{2}}}}, // CPU driven 2nd best, same biclique
                {{{{PM_SMALL, true,-1, 20.0,10.0}}},{{PM_S,ML,{},{0L}},{PM_S,LM,{},{1L}}},{{{0,0},{2}}}}, // CPU driven 2nd best, different biclique
                // 2nd doesn't have enough leftover effective capacity
                {{{PM_L_M_U_LM}},{{PM_L,LL,{},{0L}},{PM_L,LM,{},{0L}}},{{{0,0},{1}}}}, // same biclique
                {{{PM_L_M_U_LM}},{{PM_L,LL,{},{0L}},{PM_L,LM,{},{1L}}},{{{0,0},{1}}}}, // different biclique
                // 2nd doens't have enough effective capacity
                {{{PM_L_M_U_ML}},{{PM_L,LL,{},{0L}},{PM_L,LL,{0.4},{0L}}},{{{0,0},{1}}}}, // same biclique
                {{{PM_L_M_U_ML}},{{PM_L,LL,{},{0L}},{PM_L,LL,{0.4},{1L}}},{{{0,0},{1}}}}, // different biclique
                // 2nd doesn't sell all required commodities
                {{{PM_L_M_U_LL}},{{PM_L,LL,{},{0L}},{PM_S,{},{},{0L}}},{{{0,0},{1}}}}, // cheaper, same biclique
                {{{PM_L_M_U_LL}},{{PM_L,LL,{},{0L}},{PM_S,{},{},{1L}}},{{{0,0},{1}}}}, // cheaper, different biclique
                {{{PM_L_M_U_LL}},{{PM_L,LL,{},{0L}},{PM_S,MM,{},{0L}}},{{{0,0},{1}}}}, // more expensive, same biclique
                {{{PM_L_M_U_LL}},{{PM_L,LL,{},{0L}},{PM_S,MM,{},{1L}}},{{{0,0},{1}}}}, // more expensive, different biclique
                // 1st doesn't have enough leftover effective capacity
                // 2nd fits
                {{{PM_L_M_U_LM}},{{PM_L,LM,{},{0L}},{PM_L,ML,{},{0L}}},{{{0,0},{2}}}}, // same biclique
                {{{PM_L_M_U_LM}},{{PM_L,LM,{},{0L}},{PM_L,ML,{},{1L}}},{{{0,0},{2}}}}, // different biclique
                // 2nd doesn't have enough leftover effective capacity
     /* PBS? */ {{{PM_L_M_U_LM}},{{PM_L,LM,{},{0L}},{PM_L,LM,{},{0L}}},{}}, // MEM both, same biclique
     /* PBS? */ {{{PM_L_M_U_LM}},{{PM_L,LM,{},{0L}},{PM_L,LM,{},{1L}}},{}}, // MEM both, different biclique
     /* PBS? */ {{{PM_L_M_U_MM}},{{PM_L,LM,{},{0L}},{PM_L,ML,{},{0L}}},{}}, // one MEM one CPU, same biclique
     /* PBS? */ {{{PM_L_M_U_MM}},{{PM_L,LM,{},{0L}},{PM_L,ML,{},{1L}}},{}}, // one MEM one CPU, different biclique
                // 2nd doesn't have enough effective capacity
/* PBS !PBD? */ {{{PM_L_M_U_ML}},{{PM_L,ML,{},{0L}},{PM_L,{},{0.4},{0L}}},{}}, // CPU both, same biclique
/* PBS !PBD? */ {{{PM_L_M_U_ML}},{{PM_L,ML,{},{0L}},{PM_L,{},{0.4},{1L}}},{}}, // CPU both, different biclique
/* PBS !PBD? */ {{{PM_L_M_U_MM}},{{PM_L,ML,{},{0L}},{PM_L,{},{1.0,0.4},{0L}}},{}}, // one CPU one MEM, same biclique
/* PBS !PBD? */ {{{PM_L_M_U_MM}},{{PM_L,ML,{},{0L}},{PM_L,{},{1.0,0.4},{1L}}},{}}, // one CPU one MEM, different biclique
                // 2nd doesn't sell all required commodities
/* PBS !RCF? */ {{{PM_L_M_U_ML}},{{PM_L,MM,{},{0L}},{PM_S,LL,{},{0L}}},{}}, // cheaper, same biclique
/* PBS !RCF? */ {{{PM_L_M_U_ML}},{{PM_L,MM,{},{0L}},{PM_S,LL,{},{1L}}},{}}, // cheaper, different biclique
                // 1st doesn't have enough effective capacity
                // 2nd fits
                {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0L}},{PM_L,LL,{},{0L}}},{{{0,0},{2}}}}, // same biclique
                {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0L}},{PM_L,LL,{},{1L}}},{{{0,0},{2}}}}, // different biclique
                // 2nd doesn't have enough leftover effective capacity
/* PBS !PBD? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0L}},{PM_L,MM,{},{0L}}},{}}, // CPU both, same biclique
/* PBS !PBD? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0L}},{PM_L,MM,{},{1L}}},{}}, // CPU both, different biclique
/* PBS !PBD? */ {{{PM_L_M_U_MM}},{{PM_L,LL,{0.4},{0L}},{PM_L,LM,{},{0L}}},{}}, // one CPU one MEM, same biclique
/* PBS !PBD? */ {{{PM_L_M_U_MM}},{{PM_L,LL,{0.4},{0L}},{PM_L,LM,{},{1L}}},{}}, // one CPU one MEM, different biclique
                // 2nd doesn't have enough effective capacity
     /* PBD? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0L}},{PM_L,{},{0.4},{0L}}},{}}, // CPU both, same biclique
     /* PBD? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0L}},{PM_L,{},{0.4},{1L}}},{}}, // CPU both, different biclique
     /* PBD? */ {{{PM_L_M_U_MM}},{{PM_L,LL,{0.4},{0L}},{PM_L,{},{1.0,0.4},{0L}}},{}}, // one CPU one MEM, same biclique
     /* PBD? */ {{{PM_L_M_U_MM}},{{PM_L,LL,{0.4},{0L}},{PM_L,{},{1.0,0.4},{1L}}},{}}, // one CPU one MEM, different biclique
                // 2nd doesn't sell all required commodities
/* PBD !RCF? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0L}},{PM_S,{},{},{0L}}},{}}, // cheaper, same biclique
/* PBD !RCF? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0L}},{PM_S,{},{},{1L}}},{}}, // cheaper, different biclique
                // 1st doesn't sell all required commodities
                // 2nd fits
                {{{PM_L_M_U_LL}},{{PM_S,MM,{},{0L}},{PM_L,{},{},{0L}}},{{{0,0},{2}}}}, // cheaper, same biclique
                {{{PM_L_M_U_LL}},{{PM_S,MM,{},{0L}},{PM_L,{},{},{1L}}},{{{0,0},{2}}}}, // cheaper, different biclique
                {{{PM_L_M_U_LL}},{{PM_S,LL,{},{0L}},{PM_L,MM,{},{0L}}},{{{0,0},{2}}}}, // more expensive, same biclique
                {{{PM_L_M_U_LL}},{{PM_S,LL,{},{0L}},{PM_L,MM,{},{1L}}},{{{0,0},{2}}}}, // more expensive, different biclique
                // 2nd doesn't have enough leftover effective capacity
/* PBS !RCF? */ {{{PM_L_M_U_LM}},{{PM_S,LL,{},{0L}},{PM_L,MM,{},{0L}}},{}}, // MEM, same biclique
/* PBS !RCF? */ {{{PM_L_M_U_LM}},{{PM_S,LL,{},{0L}},{PM_L,MM,{},{1L}}},{}}, // MEM, different biclique
                // 2nd doesn't have enough effective capacity
/* PBD !RCF? */ {{{PM_L_M_U_LM}},{{PM_S,LL,{},{0L}},{PM_L,{},{1.0,0.4},{0L}}},{}}, // MEM, same biclique
/* PBD !RCF? */ {{{PM_L_M_U_LM}},{{PM_S,LL,{},{0L}},{PM_L,{},{1.0,0.4},{1L}}},{}}, // MEM, different biclique
                // 2nd doesn't sell all required commodities
     /* RCF? */ {{{PM_L_M_U_LL}},{{PM_S,LL,{},{0L}},{PM_S,LL,{},{0L}}},{}}, // same biclique
     /* RCF? */ {{{PM_L_M_U_LL}},{{PM_S,LL,{},{0L}},{PM_S,LL,{},{1L}}},{}}, // different biclique

                // 1 VM, 1 PM, 1 ST, 2 edges

                // 1 VM, 2 PM, 2 ST, 2 edges
                {{{{PM_SMALL,true,1,1.0,1.0},{ST_SMALL,true,2,1.0,1.0}}}, // 1 compoundMove
                 {{PM_S,{10.0,10.0},{},{0L}},{ST_S,{10.0,10.0},{},{0L}},
                  {PM_S,{ 0.0, 0.0},{},{1L}},{ST_S,{ 0.0, 0.0},{},{0L,1L}}},
                 {{{0,0,1},{3,4}}}},
                {{{{PM_SMALL,true,1,1.0,1.0},{ST_SMALL,true,2,1.0,1.0}}}, // 2 moves
                 {{PM_S,{10.0,10.0},{},{0L}},{ST_S,{10.0,10.0},{},{0L}},
                  {PM_S,{ 0.0, 0.0},{},{0L}},{ST_S,{ 0.0, 0.0},{},{0L}}},
                 {{{0,0},{3}},{{0,1},{4}}}},

                // 1 VM, 3 PMs, 3 STs, 2 edges
                {{{{PM_SMALL,true ,1,1.0,1.0},{ST_SMALL,true ,2,1.0,1.0}}}, // best share-nothing move. M1 would select 3,4 instead
                 {{PM_S,{10.0,10.0},{},{0L}},{ST_S,{ 1.0, 1.0},{},{0L}},
                  {PM_S,{ 1.0, 1.0},{},{1L}},{ST_S,{12.0,12.0},{},{1L}},
                  {PM_S,{ 4.0, 4.0},{},{2L}},{ST_S,{ 3.0, 3.0},{},{2L}}},
                 {{{0,0,1},{5,6}}}},
                {{{{PM_SMALL,true ,1,1.0,1.0},{ST_SMALL,true ,2,1.0,1.0}}}, // best share-nothing move with overlap.
                 {{PM_S,{10.0,10.0},{},{0L}},{ST_S,{ 1.0, 1.0},{},{0L}},
                  {PM_S,{ 1.0, 1.0},{},{0L}},{ST_S,{12.0,12.0},{},{0L,1L}},
                  {PM_S,{ 4.0, 4.0},{},{1L}},{ST_S,{ 3.0, 3.0},{},{1L}}},
                 {{{0,0,1},{3,2}}}},
                {{{{PM_SMALL,true ,1,1.0,1.0},{ST_SMALL,true ,2,1.0,1.0}}}, // best share-nothing move with overlap.
                 {{PM_S,{10.0,10.0},{},{0L}},{ST_S,{ 1.0, 1.0},{},{0L}},
                  {PM_S,{ 4.0, 4.0},{},{0L}},{ST_S,{ 3.0, 3.0},{},{0L,1L}},
                  {PM_S,{ 1.0, 1.0},{},{1L}},{ST_S,{ 6.0, 6.0},{},{1L}}},
                 {{{0,0,1},{5,4}}}},
                {{{{PM_SMALL,true ,1,1.0,1.0},{ST_SMALL,false,2,1.0,1.0}}}, // one shopping list is immovable.
                 {{PM_S,{10.0,10.0},{},{0L}},{ST_S,{ 7.0, 7.0},{},{0L}},
                  {PM_S,{ 4.0, 4.0},{},{0L}},{ST_S,{ 3.0, 3.0},{},{0L,1L}},
                  {PM_S,{ 1.0, 1.0},{},{1L}},{ST_S,{ 6.0, 6.0},{},{1L}}},
                 {{{0,0},{3}}}},
                // We should keep all the storage shopping lists first, and then the PM shopping
                // list (conforms with how XL sorts the shopping lists). And this especially matters when
                // there are more than 2 shopping lists, which is the case in the below 2 test cases.
                // This is because, we ignore simulation (QuoteSummer#simulate) for the last 2 SLs
                // of a trader - the last storage SL and the PM SL.
                {{{{ST_SMALL, true, 2, 1.0, 1.0}, {ST_SMALL, true, 3, 1.0, 1.0}, {PM_SMALL, true, 1, 1.0, 1.0}}}, // 1 compoundMove
                 {{PM_S, {10.0, 10.0}, {}, {0L}}, {ST_S, {10.0, 10.0}, {}, {0L}},
                  {ST_S, {10.0, 10.0}, {}, {0L, 1L}}, {PM_S, {0.0, 0.0}, {}, {1L}},
                  {ST_S, {0.0, 0.0}, {}, {0L, 1L}}, {ST_S, {0.0, 0.0}, {}, {1L}}},
                 {{{0, 2, 0, 1}, {4, 5, 6}}}},
                {{{{ST_SMALL, true, 2, 1.0, 1.0}, {ST_SMALL, true, 3, 1.0, 1.0}, {PM_SMALL, true, 1, 1.0, 1.0}}},  // 1 move,  1 compoundMove
                    {{PM_S, {10.0, 10.0}, {}, {0L}}, {ST_S, {10.0, 10.0}, {}, {0L}},
                        {ST_S, {10.0, 10.0}, {}, {0L, 1L}}, {PM_S, { 0.0,  0.0}, {}, {1L}},
                        {ST_S, { 0.0,  0.0}, {}, {1L}}, {ST_S, { 0.0,  0.0}, {}, {0L, 1L}}},
                    {{{0, 1}, {6}}, {{0, 0, 2}, {5, 4}}}},

                // 1 VM, 1 PM, 2 STs
                {
                    {
                        {
                            {PM_SMALL, true, 1, 40.0, 20.0},
                            {ST_SMALL, true, 2, 40.0, 40.0},
                            {ST_SMALL, true, 2, 50.0, 50.0},
                            {ST_SMALL, true, 3, 20.0, 20.0}
                        }
                    },
                    {
                        {PM_S, {70.0, 60.0}, {}, {0L}},
                        {ST_S, {90.0, 90.0}, {}, {0L}},
                        {ST_S, {20.0, 20.0}, {}, {0L}},
                    },
                    {
                        {{0,1},{3}}
                    }
                }
            };

            // Convert the multidimensional array to a list of test cases.
            for (Object[][][][] parameters : shopTogetherTestCases) {
                Economy economy = createEconomy(parameters[0], parameters[1]);
                output.add(new Object[] {economy, expectedShopTogetherOutput(economy, parameters[2])});
            }

            // Add any other test cases not expressible using the testCase method.

            // 1 VM, 0 PMs, 0 STs, 0 edges
            Economy economy = new Economy();
            economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
            economy.populateMarketsWithSellersAndMergeConsumerCoverage();
            output.add(new Object[]{economy, NO_ACTIONS});

            return output;
        }

        private static Action[] expectedShopTogetherOutput(Economy e, Object[][][] moves) {
            // Construct results
            Action[] results = new Action[moves.length];
            for (int i = 0 ; i < moves.length ; ++i) {
                // Find the subset of shopping lists that should move
                List<ShoppingList> shoppingListsToMove = new ArrayList<>();
                List<ShoppingList> shoppingLists = new ArrayList<>(e.getMarketsAsBuyer
                    (e.getTraders().get((int)moves[i][0][0])).keySet());
                for (int j = 1 ; j < moves[i][0].length ; ++j) {
                    shoppingListsToMove.add(shoppingLists.get((int)moves[i][0][j]));
                }
                final int k = i;
                // Decide whether to generate a move action or a compoundMove action.
                if (moves[i][1].length >= 2 &&
                    IntStream.range(0, moves[i][1].length).anyMatch(index -> Sets.intersection(
                        e.getTraders().get((int)moves[k][0][index + 1] + 1).getCliques(),
                        e.getTraders().get((int)moves[k][1][index]).getCliques()).size() == 0)) {
                    results[i] = CompoundMove.createAndCheckCompoundMoveWithImplicitSources(
                        e, shoppingListsToMove, Stream.of(moves[i][1]).map(index ->
                            e.getTraders().get((int)index)).collect(Collectors.toList()));
                } else {
                    results[i] = new Move(e, shoppingListsToMove.get(0), shoppingListsToMove.get(0).getSupplier(),
                        e.getTraders().get((int)moves[i][1][0]));
                }
            }

            return results;
        }
    } // end ShopTogetherPlacementDecisions class

    private static Economy createEconomy(Object[][][] buyerConfigurations, Object[][][] sellerConfigurations) {
        Economy e = new Economy();

        // Add buyers
        for (@SuppressWarnings("unused") Object[][] dummy : buyerConfigurations) {
            e.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY).getSettings().setQuoteFactor(0.999)
                .setMoveCostFactor(0);
        }

        // Add sellers
        for (Object[][] sellerConfiguration : sellerConfigurations) {
            // Break-up input
            final Object[] parameters = sellerConfiguration[0];
            final Object[] quantities = sellerConfiguration[1];
            final Object[] utilUpperBounds = sellerConfiguration[2];
            final Long[] bicliques = Arrays.copyOf(sellerConfiguration[3],
                sellerConfiguration[3].length, Long[].class);

            // Add trader to economy
            Trader seller = e.addTrader((int)parameters[1], TraderState.ACTIVE, (Basket)parameters[0], Arrays.asList(bicliques));
            ((TraderSettings) seller).setCanAcceptNewCustomers(true);
            // Give capacity and utilization upper bound default values
            for (@NonNull CommoditySold commoditySold : seller.getCommoditiesSold()) {
                commoditySold.setCapacity(CAPACITY).getSettings().setUtilizationUpperBound(UTILIZATION_UPPER_BOUND);
            }

            // Populate quantities sold
            for (int i = 0 ; i < quantities.length ; ++i) {
                seller.getCommoditiesSold().get(i).setQuantity((double)quantities[i]);
            }

            // Override utilization upper bounds where an explicit value is given
            for (int i = 0 ; i < utilUpperBounds.length ; ++i) {
                seller.getCommoditiesSold().get(i).getSettings().setUtilizationUpperBound((double)utilUpperBounds[i]);
            }
        }

        // Add shopping lists to buyers
        for (int bci = 0 ; bci < buyerConfigurations.length ; ++bci) {

            for (int sli = 0 ; sli < buyerConfigurations[bci].length ; ++sli) {
                ShoppingList shoppingList = e.addBasketBought(e.getTraders().get(bci),
                    (Basket)buyerConfigurations[bci][sli][0]);

                shoppingList.setMovable((boolean)buyerConfigurations[bci][sli][1]);
                if ((int)buyerConfigurations[bci][sli][2] >= 0) {
                    // The supplier of the i-th shopping list is the i-th seller.
                    shoppingList.move(e.getTraders().get((int)buyerConfigurations[bci][sli][2]));
                }

                for (int i = 3 ; i < buyerConfigurations[bci][sli].length; ++i) {
                    shoppingList.setQuantity(i-3, (double)buyerConfigurations[bci][sli][i]);
                }
            }
        }
        e.populateMarketsWithSellersAndMergeConsumerCoverage();

        return e;
    }

    @RunWith(Parameterized.class)
    public static class ShopAlonePlacementDecisions {
        // Fields needed by parameterized runner
        @Parameter(value = 0) public @NonNull Economy economy;
        @Parameter(value = 1) public @NonNull ShoppingList shoppingList;
        @Parameter(value = 2) public @NonNull Action @NonNull [] actions;

        @Test
        public final void testShopAlonePlacementDecisions() {
            assertArrayEquals(actions, Placement.generateShopAlonePlacementDecisions(economy, shoppingList)
                .getActions().toArray());
        }

        @Parameters(name = "Test #{index}: placementActions({0}) == {1}")
        public static Collection<Object[]> shopAloneTestCases() {
            final List<@NonNull Object @NonNull []> output = new ArrayList<>();
            /*
             * This is a 5 level array of arrays:
             *
             * level 1: all test cases (size may vary)
             * level 2: one test case (size should be 3)
             *
             * level 3a: 1 buyers (size == 1)
             * level 4a: 1 shopping lists for the buyer (size == 1)
             * level 5a: the basket bought, whether it's movable and
             *           the economy index of the provider of this shopping list (negative means unplaced)
             *           followed by the quantity bought (size >= 3)
             *
             * level 3b: all sellers (size may vary)
             * level 4b: one seller (size should be 4)
             * level 5b.1: the basket sold and trader type in that order (size should be 2)
             * level 5b.2: the quantities sold index to index (size may vary)
             * level 5b.3: the utilization upper bounds index to index. (size may vary)
             * level 5b.4: the bicliques sold by this trader (size may vary)
             *
             * level 3c: all actions (size may vary)
             * level 4c: one action (size == 1)
             * level 5c.1: the economy index for the buyer that should move, followed by the indices
             *             its shopping list that should move (size == 2)
             * level 5c.2: the economy index for the seller the buyer should move to (size == 1)
             *
             * EXAMPLE: {{{{STORAGE, true, false, 20.0}}},{{{STORAGE, ST_TYPE}, {20.0},{0.8},{0L}}},{{{0,0},{1}}}}
             *          There is 1 buyer, which buys storage, is movable and initially unplaced and buys 20
             *          There is 1 seller which sells storage, it has already sold 20, its utilization is 0.8
             *              and is in the biclique 0.
             *          The buyer fits in the seller, thus there is a move of the buyer with index 0 (the index
             *              of the storage commodity is 0) to the seller with index 1.
             */
            final Object[][][][][] shopAloneTestCases = {

                // 1 buyer, no seller
                {{{{STORAGE, true, -1, 20.0}}},{},{}}, // movable
                {{{{STORAGE, false, -1, 20.0}}},{},{}}, // immovable

                // 1 buyer movable and initially placed, 1 seller
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{40.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{60.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 70.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 40.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 90.0}}},{{{STORAGE, ST_TYPE},{0.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 100.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 70.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 40.0}}},{{{STORAGE, ST_TYPE},{80.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 60.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.7},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.6},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 80.0}}},{{{STORAGE, ST_TYPE},{30.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 10.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.5},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 105.0}}},{{{STORAGE, ST_TYPE},{0.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 90.0}}},{{{STORAGE, ST_TYPE},{0.0},{0.8},{0L}}},{}}, // can't fit STORAGE

                // 1 buyer movable and initially unplaced, 1 seller
                {{{{STORAGE, true, -1, 20.0}}},{{{STORAGE, ST_TYPE}, {20.0},{0.8},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 20.0}}},{{{STORAGE, ST_TYPE},{40.0},{0.7},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 20.0}}},{{{STORAGE, ST_TYPE},{70.0},{},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 40.0}}},{{{STORAGE,ST_TYPE},{30.0},{},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 40.0}}},{{{STORAGE,ST_TYPE},{60.0},{1.0},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.5},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 90.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 105.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 60.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.7},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 70.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.8},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 30.0}}},{{{STORAGE,ST_TYPE},{60.0},{0.8},{0L}}},{}}, // can't fit STORAGE

                // Same as above. Just checking that the results won't be changed by additional trader
                // 1 buyer movable and initially placed, 1 seller
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{40.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{60.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 70.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 40.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 90.0}}},{{{STORAGE, ST_TYPE},{0.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // fits
                {{{{STORAGE, true, 1, 100.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 70.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 40.0}}},{{{STORAGE, ST_TYPE},{80.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.5},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 105.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 100.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 105.0}}},{{{STORAGE, ST_TYPE},{30.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 105.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 105.0}}},{{{STORAGE, ST_TYPE},{80.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, 1, 105.0}}},{{{STORAGE, ST_TYPE},{100.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{}}, // can't fit STORAGE

                // 1 buyer movable and initially unplaced, 1 seller
                {{{{STORAGE, true, -1, 20.0}}},{{{STORAGE, ST_TYPE}, {20.0},{0.8},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 20.0}}},{{{STORAGE, ST_TYPE},{40.0},{0.7},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 20.0}}},{{{STORAGE, ST_TYPE},{70.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.35},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 40.0}}},{{{STORAGE,ST_TYPE},{30.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 40.0}}},{{{STORAGE,ST_TYPE},{60.0},{1.0},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.5},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.5},{0L}}},{{{0,0},{1}}}}, // fits
                {{{{STORAGE, true, -1, 90.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 105.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 60.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.7},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.6},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 70.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.8},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // can't fit STORAGE
                {{{{STORAGE, true, -1, 30.0}}},{{{STORAGE,ST_TYPE},{60.0},{0.8},{0L}},{{STORAGE, ST_TYPE},{70.0},{0.8},{0L}}},{}}, // can't fit STORAGE

                // 1 buyer movable and initially placed and 2 sellers
                // 1st seller fits
                // 2nd seller fits (moves to distribute load)
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // best STORAGE, same biclique
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{1L}}},{}}, // best STORAGE, different biclique
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{{{0,0},{2}}}}, // improves STORAGE, same biclique
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{1L}}},{{{0,0},{2}}}}, // improves STORAGE, different biclique
                {{{{STORAGE, true, 1, 10.0}}},{{{STORAGE, ST_TYPE},{60.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{{{0,0},{2}}}}, // improves STORAGE, same biclique
                {{{{STORAGE, true, 1, 10.0}}},{{{STORAGE, ST_TYPE},{60.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{1L}}},{{{0,0},{2}}}}, // improves STORAGE, different biclique
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{70.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{{{0,0},{2}}}}, // improves STORAGE, same biclique
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{70.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{1L}}},{{{0,0},{2}}}}, // improves STORAGE, different biclique
                // 2nd seller doesn't have enough leftover effective capacity
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // same biclique
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{1L}}},{}}, // different biclique
                // 2nd seller doens't have enough effective capacity
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.4},{0L}}},{}}, // same biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.4},{1L}}},{}}, // different biclique

                // 1st seller doesn't have enough leftover effective capacity
                // 2nd seller fits
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{100.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{{{0,0},{2}}}}, // same biclique
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{100.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{1L}}},{{{0,0},{2}}}}, // different biclique
                // 2nd seller doesn't have enough leftover effective capacity
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{100.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // same biclique
                {{{{STORAGE, true, 1, 20.0}}},{{{STORAGE, ST_TYPE},{100.0},{},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{1L}}},{}}, // different biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.6},{0L}},{{STORAGE, ST_TYPE},{50.0},{0.5},{0L}}},{}}, // same biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.6},{0L}},{{STORAGE, ST_TYPE},{50.0},{0.5},{1L}}},{}}, // different biclique
                // 2nd seller doesn't have enough effective capacity
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{100.0},{},{0L}},{{STORAGE, ST_TYPE},{0.0},{0.4},{0L}}},{}}, // same biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{100.0},{},{0L}},{{STORAGE, ST_TYPE},{0.0},{0.4},{1L}}},{}}, // different biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{100.0},{},{0L}},{{STORAGE, ST_TYPE},{80.0},{0.6},{0L}}},{}}, // same biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{100.0},{},{0L}},{{STORAGE, ST_TYPE},{80.0},{0.6},{1L}}},{}}, // different biclique

                // 1st seller doesn't have enough effective capacity
                // 2nd seller fits
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{{{0,0},{2}}}}, // same biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{1L}}},{{{0,0},{2}}}}, // different biclique
                // 2nd seller doesn't have enough leftover effective capacity
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{80.0},{},{0L}}},{}}, // same biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{80.0},{},{1L}}},{}}, // different biclique
                // 2nd seller doesn't have enough effective capacity
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{0L}}},{}}, // same biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{100.0},{},{1L}}},{}}, // different biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}}},{}}, // same biclique
                {{{{STORAGE, true, 1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{50.0},{0.4},{1L}}},{}}, // different biclique

                // the buyer is movable and initially unplaced
                // 1st seller fits
                // 2nd seller fits (moves to distribute load)
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{{{0,0},{1}}}}, // 1st is best STORAGE, same biclique
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{1L}}},{{{0,0},{1}}}}, // 1st is best STORAGE, different biclique
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{{{0,0},{2}}}}, // 2nd is best STORAGE, same biclique
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{1L}}},{{{0,0},{2}}}}, // 2nd is best STORAGE, different biclique
                // 2nd seller doesn't have enough leftover effective capacity
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{80.0},{},{0L}}},{{{0,0},{1}}}}, // same biclique
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{80.0},{},{1L}}},{{{0,0},{1}}}}, // different biclique
                // 2nd seller doens't have enough effective capacity
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{0.4},{0L}}},{{{0,0},{1}}}}, // same biclique
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{0.4},{1L}}},{{{0,0},{1}}}}, // different biclique

                // 1st seller doesn't have enough leftover effective capacity
                // 2nd seller fits
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{80.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{{{0,0},{2}}}}, // same biclique
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{80.0},{},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{1L}}},{{{0,0},{2}}}}, // different biclique
                // 2nd seller doesn't have enough leftover effective capacity
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{80.0},{},{0L}},{{STORAGE, ST_TYPE},{80.0},{},{0L}}},{}}, // same biclique
                {{{{STORAGE, true,-1, 20.0}}},{{{STORAGE, ST_TYPE},{80.0},{},{0L}},{{STORAGE, ST_TYPE},{80.0},{},{1L}}},{}}, // different biclique
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.6},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.6},{0L}}},{}}, // same biclique
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{0.6},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.6},{1L}}},{}}, // different biclique
                // 2nd doesn't have enough effective capacity
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{},{0.4},{0L}}},{}}, // same biclique
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{50.0},{},{0L}},{{STORAGE, ST_TYPE},{},{0.4},{1L}}},{}}, // different biclique

                // 1st seller doesn't have enough effective capacity
                // 2nd seller fits
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{0L}}},{{{0,0},{2}}}}, // same biclique
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{20.0},{},{1L}}},{{{0,0},{2}}}}, // different biclique
                // 2nd seller doesn't have enough leftover effective capacity
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{0L}}},{}}, // same biclique
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{50.0},{},{1L}}},{}}, // different biclique
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.6},{0L}}},{}}, // same biclique
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{20.0},{0.6},{1L}}},{}}, // different biclique
                // 2nd seller doesn't have enough effective capacity
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{0.0},{0.4},{0L}}},{}}, // same biclique
                {{{{STORAGE, true,-1, 50.0}}},{{{STORAGE, ST_TYPE},{20.0},{0.4},{0L}},{{STORAGE, ST_TYPE},{0.0},{0.4},{1L}}},{}}, // different biclique
            };

            // Convert the multidimensional array to a list of test cases.
            for (Object[][][][] parameters : shopAloneTestCases) {
                Economy economy = createEconomy(parameters[0], parameters[1]);
                Object[] objects = expectedShopAloneOutput(economy, parameters[2]);
                output.add(new Object[] {economy, objects[0], objects[1]});
            }

            return output;
        }

        private static Object[] expectedShopAloneOutput(Economy e, Object[][][] moves) {
            // Construct results
            Action[] results;
            ShoppingList shoppingList = e.getMarketsAsBuyer(e.getTraders().get(0)).keySet().iterator().next();
            if (e.getTraders().size() <= 1) {
                if (shoppingList.isMovable()) {
                    results = new Action[1];
                    results[0] = new Reconfigure(e, shoppingList);
                }
                else {
                    results = new Action[0];
                }
            } else {
                results = new Action[moves.length];
                if (moves.length == 1) {
                    results[0] = new Move(e, shoppingList, e.getTraders().get((int)moves[0][1][0]));
                }
            }

            return new Object[] {shoppingList, results};
        }
    } // end ShopAlonePlacementDecisions class

    // Scaling group tests
    public static class ScalingGroupTests {
        private double[] quantities = {1800.0, 4194304.0};

        /**
         * Return a list of test Traders.  All buyers will be added to a single scaling group
         * @param economy economy to add Traders to
         * @param numBuyers number of buyers to create
         * @param numSellers number of sellers to create
         * @return list of Traders.  The first numBuyers Traders are the buyers, and the remaining
         * Traders are the sellers.
         */
        private Trader[] createTraders(Economy economy, int numBuyers, int numSellers) {
            Trader[] traders = new Trader[numBuyers + numSellers];
            int traderIndex = 0;
            for (int i = 1; i <= numBuyers; i++) {
                // Create two Traders in a single scaling group.
                Trader trader = economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
                trader.setDebugInfoNeverUseInCode("buyer-" + i);
                trader.setScalingGroupId("sg-1");
                trader.getSettings().setQuoteFactor(0.999).setMoveCostFactor(0);
                ShoppingList shoppingList = economy.addBasketBought(trader, PM_SMALL).setMovable(true);
                // First buyer is the group leader
                shoppingList.setGroupFactor(i == 1 ? numBuyers : 0);
                economy.registerShoppingListWithScalingGroup("sg-1", shoppingList);
                traders[traderIndex++] = trader;
            }

            // Add sellers to economy
            for (int i = 1; i <= numSellers; i++) {
                Trader seller = economy.addTrader(VM_TYPE, TraderState.ACTIVE, PM_SMALL);
                seller.setDebugInfoNeverUseInCode("seller-" + i);
                seller.getSettings().setCanAcceptNewCustomers(true);

                // Give capacity and utilization upper bound default values
                for (@NonNull CommoditySold commoditySold : seller.getCommoditiesSold()) {
                    commoditySold.setCapacity(CAPACITY).getSettings().setUtilizationUpperBound(UTILIZATION_UPPER_BOUND);
                }

                // Populate quantities sold
                for (i = 0; i < quantities.length; ++i) {
                    seller.getCommoditiesSold().get(i).setQuantity(quantities[i]);
                }
                traders[traderIndex++] = seller;
            }
            economy.populateMarketsWithSellersAndMergeConsumerCoverage();
            return traders;
        }

        private ShoppingList getSl(Economy economy, Trader trader) {
            // Return the first (and only) ShoppingList for buyer
            return economy.getMarketsAsBuyer(trader).keySet().iterator().next();
        }

        @Test
        public void testScalingGroupResize() {
            Economy e = new Economy();
            Trader[] traders = createTraders(e, 3, 1);  // 3 buyers, 1 seller

            // Expected actions are moves for both trader 1 and trader 2
            Trader seller = traders[3];
            final ShoppingList sl1 = getSl(e, traders[0]);
            Action[] expectedActions = {
                new Move(e, sl1, seller).setImportance(Double.POSITIVE_INFINITY),
                new Move(e, getSl(e, traders[1]), seller).setImportance(Double.POSITIVE_INFINITY),
                new Move(e, getSl(e, traders[2]), seller).setImportance(Double.POSITIVE_INFINITY)
            };

            PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, sl1);
            assertArrayEquals(expectedActions, results.getActions().toArray());
            assertTrue(results.getUnplacedTraders().isEmpty());
        }

        @Test
        public void testScalingGroupReconfigure() {
            Economy e = new Economy();
            Trader[] traders = createTraders(e, 3, 0);  // 3 buyers, no sellers

            final ShoppingList sl1 = getSl(e, traders[0]);
            Action[] expectedActions = {
                new Reconfigure(e, sl1).setImportance(Double.POSITIVE_INFINITY),
                new Reconfigure(e, getSl(e, traders[1])).setImportance(Double.POSITIVE_INFINITY),
                new Reconfigure(e, getSl(e, traders[2])).setImportance(Double.POSITIVE_INFINITY)
            };

            PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, sl1);
            assertArrayEquals(expectedActions, results.getActions().toArray());
            assertTrue(results.getUnplacedTraders().isEmpty());
        }
    }
} // end PlacementTest class
