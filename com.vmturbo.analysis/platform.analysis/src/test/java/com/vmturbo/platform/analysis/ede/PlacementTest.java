package com.vmturbo.platform.analysis.ede;

import static org.junit.Assert.assertArrayEquals;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

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

    private static final CommoditySpecification SEGMENT_0 = new CommoditySpecification(4,1004,0,0);

    // Baskets to use in tests
    private static final Basket EMPTY = new Basket();
    private static final Basket PM_SMALL = new Basket(CPU,MEM);
    private static final Basket PM_LARGE = new Basket(CPU,MEM,SEGMENT_0);
    private static final Basket ST_SMALL = new Basket(STA,LAT);
    private static final Basket ST_LARGE = new Basket(STA,LAT,SEGMENT_0);

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

    private static final Object[] PM_S_M_P_LL = {PM_SMALL, true, true, 20.0, 20.0};
    private static final Object[] PM_L_M_P_LL = {PM_LARGE, true, true, 20.0, 20.0};
    private static final Object[] PM_L_M_P_LM = {PM_LARGE, true, true, 20.0, 50.0};
    private static final Object[] PM_L_M_P_ML = {PM_LARGE, true, true, 50.0, 20.0};
    private static final Object[] PM_L_M_P_MM = {PM_LARGE, true, true, 50.0, 50.0};
    private static final Object[] PM_S_M_U_LL = {PM_SMALL, true,false, 20.0, 20.0};
    private static final Object[] PM_L_M_U_LL = {PM_LARGE, true,false, 20.0, 20.0};
    private static final Object[] PM_L_M_U_LM = {PM_LARGE, true,false, 20.0, 50.0};
    private static final Object[] PM_L_M_U_ML = {PM_LARGE, true,false, 50.0, 20.0};
    private static final Object[] PM_L_M_U_MM = {PM_LARGE, true,false, 50.0, 50.0};

    // Arrays for use in tests
    private static final Action[] NO_ACTIONS = {};

    @RunWith(Parameterized.class)
    public static class PlacementDecisions {
        // Fields needed by parameterized runner
        @Parameter(value = 0) public @NonNull Economy economy;
        @Parameter(value = 1) public @NonNull Action @NonNull [] actions;

        @Test
        public final void testPlacementDecisions() {
            assertArrayEquals(actions, Placement.shopTogetherDecisions(economy).toArray());
        }

        // TODO: add tests with inactive traders
        // TODO: add tests with partially immovable traders
        @Parameters(name = "Test #{index}: placementActions({0}) == {1}")
        public static Collection<Object[]> testCases() {
            final List<@NonNull Object @NonNull []> output = new ArrayList<>();
            /*
             * This is a 5 level array of arrays:
             *
             * level 1: all test cases (size may vary)
             * level 2: one test case (size should be 3)
             *
             * level 3a: all buyers (size may vary)
             * level 4a: all shopping lists for one buyer (size may vary)
             * level 5a: the basket bought, whether it's movable and whether it's initially placed
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
             * level 5c.2: the economy indices for the sellers the buyer should move to (size may vary)
             *
             */
            final Object[][][][][] testCases = {
                // TODO: also test error conditions like:
                // {{{{PM_SMALL, true, true, 20.0,20.0}}},{{PM_S,LL,{}}},{}},

                // 0 VMs, 0 PMs, 0 STs, 0 edges
                {{},{},{}},
                // 1 VM, 0 PMs, 0 STs, 1 edge
                {{{{PM_SMALL,true ,false}}},{},{}}, // movable
/* PBD? RCF? */ {{{{PM_SMALL,false,false}}},{},{}}, // immovable
                // 0 VMs, 1 PM, 0 STs, 0 edges
   /* error? */ {{},{{PM_S,{},{},{}}},{}}, // no bicliques
                {{},{{PM_S,{},{},{0}}},{}}, // empty host
                {{},{{PM_S,{10.0,10.0},{},{0}}},{}}, // empty host with overhead
                // 1 VM, 1 PM, 0 STs, 1 edge
                 // movable and initially placed
                  // baskets match
                {{{PM_S_M_P_LL}},{{PM_S,LL,{},{0}}},{}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, true, 90.0,20.0}}},{{PM_S,HL,{},{0}}},{}}, // can't fit CPU
     /* PBD? */ {{{{PM_SMALL, true, true,105.0,20.0}}},{{PM_S,HL,{},{0}}},{}}, // not enough capacity for CPU
     /* PBD? */ {{{{PM_SMALL, true, true,105.0,20.0}}},{{PM_S,ML,{},{0}}},{}}, // not enough capacity for CPU but host seems fine!
                  // basket subset
                {{{PM_S_M_P_LL}},{{PM_L,LL,{},{0}}},{}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, true, 20.0,90.0}}},{{PM_L,LH,{},{0}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_SMALL, true, true,105.0,90.0}}},{{PM_L,HH,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU
/* PBD !PBS? */ {{{{PM_SMALL, true, true,105.0,90.0}}},{{PM_L,MH,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU, but host seems fine for CPU!
                  // basket superset
     /* RCF? */ {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0}}},{}}, // fits
/* RCF !PBS? */ {{{{PM_LARGE, true, true, 90.0,20.0}}},{{PM_S,HL,{},{0}}},{}}, // can't fit CPU
/* RCF !PBD? */ {{{{PM_LARGE, true, true,105.0,20.0}}},{{PM_S,HL,{},{0}}},{}}, // not enough capacity for CPU
/* RCF !PBD? */ {{{{PM_LARGE, true, true,105.0,20.0}}},{{PM_S,ML,{},{0}}},{}}, // not enough capacity for CPU but host seems fine!
                  // baskets match (but are bigger)
                {{{PM_L_M_P_LL}},{{PM_L,LL,{},{0}}},{}}, // fits
     /* PBS? */ {{{{PM_LARGE, true, true, 20.0,90.0}}},{{PM_L,LH,{},{0}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_LARGE, true, true,105.0,90.0}}},{{PM_L,HH,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU
/* PBD !PBS? */ {{{{PM_LARGE, true, true,105.0,90.0}}},{{PM_L,MH,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU, but host seems fine for CPU!
                 // movable and initially unplaced
                  // baskets match
                {{{{PM_SMALL, true,false, 20.0,20.0}}},{{PM_S,LL,{},{0}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_SMALL, true,false, 90.0,20.0}}},{{PM_S,LL,{},{0}}},{}}, // can't fit CPU
     /* PBD? */ {{{{PM_SMALL, true,false,105.0,20.0}}},{{PM_S,LL,{},{0}}},{}}, // not enough capacity for CPU
                  // basket subset
                {{{{PM_SMALL, true,false, 20.0,20.0}}},{{PM_L,LL,{},{0}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_SMALL, true,false, 20.0,90.0}}},{{PM_L,LL,{},{0}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_SMALL, true,false,105.0,90.0}}},{{PM_L,LL,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU
                  // basket superset
     /* RCF? */ {{{{PM_LARGE, true,false, 20.0,20.0}}},{{PM_S,LL,{},{0}}},{}}, // fits
/* RCF !PBS? */ {{{{PM_LARGE, true,false, 90.0,20.0}}},{{PM_S,LL,{},{0}}},{}}, // can't fit CPU
/* RCF !PBD? */ {{{{PM_LARGE, true,false,105.0,20.0}}},{{PM_S,LL,{},{0}}},{}}, // not enough capacity for CPU
                  // baskets match (but are bigger)
                {{{{PM_LARGE, true,false, 20.0,20.0}}},{{PM_L,LL,{},{0}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_LARGE, true,false, 20.0,90.0}}},{{PM_L,LL,{},{0}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_LARGE, true,false,105.0,90.0}}},{{PM_L,LL,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU

                // 1 VM, 1 PM, 1 ST, 1 edge
                // (Same as above. Just checking that the results won't be changed by additional trader)
                 // movable and initially placed
                  // baskets match
                {{{PM_S_M_P_LL}},{{PM_S,LL,{},{0}},{ST_S,LL,{},{0}}},{}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, true, 90.0,20.0}}},{{PM_S,HL,{},{0}},{ST_S,ML,{},{0}}},{}}, // can't fit CPU
     /* PBD? */ {{{{PM_SMALL, true, true,105.0,20.0}}},{{PM_S,HL,{},{0}},{ST_S,LL,{},{1}}},{}}, // not enough capacity for CPU
     /* PBD? */ {{{{PM_SMALL, true, true,105.0,20.0}}},{{PM_S,ML,{},{0}},{ST_S,HM,{},{0}}},{}}, // not enough capacity for CPU but host seems fine!
                  // basket subset
                {{{PM_S_M_P_LL}},{{PM_L,LL,{},{0}},{ST_L,LL,{},{0}}},{}}, // fits
     /* PBS? */ {{{{PM_SMALL, true, true, 20.0,90.0}}},{{PM_L,LH,{},{0}},{ST_S,ML,{},{1}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_SMALL, true, true,105.0,90.0}}},{{PM_L,HH,{},{0}},{ST_S,LL,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU
/* PBD !PBS? */ {{{{PM_SMALL, true, true,105.0,90.0}}},{{PM_L,MH,{},{0}},{ST_S,HM,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU, but host seems fine for CPU!
                  // basket superset
     /* RCF? */ {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0}},{ST_S,LL,{},{1}}},{}}, // fits
/* RCF !PBS? */ {{{{PM_LARGE, true, true, 90.0,20.0}}},{{PM_S,HL,{},{0}},{ST_L,ML,{},{0}}},{}}, // can't fit CPU
/* RCF !PBD? */ {{{{PM_LARGE, true, true,105.0,20.0}}},{{PM_S,HL,{},{0}},{ST_S,LL,{},{0}}},{}}, // not enough capacity for CPU
/* RCF !PBD? */ {{{{PM_LARGE, true, true,105.0,20.0}}},{{PM_S,ML,{},{0}},{ST_S,HM,{},{1}}},{}}, // not enough capacity for CPU but host seems fine!
                  // baskets match (but are bigger)
                {{{PM_L_M_P_LL}},{{PM_L,LL,{},{0}},{ST_S,LL,{},{0}}},{}}, // fits
     /* PBS? */ {{{{PM_LARGE, true, true, 20.0,90.0}}},{{PM_L,LH,{},{0}},{ST_S,ML,{},{0}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_LARGE, true, true,105.0,90.0}}},{{PM_L,HH,{},{0}},{ST_L,LL,{},{1}}},{}}, // can't fit MEM & not enough capacity for CPU
/* PBD !PBS? */ {{{{PM_LARGE, true, true,105.0,90.0}}},{{PM_L,MH,{},{0}},{ST_S,HM,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU, but host seems fine for CPU!
                 // movable and initially unplaced
                  // baskets match
                {{{{PM_SMALL, true,false, 20.0,20.0}}},{{PM_S,LL,{},{0}},{ST_S,LL,{},{0}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_SMALL, true,false, 90.0,20.0}}},{{PM_S,LL,{},{0}},{ST_S,ML,{},{1}}},{}}, // can't fit CPU
     /* PBD? */ {{{{PM_SMALL, true,false,105.0,20.0}}},{{PM_S,LL,{},{0}},{ST_S,LL,{},{0}}},{}}, // not enough capacity for CPU
                  // basket subset
                {{{{PM_SMALL, true,false, 20.0,20.0}}},{{PM_L,LL,{},{0}},{ST_L,HM,{},{0}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_SMALL, true,false, 20.0,90.0}}},{{PM_L,LL,{},{0}},{ST_S,LL,{},{1}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_SMALL, true,false,105.0,90.0}}},{{PM_L,LL,{},{0}},{ST_S,ML,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU
                  // basket superset
     /* RCF? */ {{{{PM_LARGE, true,false, 20.0,20.0}}},{{PM_S,LL,{},{0}},{ST_S,LL,{},{0}}},{}}, // fits
/* RCF !PBS? */ {{{{PM_LARGE, true,false, 90.0,20.0}}},{{PM_S,LL,{},{0}},{ST_S,HM,{},{1}}},{}}, // can't fit CPU
/* RCF !PBD? */ {{{{PM_LARGE, true,false,105.0,20.0}}},{{PM_S,LL,{},{0}},{ST_L,LL,{},{0}}},{}}, // not enough capacity for CPU
                  // baskets match (but are bigger)
                {{{{PM_LARGE, true,false, 20.0,20.0}}},{{PM_L,LL,{},{0}},{ST_S,ML,{},{0}}},{{{0,0},{1}}}}, // fits
     /* PBS? */ {{{{PM_LARGE, true,false, 20.0,90.0}}},{{PM_L,LL,{},{0}},{ST_S,LL,{},{1}}},{}}, // can't fit MEM
/* PBD !PBS? */ {{{{PM_LARGE, true,false,105.0,90.0}}},{{PM_L,LL,{},{0}},{ST_S,HM,{},{0}}},{}}, // can't fit MEM & not enough capacity for CPU

                // 1 VM, 2 PMs, 0 STs, 1 edge
                 // movable and initially placed
                  // 1st fits
                   // 2nd fits (moves to distribute load)
                {{{PM_S_M_P_LL}},{{PM_S,LL,{},{0}},{PM_S,MM,{},{0}}},{}}, // best CPU,MEM, same biclique
                {{{PM_S_M_P_LL}},{{PM_S,LL,{},{0}},{PM_S,MM,{},{1}}},{}}, // best CPU,MEM, different biclique
                {{{PM_S_M_P_LL}},{{PM_S,MM,{},{0}},{PM_S,LL,{},{0}}},{{{0,0},{2}}}}, // improve CPU,MEM, same biclique
                {{{PM_S_M_P_LL}},{{PM_S,MM,{},{0}},{PM_S,LL,{},{1}}},{{{0,0},{2}}}}, // improve CPU,MEM, different biclique
                {{{{PM_SMALL, true, true, 10.0,20.0}}},{{PM_S,{60.0,40.0},{},{0}},{PM_S,LM,{},{0}}},{}}, // MEM driven best, same biclique
                {{{{PM_SMALL, true, true, 10.0,20.0}}},{{PM_S,{60.0,40.0},{},{0}},{PM_S,LM,{},{1}}},{}}, // MEM driven best, different biclique
                {{{{PM_SMALL, true, true, 20.0,10.0}}},{{PM_S,{70.0,30.0},{},{0}},{PM_S,LM,{},{0}}},{{{0,0},{2}}}}, // CPU driven improve, same biclique
                {{{{PM_SMALL, true, true, 20.0,10.0}}},{{PM_S,{70.0,30.0},{},{0}},{PM_S,LM,{},{1}}},{{{0,0},{2}}}}, // CPU driven improve, different biclique
                    // 2nd doesn't have enough leftover effective capacity
                {{{PM_L_M_P_LM}},{{PM_L,MM,{},{0}},{PM_L,LM,{},{0}}},{}}, // same biclique
                {{{PM_L_M_P_LM}},{{PM_L,MM,{},{0}},{PM_L,LM,{},{1}}},{}}, // different biclique
                    // 2nd doens't have enough effective capacity
                {{{PM_L_M_P_ML}},{{PM_L,ML,{},{0}},{PM_L,LL,{0.4},{0}}},{}}, // same biclique
                {{{PM_L_M_P_ML}},{{PM_L,ML,{},{0}},{PM_L,LL,{0.4},{1}}},{}}, // different biclique
                    // 2nd doesn't sell all required commodities
                {{{PM_L_M_P_LL}},{{PM_L,MM,{},{0}},{PM_S,{},{},{0}}},{}}, // cheaper, same biclique
                {{{PM_L_M_P_LL}},{{PM_L,MM,{},{0}},{PM_S,{},{},{1}}},{}}, // cheaper, different biclique
                {{{PM_L_M_P_LL}},{{PM_L,MM,{},{0}},{PM_S,MM,{},{0}}},{}}, // more expensive, same biclique
                {{{PM_L_M_P_LL}},{{PM_L,MM,{},{0}},{PM_S,MM,{},{1}}},{}}, // more expensive, different biclique
                   // 1st doesn't have enough leftover effective capacity
                    // 2nd fits
                {{{PM_L_M_P_LM}},{{PM_L,LH,{},{0}},{PM_L,ML,{},{0}}},{{{0,0},{2}}}}, // same biclique
                {{{PM_L_M_P_LM}},{{PM_L,LH,{},{0}},{PM_L,ML,{},{1}}},{{{0,0},{2}}}}, // different biclique
                    // 2nd doesn't have enough leftover effective capacity
     /* PBS? */ {{{PM_L_M_P_LM}},{{PM_L,LH,{},{0}},{PM_L,LM,{},{0}}},{}}, // MEM both, same biclique
     /* PBS? */ {{{PM_L_M_P_LM}},{{PM_L,LH,{},{0}},{PM_L,LM,{},{1}}},{}}, // MEM both, different biclique
     /* PBS? */ {{{PM_L_M_P_MM}},{{PM_L,MH,{},{0}},{PM_L,ML,{},{0}}},{}}, // one MEM one CPU, same biclique
     /* PBS? */ {{{PM_L_M_P_MM}},{{PM_L,MH,{},{0}},{PM_L,ML,{},{1}}},{}}, // one MEM one CPU, different biclique
                    // 2nd doesn't have enough effective capacity
/* PBS !PBD? */ {{{PM_L_M_P_ML}},{{PM_L,HL,{},{0}},{PM_L,{},{0.4},{0}}},{}}, // CPU both, same biclique
/* PBS !PBD? */ {{{PM_L_M_P_ML}},{{PM_L,HL,{},{0}},{PM_L,{},{0.4},{1}}},{}}, // CPU both, different biclique
/* PBS !PBD? */ {{{PM_L_M_P_MM}},{{PM_L,HL,{},{0}},{PM_L,{},{1.0,0.4},{0}}},{}}, // one CPU one MEM, same biclique
/* PBS !PBD? */ {{{PM_L_M_P_MM}},{{PM_L,HL,{},{0}},{PM_L,{},{1.0,0.4},{1}}},{}}, // one CPU one MEM, different biclique
                    // 2nd doesn't sell all required commodities
/* PBS !RCF? */ {{{PM_L_M_P_ML}},{{PM_L,HM,{},{0}},{PM_S,LL,{},{0}}},{}}, // cheaper, same biclique
/* PBS !RCF? */ {{{PM_L_M_P_ML}},{{PM_L,HM,{},{0}},{PM_S,LL,{},{1}}},{}}, // cheaper, different biclique
                   // 1st doesn't have enough effective capacity
                    // 2nd fits
                {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0}},{PM_L,LL,{},{0}}},{{{0,0},{2}}}}, // same biclique
                {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0}},{PM_L,LL,{},{1}}},{{{0,0},{2}}}}, // different biclique
                    // 2nd doesn't have enough leftover effective capacity
/* PBS !PBD? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0}},{PM_L,MM,{},{0}}},{}}, // CPU both, same biclique
/* PBS !PBD? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0}},{PM_L,MM,{},{1}}},{}}, // CPU both, different biclique
/* PBS !PBD? */ {{{PM_L_M_P_MM}},{{PM_L,ML,{0.4},{0}},{PM_L,LM,{},{0}}},{}}, // one CPU one MEM, same biclique
/* PBS !PBD? */ {{{PM_L_M_P_MM}},{{PM_L,ML,{0.4},{0}},{PM_L,LM,{},{1}}},{}}, // one CPU one MEM, different biclique
                    // 2nd doesn't have enough effective capacity
     /* PBD? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0}},{PM_L,{},{0.4},{0}}},{}}, // CPU both, same biclique
     /* PBD? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0}},{PM_L,{},{0.4},{1}}},{}}, // CPU both, different biclique
     /* PBD? */ {{{PM_L_M_P_MM}},{{PM_L,ML,{0.4},{0}},{PM_L,{},{1.0,0.4},{0}}},{}}, // one CPU one MEM, same biclique
     /* PBD? */ {{{PM_L_M_P_MM}},{{PM_L,ML,{0.4},{0}},{PM_L,{},{1.0,0.4},{1}}},{}}, // one CPU one MEM, different biclique
                    // 2nd doesn't sell all required commodities
/* PBD !RCF? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0}},{PM_S,{},{},{0}}},{}}, // cheaper, same biclique
/* PBD !RCF? */ {{{PM_L_M_P_ML}},{{PM_L,ML,{0.4},{0}},{PM_S,{},{},{1}}},{}}, // cheaper, different biclique
                   // 1st doesn't sell all required commodities
                    // 2nd fits
                {{{PM_L_M_P_LL}},{{PM_S,MM,{},{0}},{PM_L,{},{},{0}}},{{{0,0},{2}}}}, // cheaper, same biclique
                {{{PM_L_M_P_LL}},{{PM_S,MM,{},{0}},{PM_L,{},{},{1}}},{{{0,0},{2}}}}, // cheaper, different biclique
                {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0}},{PM_L,MM,{},{0}}},{{{0,0},{2}}}}, // more expensive, same biclique
                {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0}},{PM_L,MM,{},{1}}},{{{0,0},{2}}}}, // more expensive, different biclique
                    // 2nd doesn't have enough leftover effective capacity
/* PBS !RCF? */ {{{PM_L_M_P_LM}},{{PM_S,LM,{},{0}},{PM_L,MM,{},{0}}},{}}, // MEM, same biclique
/* PBS !RCF? */ {{{PM_L_M_P_LM}},{{PM_S,LM,{},{0}},{PM_L,MM,{},{1}}},{}}, // MEM, different biclique
                    // 2nd doesn't have enough effective capacity
/* PBD !RCF? */ {{{PM_L_M_P_LM}},{{PM_S,LM,{},{0}},{PM_L,{},{1.0,0.4},{0}}},{}}, // MEM, same biclique
/* PBD !RCF? */ {{{PM_L_M_P_LM}},{{PM_S,LM,{},{0}},{PM_L,{},{1.0,0.4},{1}}},{}}, // MEM, different biclique
                    // 2nd doesn't sell all required commodities
     /* RCF? */ {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0}},{PM_S,LL,{},{0}}},{}}, // same biclique
     /* RCF? */ {{{PM_L_M_P_LL}},{{PM_S,LL,{},{0}},{PM_S,LL,{},{1}}},{}}, // different biclique
                 // movable and initially unplaced
                  // 1st fits
                   // 2nd fits (moves to distribute load)
                {{{PM_S_M_U_LL}},{{PM_S,LL,{},{0}},{PM_S,MM,{},{0}}},{{{0,0},{1}}}}, // 1st is best CPU,MEM, same biclique
                {{{PM_S_M_U_LL}},{{PM_S,LL,{},{0}},{PM_S,MM,{},{1}}},{{{0,0},{1}}}}, // 1st is best CPU,MEM, different biclique
                {{{PM_S_M_U_LL}},{{PM_S,MM,{},{0}},{PM_S,LL,{},{0}}},{{{0,0},{2}}}}, // 2nd is best CPU,MEM, same biclique
                {{{PM_S_M_U_LL}},{{PM_S,MM,{},{0}},{PM_S,LL,{},{1}}},{{{0,0},{2}}}}, // 2nd is best CPU,MEM, different biclique
                {{{{PM_SMALL, true,false, 10.0,20.0}}},{{PM_S,ML,{},{0}},{PM_S,LM,{},{0}}},{{{0,0},{1}}}}, // MEM driven 1st best, same biclique
                {{{{PM_SMALL, true,false, 10.0,20.0}}},{{PM_S,ML,{},{0}},{PM_S,LM,{},{1}}},{{{0,0},{1}}}}, // MEM driven 1st best, different biclique
                {{{{PM_SMALL, true,false, 20.0,10.0}}},{{PM_S,ML,{},{0}},{PM_S,LM,{},{0}}},{{{0,0},{2}}}}, // CPU driven 2nd best, same biclique
                {{{{PM_SMALL, true,false, 20.0,10.0}}},{{PM_S,ML,{},{0}},{PM_S,LM,{},{1}}},{{{0,0},{2}}}}, // CPU driven 2nd best, different biclique
                    // 2nd doesn't have enough leftover effective capacity
                {{{PM_L_M_U_LM}},{{PM_L,LL,{},{0}},{PM_L,LM,{},{0}}},{{{0,0},{1}}}}, // same biclique
                {{{PM_L_M_U_LM}},{{PM_L,LL,{},{0}},{PM_L,LM,{},{1}}},{{{0,0},{1}}}}, // different biclique
                    // 2nd doens't have enough effective capacity
                {{{PM_L_M_U_ML}},{{PM_L,LL,{},{0}},{PM_L,LL,{0.4},{0}}},{{{0,0},{1}}}}, // same biclique
                {{{PM_L_M_U_ML}},{{PM_L,LL,{},{0}},{PM_L,LL,{0.4},{1}}},{{{0,0},{1}}}}, // different biclique
                    // 2nd doesn't sell all required commodities
                {{{PM_L_M_U_LL}},{{PM_L,LL,{},{0}},{PM_S,{},{},{0}}},{{{0,0},{1}}}}, // cheaper, same biclique
                {{{PM_L_M_U_LL}},{{PM_L,LL,{},{0}},{PM_S,{},{},{1}}},{{{0,0},{1}}}}, // cheaper, different biclique
                {{{PM_L_M_U_LL}},{{PM_L,LL,{},{0}},{PM_S,MM,{},{0}}},{{{0,0},{1}}}}, // more expensive, same biclique
                {{{PM_L_M_U_LL}},{{PM_L,LL,{},{0}},{PM_S,MM,{},{1}}},{{{0,0},{1}}}}, // more expensive, different biclique
                   // 1st doesn't have enough leftover effective capacity
                    // 2nd fits
                {{{PM_L_M_U_LM}},{{PM_L,LM,{},{0}},{PM_L,ML,{},{0}}},{{{0,0},{2}}}}, // same biclique
                {{{PM_L_M_U_LM}},{{PM_L,LM,{},{0}},{PM_L,ML,{},{1}}},{{{0,0},{2}}}}, // different biclique
                    // 2nd doesn't have enough leftover effective capacity
     /* PBS? */ {{{PM_L_M_U_LM}},{{PM_L,LM,{},{0}},{PM_L,LM,{},{0}}},{}}, // MEM both, same biclique
     /* PBS? */ {{{PM_L_M_U_LM}},{{PM_L,LM,{},{0}},{PM_L,LM,{},{1}}},{}}, // MEM both, different biclique
     /* PBS? */ {{{PM_L_M_U_MM}},{{PM_L,LM,{},{0}},{PM_L,ML,{},{0}}},{}}, // one MEM one CPU, same biclique
     /* PBS? */ {{{PM_L_M_U_MM}},{{PM_L,LM,{},{0}},{PM_L,ML,{},{1}}},{}}, // one MEM one CPU, different biclique
                    // 2nd doesn't have enough effective capacity
/* PBS !PBD? */ {{{PM_L_M_U_ML}},{{PM_L,ML,{},{0}},{PM_L,{},{0.4},{0}}},{}}, // CPU both, same biclique
/* PBS !PBD? */ {{{PM_L_M_U_ML}},{{PM_L,ML,{},{0}},{PM_L,{},{0.4},{1}}},{}}, // CPU both, different biclique
/* PBS !PBD? */ {{{PM_L_M_U_MM}},{{PM_L,ML,{},{0}},{PM_L,{},{1.0,0.4},{0}}},{}}, // one CPU one MEM, same biclique
/* PBS !PBD? */ {{{PM_L_M_U_MM}},{{PM_L,ML,{},{0}},{PM_L,{},{1.0,0.4},{1}}},{}}, // one CPU one MEM, different biclique
                    // 2nd doesn't sell all required commodities
/* PBS !RCF? */ {{{PM_L_M_U_ML}},{{PM_L,MM,{},{0}},{PM_S,LL,{},{0}}},{}}, // cheaper, same biclique
/* PBS !RCF? */ {{{PM_L_M_U_ML}},{{PM_L,MM,{},{0}},{PM_S,LL,{},{1}}},{}}, // cheaper, different biclique
                   // 1st doesn't have enough effective capacity
                    // 2nd fits
                {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0}},{PM_L,LL,{},{0}}},{{{0,0},{2}}}}, // same biclique
                {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0}},{PM_L,LL,{},{1}}},{{{0,0},{2}}}}, // different biclique
                    // 2nd doesn't have enough leftover effective capacity
/* PBS !PBD? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0}},{PM_L,MM,{},{0}}},{}}, // CPU both, same biclique
/* PBS !PBD? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0}},{PM_L,MM,{},{1}}},{}}, // CPU both, different biclique
/* PBS !PBD? */ {{{PM_L_M_U_MM}},{{PM_L,LL,{0.4},{0}},{PM_L,LM,{},{0}}},{}}, // one CPU one MEM, same biclique
/* PBS !PBD? */ {{{PM_L_M_U_MM}},{{PM_L,LL,{0.4},{0}},{PM_L,LM,{},{1}}},{}}, // one CPU one MEM, different biclique
                    // 2nd doesn't have enough effective capacity
     /* PBD? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0}},{PM_L,{},{0.4},{0}}},{}}, // CPU both, same biclique
     /* PBD? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0}},{PM_L,{},{0.4},{1}}},{}}, // CPU both, different biclique
     /* PBD? */ {{{PM_L_M_U_MM}},{{PM_L,LL,{0.4},{0}},{PM_L,{},{1.0,0.4},{0}}},{}}, // one CPU one MEM, same biclique
     /* PBD? */ {{{PM_L_M_U_MM}},{{PM_L,LL,{0.4},{0}},{PM_L,{},{1.0,0.4},{1}}},{}}, // one CPU one MEM, different biclique
                    // 2nd doesn't sell all required commodities
/* PBD !RCF? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0}},{PM_S,{},{},{0}}},{}}, // cheaper, same biclique
/* PBD !RCF? */ {{{PM_L_M_U_ML}},{{PM_L,LL,{0.4},{0}},{PM_S,{},{},{1}}},{}}, // cheaper, different biclique
                   // 1st doesn't sell all required commodities
                    // 2nd fits
                {{{PM_L_M_U_LL}},{{PM_S,MM,{},{0}},{PM_L,{},{},{0}}},{{{0,0},{2}}}}, // cheaper, same biclique
                {{{PM_L_M_U_LL}},{{PM_S,MM,{},{0}},{PM_L,{},{},{1}}},{{{0,0},{2}}}}, // cheaper, different biclique
                {{{PM_L_M_U_LL}},{{PM_S,LL,{},{0}},{PM_L,MM,{},{0}}},{{{0,0},{2}}}}, // more expensive, same biclique
                {{{PM_L_M_U_LL}},{{PM_S,LL,{},{0}},{PM_L,MM,{},{1}}},{{{0,0},{2}}}}, // more expensive, different biclique
                    // 2nd doesn't have enough leftover effective capacity
/* PBS !RCF? */ {{{PM_L_M_U_LM}},{{PM_S,LL,{},{0}},{PM_L,MM,{},{0}}},{}}, // MEM, same biclique
/* PBS !RCF? */ {{{PM_L_M_U_LM}},{{PM_S,LL,{},{0}},{PM_L,MM,{},{1}}},{}}, // MEM, different biclique
                    // 2nd doesn't have enough effective capacity
/* PBD !RCF? */ {{{PM_L_M_U_LM}},{{PM_S,LL,{},{0}},{PM_L,{},{1.0,0.4},{0}}},{}}, // MEM, same biclique
/* PBD !RCF? */ {{{PM_L_M_U_LM}},{{PM_S,LL,{},{0}},{PM_L,{},{1.0,0.4},{1}}},{}}, // MEM, different biclique
                    // 2nd doesn't sell all required commodities
     /* RCF? */ {{{PM_L_M_U_LL}},{{PM_S,LL,{},{0}},{PM_S,LL,{},{0}}},{}}, // same biclique
     /* RCF? */ {{{PM_L_M_U_LL}},{{PM_S,LL,{},{0}},{PM_S,LL,{},{1}}},{}}, // different biclique

                // 1 VM, 1 PM, 1 ST, 2 edges


                // 1 VM, 3 PMs, 3 STs, 2 edges
                {{{{PM_SMALL,true ,true,1.0,1.0},{ST_SMALL,true ,true,1.0,1.0}}}, // best share-nothing move. M1 would select 3,4 instead
                 {{PM_S,{10.0,10.0},{},{0}},{ST_S,{ 1.0, 1.0},{},{0}},
                  {PM_S,{ 1.0, 1.0},{},{1}},{ST_S,{12.0,12.0},{},{1}},
                  {PM_S,{ 4.0, 4.0},{},{2}},{ST_S,{ 3.0, 3.0},{},{2}}},
                 {{{0,0,1},{5,6}}}},
                {{{{PM_SMALL,true ,true,1.0,1.0},{ST_SMALL,true ,true,1.0,1.0}}}, // best share-nothing move with overlap.
                 {{PM_S,{10.0,10.0},{},{0}},{ST_S,{ 1.0, 1.0},{},{0}},
                  {PM_S,{ 1.0, 1.0},{},{0}},{ST_S,{12.0,12.0},{},{0,1}},
                  {PM_S,{ 4.0, 4.0},{},{1}},{ST_S,{ 3.0, 3.0},{},{1}}},
                 {{{0,0,1},{3,2}}}},
                 {{{{PM_SMALL,true ,true,1.0,1.0},{ST_SMALL,true ,true,1.0,1.0}}}, // best share-nothing move with overlap.
                  {{PM_S,{10.0,10.0},{},{0}},{ST_S,{ 1.0, 1.0},{},{0}},
                   {PM_S,{ 4.0, 4.0},{},{0}},{ST_S,{ 3.0, 3.0},{},{0,1}},
                   {PM_S,{ 1.0, 1.0},{},{1}},{ST_S,{ 6.0, 6.0},{},{1}}},
                  {{{0,0,1},{5,4}}}},
                 {{{{PM_SMALL,true ,true,1.0,1.0},{ST_SMALL,false,true,1.0,1.0}}}, // one shopping list is immovable.
                  {{PM_S,{10.0,10.0},{},{0}},{ST_S,{ 7.0, 7.0},{},{0}},
                   {PM_S,{ 4.0, 4.0},{},{0}},{ST_S,{ 3.0, 3.0},{},{0,1}},
                   {PM_S,{ 1.0, 1.0},{},{1}},{ST_S,{ 6.0, 6.0},{},{1}}},
                  {{{0,0},{3}}}}
            };

            // Convert the multidimential array to a list of test cases.
            for (Object[][][][] parameters : testCases) {
                output.add(testCase(parameters[0],parameters[1],parameters[2]));
            }

            // Add any other test cases not expressible using the testCase method.

            // 1 VM, 0 PMs, 0 STs, 0 edges
            Economy economy = new Economy();
            economy.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
            output.add(new Object[]{economy, NO_ACTIONS});

            return output;
        }

    } // end PlacementDecisions class

    private static Object[] testCase(Object[][][] buyerConfigurations, Object[][][] sellerConfigurations, Object[][][] moves) {
        Economy e = new Economy();

        // Add buyers
        for (@SuppressWarnings("unused") Object[][] dummy : buyerConfigurations) {
            e.addTrader(VM_TYPE, TraderState.ACTIVE, EMPTY);
        }

        // Add sellers
        for (Object[][] sellerConfiguration : sellerConfigurations) {
            // Break-up input
            final Object[] parameters = sellerConfiguration[0];
            final Object[] quantities = sellerConfiguration[1];
            final Object[] utilUpperBounds = sellerConfiguration[2];
            final Integer[] bicliques = Arrays.copyOf(sellerConfiguration[3],
                                                      sellerConfiguration[3].length, Integer[].class);

            // Add trader to economy
            Trader seller = e.addTrader((int)parameters[1], TraderState.ACTIVE, (Basket)parameters[0], Arrays.asList(bicliques));

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
        ShoppingList[][] shoppingLists = new ShoppingList[buyerConfigurations.length][];
        for (int bci = 0 ; bci < buyerConfigurations.length ; ++bci) {
            shoppingLists[bci] = new ShoppingList[buyerConfigurations[bci].length];

            for (int sli = 0 ; sli < buyerConfigurations[bci].length ; ++sli) {
                shoppingLists[bci][sli] = e.addBasketBought(e.getTraders().get(bci),(Basket)buyerConfigurations[bci][sli][0]);

                shoppingLists[bci][sli].setMovable((boolean)buyerConfigurations[bci][sli][1]);
                if ((boolean)buyerConfigurations[bci][sli][2]) {
                    shoppingLists[bci][sli].move(e.getTraders().get(buyerConfigurations.length+sli));
                }

                for (int i = 3 ; i < buyerConfigurations[bci][sli].length ; ++i) {
                    shoppingLists[bci][sli].setQuantity(i-3, (double)buyerConfigurations[bci][sli][i]);
                }
            }
        }

        // Construct results
        Action[] results = new Action[moves.length];
        for (int i = 0 ; i < moves.length ; ++i) {
            // Find the subset of shopping lists that should move
            List<ShoppingList> shoppingListsToMove = new ArrayList<>();
            for (int j = 1 ; j < moves[i][0].length ; ++j) {
                shoppingListsToMove.add(shoppingLists[(int)moves[i][0][0]][(int)moves[i][0][j]]);
            }

            results[i] = new CompoundMove(e, shoppingListsToMove, Stream.of(moves[i][1])
                .map(index -> e.getTraders().get((int)index)).collect(Collectors.toList()));
        }

        return new Object[]{e,results};
    }

} // end PlacementTest class
