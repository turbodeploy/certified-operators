package com.vmturbo.platform.analysis.ede;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.lang.reflect.Field;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.*;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.*;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.FunctionalOperator;
import com.vmturbo.platform.analysis.utilities.FunctionalOperatorUtil;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.analysis.topology.Topology;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class CloudActionsIntegtationTest {

    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification COUPON = new CommoditySpecification(1);
    private static final CommoditySpecification FAMILY = new CommoditySpecification(2);
    private static final CommoditySpecification LICENSE = new CommoditySpecification(5, 5, 0, 0);
    private static final Basket SOLDbyTP = new Basket(CPU, COUPON, FAMILY, LICENSE);
    private static final Basket SOLDbyCBTP = new Basket(CPU, COUPON, LICENSE);
    private static final Basket BOUGHTbyVM = new Basket(CPU, COUPON, LICENSE);

    private @NonNull Economy first;
    private @NonNull Topology firstTopology;
    private @Nonnull Trader tp1, tp2, cbtp1, cbtp2, vm1, vm2, vm3;
    private static final long BA = 1, REGION = 2, ZONE = 3;
    private static final double VERY_LOW_PRICE = 2, LOW_PRICE = 5, HIGH_PRICE = 10;
    ShoppingList shoppingListOfVm1, shoppingListOfVm2, shoppingListOfVm3;

    private @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOids = HashBiMap.create();

    @Before
    public void setUp() throws Exception {
        first = new Economy();
        firstTopology = new Topology();
        first.setTopology(firstTopology);
        tp1 = first.addTrader(0, TraderState.ACTIVE, SOLDbyTP, new Basket());
        tp2 = first.addTrader(0, TraderState.ACTIVE, SOLDbyTP, new Basket());
        cbtp1 = first.addTrader(0, TraderState.ACTIVE, SOLDbyCBTP, new Basket(FAMILY));
        cbtp2 = first.addTrader(0, TraderState.ACTIVE, SOLDbyCBTP, new Basket(FAMILY));

        vm1 = first.addTrader(1, TraderState.ACTIVE, new Basket(), BOUGHTbyVM);
        vm2 = first.addTrader(1, TraderState.ACTIVE, new Basket(), BOUGHTbyVM);
        vm3 = first.addTrader(1, TraderState.ACTIVE, new Basket(), BOUGHTbyVM);

        Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0));
        vm1.setDebugInfoNeverUseInCode("VirtualMachine|1");
        vm2.setDebugInfoNeverUseInCode("VirtualMachine|2");
        vm3.setDebugInfoNeverUseInCode("VirtualMachine|3");
        tp1.setDebugInfoNeverUseInCode("OnDemandMarketTier|1");
        tp2.setDebugInfoNeverUseInCode("OnDemandMarketTier|2");
        cbtp1.setDebugInfoNeverUseInCode("DiscountedMarketTier|1");
        cbtp2.setDebugInfoNeverUseInCode("DiscountedMarketTier|2");
        traderOids.put(vm1, 1L);
        traderOids.put(vm2, 2L);
        traderOids.put(vm3, 3L);
        traderOids.put(tp1, 4L);
        traderOids.put(tp2, 5L);
        traderOids.put(cbtp1, 6L);
        traderOids.put(cbtp2, 7L);

        shoppingListOfVm1 = first.getMarketsAsBuyer(vm1).keySet().iterator().next();
        vm1.getSettings().setContext(context);
        shoppingListOfVm2 = first.getMarketsAsBuyer(vm2).keySet().iterator().next();
        vm2.getSettings().setContext(context);
        shoppingListOfVm3 = first.getMarketsAsBuyer(vm3).keySet().iterator().next();
        vm3.getSettings().setContext(context);

        // setup CPU usage
        shoppingListOfVm1.setQuantity(0, 40).setMovable(true);
        shoppingListOfVm2.setQuantity(0, 60).setMovable(true);

        // create costDTOs
        CostDTOs.CostDTO costDTO_tp1 = CostDTOs.CostDTO.newBuilder()
                .setComputeTierCost(CostDTOs.CostDTO.ComputeTierCostDTO.newBuilder()
                        .setCouponBaseType(COUPON.getBaseType())
                        .setLicenseCommodityBaseType(LICENSE.getBaseType())
                        .addCostTupleList(CostDTOs.CostDTO.CostTuple.newBuilder()
                                .setBusinessAccountId(BA)
                                .setLicenseCommodityType(LICENSE.getType())
                                .setRegionId(REGION)
                                .setPrice(LOW_PRICE).build())
                        .build())
                .build();
        FunctionalOperator ignore = FunctionalOperatorUtil.createIgnoreConsumptionUpdatingFunction(costDTO_tp1, UpdatingFunctionDTOs.UpdatingFunctionTO.newBuilder()
                .setIgnoreConsumption(UpdatingFunctionDTOs.UpdatingFunctionTO.IgnoreConsumption.newBuilder()
                        .build())
                .build());

        tp1.getSettings().setCostFunction(CostFunctionFactory.createCostFunctionForComputeTier(costDTO_tp1.getComputeTierCost()));
        tp1.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        tp1.getCommoditySold(CPU).setCapacity(50).getSettings().setUpdatingFunction(ignore);
        tp1.getCommoditySold(COUPON).setCapacity(8).getSettings().setUpdatingFunction(FunctionalOperatorUtil.createIgnoreConsumptionUpdatingFunction(costDTO_tp1,
                UpdatingFunctionDTOs.UpdatingFunctionTO.newBuilder().build()));

        CostDTOs.CostDTO costDTO_tp2 = CostDTOs.CostDTO.newBuilder()
                .setComputeTierCost(CostDTOs.CostDTO.ComputeTierCostDTO.newBuilder()
                        .setCouponBaseType(COUPON.getBaseType())
                        .setLicenseCommodityBaseType(LICENSE.getBaseType())
                        .addCostTupleList(CostDTOs.CostDTO.CostTuple.newBuilder()
                                .setBusinessAccountId(BA)
                                .setLicenseCommodityType(LICENSE.getType())
                                .setRegionId(REGION)
                                .setPrice(HIGH_PRICE)
                                .build())
                        .build())
                .build();
        tp2.getSettings().setCostFunction(CostFunctionFactory.createCostFunctionForComputeTier(costDTO_tp2.getComputeTierCost()));
        tp2.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        tp2.getCommoditySold(CPU).setCapacity(100).getSettings().setUpdatingFunction(ignore);
        tp2.getCommoditySold(COUPON).setCapacity(16).getSettings().setUpdatingFunction(FunctionalOperatorUtil.createIgnoreConsumptionUpdatingFunction(costDTO_tp2,
                UpdatingFunctionDTOs.UpdatingFunctionTO.newBuilder().build()));

        CostDTOs.CostDTO costDTO_cbtp = CostDTOs.CostDTO.newBuilder()
                .setCbtpResourceBundle(CostDTOs.CostDTO.CbtpCostDTO.newBuilder()
                        .setCouponBaseType(COUPON.getBaseType())
                        .setDiscountPercentage(0.4)
                        .setCostTuple(CostDTOs.CostDTO.CostTuple.newBuilder()
                                .setBusinessAccountId(BA)
                                .setLicenseCommodityType(LICENSE.getType())
                                .setRegionId(REGION)
                                .setPrice(VERY_LOW_PRICE)
                                .build())
                        .build())
                .build();
        cbtp1.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(costDTO_cbtp.getCbtpResourceBundle()));
        cbtp1.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        cbtp1.getCommoditySold(CPU).setCapacity(100);
        cbtp1.getCommoditySold(COUPON).getSettings().setUpdatingFunction(FunctionalOperatorUtil.createCouponUpdatingFunction(costDTO_cbtp,
                UpdatingFunctionDTOs.UpdatingFunctionTO.newBuilder().build()));

        cbtp2.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(costDTO_cbtp.getCbtpResourceBundle()));
        cbtp2.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        cbtp2.getCommoditySold(CPU).setCapacity(100);
        cbtp2.getCommoditySold(COUPON).getSettings().setUpdatingFunction(FunctionalOperatorUtil.createCouponUpdatingFunction(costDTO_cbtp,
                UpdatingFunctionDTOs.UpdatingFunctionTO.newBuilder().build()));
        // coupon used and cap unset

        tp1.getSettings().setCanAcceptNewCustomers(true).setSuspendable(false).setCloneable(false);
        tp2.getSettings().setCanAcceptNewCustomers(true).setSuspendable(false).setCloneable(false);
        cbtp1.getSettings().setCanAcceptNewCustomers(true).setSuspendable(false).setCloneable(false);
        cbtp2.getSettings().setCanAcceptNewCustomers(true).setSuspendable(false).setCloneable(false);

        first.getCommodityBought(shoppingListOfVm1, CPU).setQuantity(40);
        first.getCommodityBought(shoppingListOfVm2, CPU).setQuantity(10);
        first.getSettings().setEstimatesEnabled(false);

        first.populateMarketsWithSellers();

        first.setTopology(firstTopology);
        Field traderOidField = Topology.class.getDeclaredField("traderOids_");
        traderOidField.setAccessible(true);
        traderOidField.set(firstTopology, traderOids);
        Field unmodifiableTraderOidField = Topology.class
                .getDeclaredField("unmodifiableTraderOids_");
        unmodifiableTraderOidField.setAccessible(true);
        unmodifiableTraderOidField.set(firstTopology, traderOids);

    }

    /**
     *
     * This test verifies that when resizes occur before replay, we do not provision and suspend
     * the same entity.
     */
    @Test
    public void testCouponUpdationOnMoves_1() {
        // CBTP has a capacity of 40 coupons
        cbtp1.getCommoditySold(COUPON).setCapacity(40);
        shoppingListOfVm1.move(tp1);
        shoppingListOfVm2.move(tp1);
        Move move1 = new Move(first, shoppingListOfVm1, cbtp1).take();
        Move move2 = new Move(first, shoppingListOfVm2, cbtp1).take();
        // moving each VM to CBTP consuming 8 coupons each
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), 16, 0);
        assertEquals(shoppingListOfVm1.getQuantity(1), 8, 0);
        assertEquals(shoppingListOfVm2.getQuantity(1), 8, 0);
    }

    @Test
    public void testCouponUpdationOnMoves_2() {
        // CBTP has a capacity of 30 coupons
        cbtp1.getCommoditySold(COUPON).setCapacity(30).setQuantity(10);
        shoppingListOfVm1.move(tp1);
        shoppingListOfVm2.move(tp1);
        Move move1 = new Move(first, shoppingListOfVm1, cbtp1).take();
        Move move2 = new Move(first, shoppingListOfVm2, cbtp1).take();
        // moving each VM to CBTP consuming 8 coupons each
        // there is an overhead of 10
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), 26, 0);
    }

    // when there is a partially covered VM on a CBTP, if there is some other VM that shops before it,
    // it will try to move but not get any coupons as the partially covered VM will take up all coupons
    @Test
    public void testCouponUpdationOnMoves_3() {
        // CBTP has a capacity of 30 coupons
        cbtp1.getCommoditySold(COUPON).setCapacity(16).setQuantity(8);
        shoppingListOfVm1.move(cbtp1);
        // Partially covered VM is on the CBTP even though there are coupons
        first.getCommodityBought(shoppingListOfVm1, COUPON).setQuantity(8);
        first.getCommodityBought(shoppingListOfVm1, CPU).setQuantity(60);
        first.getCommodityBought(shoppingListOfVm2, CPU).setQuantity(60);
        Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0), 16,8);
        vm1.getSettings().setContext(context);
        Move move2 = new Move(first, shoppingListOfVm2, cbtp1).take();
        // new VM requests 16 coupons
        // will it get remaining 8 or will the original one obtain 16 without an action?
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), 16, 0);
        // This will fail because the coupons will be allocated to the existing VM
        assertNotEquals(shoppingListOfVm2.getQuantity(1), 8, 0);
    }

    // scale up on CBTP
    @Test
    public void testCouponUpdationOnMoves_4() {
        // CBTP has a capacity of 30 coupons
        cbtp1.getCommoditySold(COUPON).setCapacity(16).setQuantity(8);
        shoppingListOfVm1.move(cbtp1);
        // VM scales on the CBTP alone without any co-customers
        first.getCommodityBought(shoppingListOfVm1, COUPON).setQuantity(8);
        first.getCommodityBought(shoppingListOfVm1, CPU).setQuantity(60);
        Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0), 8,8);
        vm1.getSettings().setContext(context);
        EconomyDTOs.Context quoteContext = EconomyDTOs.Context.newBuilder().setTotalRequestedCoupons(16)
                .setTotalAllocatedCoupons(8).build();
        Move move2 = new Move(first, shoppingListOfVm1, cbtp1, cbtp1, Optional.of(quoteContext)).take();
        // VM now requests 16 coupons
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), 16, 0);
        // All 16 coupons will be allocated
        assertEquals(shoppingListOfVm1.getQuantity(1), 16, 0);
    }

    // scale down on CBTP and different VM using the coupons
    @Test
    public void testCouponUpdationOnMoves_5() {
        // CBTP has a capacity of 30 coupons
        cbtp1.getCommoditySold(COUPON).setCapacity(16).setQuantity(16);
        shoppingListOfVm1.move(cbtp1);
        // VM scales on the CBTP alone without any co-customers
        first.getCommodityBought(shoppingListOfVm1, COUPON).setQuantity(16);
        first.getCommodityBought(shoppingListOfVm1, CPU).setQuantity(10);
        Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0), 16,16);
        vm1.getSettings().setContext(context);
        EconomyDTOs.Context quoteContext = EconomyDTOs.Context.newBuilder().setTotalRequestedCoupons(8)
                .setTotalAllocatedCoupons(8).build();
        (new Move(first, shoppingListOfVm1, cbtp1, cbtp1, Optional.of(quoteContext))).take();
        // VM now requests 8 coupons since it is scaling down
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), 8, 0);
        // 8 coupons will be allocated
        assertEquals(shoppingListOfVm1.getQuantity(1), 8, 0);
        shoppingListOfVm2.move(tp1);
        (new Move(first, shoppingListOfVm2, tp1, cbtp1, Optional.of(quoteContext))).take();
        // VM's now requests 8 coupons each
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), 16, 0);
        // 8 coupons will be allocated
        assertEquals(shoppingListOfVm1.getQuantity(1), 8, 0);
        assertEquals(shoppingListOfVm2.getQuantity(1), 8, 0);
    }

    // moving from 1 cbtp to another
    @Test
    public void testCouponUpdationOnMoves_6() {
        // CBTP has a capacity of 16 coupons off which 8 is overhead by VM1
        cbtp1.getCommoditySold(COUPON).setCapacity(16).setQuantity(16);
        cbtp2.getCommoditySold(COUPON).setCapacity(16);
        shoppingListOfVm1.move(cbtp1);
        shoppingListOfVm2.move(cbtp1);
        // VM scales on the CBTP alone without any co-customers
        first.getCommodityBought(shoppingListOfVm1, COUPON).setQuantity(8);
        first.getCommodityBought(shoppingListOfVm1, CPU).setQuantity(10);
        first.getCommodityBought(shoppingListOfVm2, COUPON).setQuantity(8);
        first.getCommodityBought(shoppingListOfVm2, CPU).setQuantity(60);
        Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0), 8,8);
        vm1.getSettings().setContext(context);
        Move move1 = new Move(first, shoppingListOfVm2, cbtp2).take();
        // VM1 now requests 8 coupons from CBTP1
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), 8, 0);
        // VM2 now requests 16 coupons from CBTP2
        assertEquals(cbtp2.getCommoditySold(COUPON).getQuantity(), 16, 0);
        // All 16 coupons will be allocated
        assertEquals(shoppingListOfVm1.getQuantity(1), 8, 0);
        assertEquals(shoppingListOfVm2.getQuantity(1), 16, 0);
    }

//    // VM moving out of a CBTP. VM coverage doesnt get updated
//    @Test
//    public void testCouponUpdationOnMoves_7() {
//        // CBTP has a capacity of 30 coupons
//        cbtp1.getCommoditySold(COUPON).setCapacity(16).setQuantity(16);
//        shoppingListOfVm1.move(cbtp1);
//        // Partially covered VM is on the CBTP even though there are coupons
//        first.getCommodityBought(shoppingListOfVm1, COUPON).setQuantity(16);
//        first.getCommodityBought(shoppingListOfVm1, CPU).setQuantity(10);
//        Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0), 16,16);
//        vm1.getSettings().setContext(context);
//        Move move2 = new Move(first, shoppingListOfVm1, tp1).take();
//        // VM scales down from a InstanceSizeInflexible RI to a cheaper TP
//        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), 0, 0);
//        // Coupon usage of the VM is not updated. Even though the VM scaled to a computeTier, it looks like it has
//        // some coverage
//        assertEquals(shoppingListOfVm1.getQuantity(1), 16, 0);
//    }

    // CSG's
    @Test
    public void testCouponUpdationOnMoves_8() {
        // CBTP has a capacity of 16 coupons off which 8 is overhead by VM1
        cbtp1.getCommoditySold(COUPON).setCapacity(16).setQuantity(16);
        cbtp2.getCommoditySold(COUPON).setCapacity(16);
        shoppingListOfVm1.move(cbtp1);
        shoppingListOfVm2.move(cbtp1);
        // VM scales on the CBTP alone without any co-customers
        first.getCommodityBought(shoppingListOfVm1, COUPON).setQuantity(8);
        first.getCommodityBought(shoppingListOfVm1, CPU).setQuantity(10);
        first.getCommodityBought(shoppingListOfVm2, COUPON).setQuantity(8);
        first.getCommodityBought(shoppingListOfVm2, CPU).setQuantity(60);
        Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0), 8,8);
        vm1.getSettings().setContext(context);
        Move move1 = new Move(first, shoppingListOfVm2, cbtp2).take();
        // VM1 now requests 8 coupons from CBTP1
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), 8, 0);
        // VM2 now requests 16 coupons from CBTP2
        assertEquals(cbtp2.getCommoditySold(COUPON).getQuantity(), 16, 0);
        // All 16 coupons will be allocated
        assertEquals(shoppingListOfVm1.getQuantity(1), 8, 0);
        assertEquals(shoppingListOfVm2.getQuantity(1), 16, 0);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CouponUpdationWithOverhead({0}, {1}, {2}, {3})")
    public final void testCouponUpdationOnMoveFromTpToCBTPwithOverhead(double cpuUsed, double couponSoldCap,
                                                                       double couponSoldUsed, double couponAllocated) {
        cbtp1.getCommoditySold(COUPON).setCapacity(couponSoldCap).setQuantity(couponSoldUsed);
        shoppingListOfVm1.move(tp1);
        first.getCommodityBought(shoppingListOfVm1, CPU).setQuantity(cpuUsed);
        Move move1 = new Move(first, shoppingListOfVm1, cbtp1).take();
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), couponSoldUsed + couponAllocated, 0);
        assertEquals(shoppingListOfVm1.getQuantity(1), couponAllocated, 0);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCouponUpdationOnMoveFromTpToCBTPwithOverhead() {
        return new Object[][] {
                // cpuUsed, CouponSoldCap, CouponSoldUsed, couponAllocated
                {10, 40, 36, 4},
                {10, 40, 24, 8},
                {10, 40, 24, 8},
                {60, 40, 32, 8},
                {60, 40, 24, 16},
                {60, 40, 16, 16}
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CouponUpdationWithCoCustomer({0}, {1}, {2}, {3}, {4}, {5}, {7})")
    public final void testCouponUpdationOnMoveFromTpToCBTPwithCoCustomer(double cpuUsed1, double cpuUsed2,
                                                                         long couponReq1, long couponAlloc1,
                                                                         double couponSoldCap, double couponSoldUsed,
                                                                         double couponAlloc2) {

        cbtp1.getCommoditySold(COUPON).setCapacity(couponSoldCap).setQuantity(couponSoldUsed);
        if (cpuUsed1 != 0) {
            first.getCommodityBought(shoppingListOfVm1, CPU).setQuantity(cpuUsed1);
            first.getCommodityBought(shoppingListOfVm1, COUPON).setQuantity(couponAlloc1);
            Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0), couponReq1, couponAlloc1);
            vm1.getSettings().setContext(context);
            shoppingListOfVm1.move(tp1);
        }
        first.getCommodityBought(shoppingListOfVm2, CPU).setQuantity(cpuUsed2);
        Move move1 = new Move(first, shoppingListOfVm2, cbtp1).take();

        // couponSoldUsed already captures usage of VM1 and overhead
        // we just add the usage of VM2
        assertEquals(cbtp1.getCommoditySold(COUPON).getQuantity(), couponSoldUsed + couponAlloc2, 0);
        assertEquals(shoppingListOfVm1.getQuantity(1), couponAlloc1, 0);
        assertEquals(shoppingListOfVm2.getQuantity(1), couponAlloc2, 0);

    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestCouponUpdationOnMoveFromTpToCBTPwithCoCustomer() {
        return new Object[][] {
                // cpuBought1, cpuBought2, CouponReqVm1, CouponAllocVm1, CouponSoldCap, CouponSoldUsed, couponAllocated
                {10, 10, 8, 8, 40, 36, 4},
                {10, 10, 8, 8, 40, 24, 8},
                {60, 10, 16, 16, 40, 32, 8},
                {60, 10, 16, 16, 40, 24, 8},
                {60, 10, 16, 16, 40, 16, 8},
                {10, 60, 8, 8, 40, 36, 4},
                {10, 60, 8, 8, 40, 24, 16},
                {10, 60, 8, 8, 40, 24, 16},
                {60, 60, 16, 16, 40, 32, 8},
                {60, 60, 16, 16, 40, 24, 16}
        };
    }
}