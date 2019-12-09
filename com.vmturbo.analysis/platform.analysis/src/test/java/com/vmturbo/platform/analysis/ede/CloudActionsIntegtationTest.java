package com.vmturbo.platform.analysis.ede;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.lang.reflect.Field;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.CostDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CoverageEntry;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.FunctionalOperator;
import com.vmturbo.platform.analysis.utilities.FunctionalOperatorUtil;
import com.vmturbo.platform.analysis.utilities.PlacementResults;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;

import com.vmturbo.platform.analysis.topology.Topology;
import org.junit.runner.RunWith;

import static com.vmturbo.platform.analysis.testUtilities.TestUtils.PM_TYPE;
import static com.vmturbo.platform.analysis.testUtilities.TestUtils.VM_TYPE;
import static org.junit.Assert.*;

@RunWith(JUnitParamsRunner.class)
public class CloudActionsIntegtationTest {

    private static final CommoditySpecification CPU = new CommoditySpecification(0).setDebugInfoNeverUseInCode("CPU");
    private static final CommoditySpecification COUPON = new CommoditySpecification(1).setDebugInfoNeverUseInCode("COUPON");
    private static final CommoditySpecification FAMILY = new CommoditySpecification(2).setDebugInfoNeverUseInCode("FAMILY");
    private static final CommoditySpecification LICENSE = new CommoditySpecification(5, 5, 0, 0).setDebugInfoNeverUseInCode("LICENSE");
    private static final Basket SOLDbyTP = new Basket(CPU, COUPON, FAMILY, LICENSE);
    private static final Basket SOLDbyCBTP = new Basket(CPU, COUPON, LICENSE);
    private static final Basket BOUGHTbyVM = new Basket(CPU, COUPON, LICENSE);
    static final Logger logger = LogManager.getLogger(CloudActionsIntegtationTest.class);

    private static final long BA = 1, REGION = 2, ZONE = 3;
    private static final double VERY_LOW_PRICE = 2, LOW_PRICE = 5, HIGH_PRICE = 10;

    private @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOids = HashBiMap.create();

    // sets up 2 VMs.
    private Trader[] setupConsumers(Economy economy) {
        Trader[] traders = new Trader[4];
        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket(), BOUGHTbyVM);
        traders[0] = vm1;
        Trader vm2 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket(), BOUGHTbyVM);
        traders[1] = vm2;
        Trader vm3 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket(), BOUGHTbyVM);
        traders[2] = vm3;

        Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0));
        vm1.setDebugInfoNeverUseInCode("VirtualMachine|1");
        vm2.setDebugInfoNeverUseInCode("VirtualMachine|2");
        vm3.setDebugInfoNeverUseInCode("VirtualMachine|3");

        traderOids.clear();
        traderOids.put(vm1, 1L);
        traderOids.put(vm2, 2L);
        traderOids.put(vm3, 3L);

        vm1.getSettings().setQuoteFactor(1).setMoveCostFactor(0).setContext(context);
        vm2.getSettings().setQuoteFactor(1).setMoveCostFactor(0).setContext(context);
        vm3.getSettings().setQuoteFactor(1).setMoveCostFactor(0).setContext(context);

        // setup CPU usage
        getSl(economy, vm1).setQuantity(0, 40).setMovable(true);
        getSl(economy, vm2).setQuantity(0, 60).setMovable(true);

        economy.getCommodityBought(getSl(economy, vm1), CPU).setQuantity(40);
        economy.getCommodityBought(getSl(economy, vm2), CPU).setQuantity(10);
        return traders;
    }

    // sets up 4 providers. 2 TPs followed by 2 CBTPs
    private Trader[] setupProviders(Economy economy, Topology topology, long startIndex) {
        Trader[] traders = new Trader[4];
        Trader tp1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, SOLDbyTP, new Basket());
        traders[0] = tp1;
        Trader tp2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, SOLDbyTP, new Basket());
        traders[1] = tp2;
        Trader cbtp1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, SOLDbyCBTP, new Basket(FAMILY));
        traders[2] = cbtp1;
        Trader cbtp2 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, SOLDbyCBTP, new Basket(FAMILY));
        traders[3] = cbtp2;

        tp1.setDebugInfoNeverUseInCode("OnDemandMarketTier|1");
        tp2.setDebugInfoNeverUseInCode("OnDemandMarketTier|2");
        cbtp1.setDebugInfoNeverUseInCode("DiscountedMarketTier|1");
        cbtp2.setDebugInfoNeverUseInCode("DiscountedMarketTier|2");

        traderOids.put(tp1, startIndex + 4L);
        traderOids.put(tp2, startIndex + 5L);
        traderOids.put(cbtp1, startIndex + 6L);
        traderOids.put(cbtp2, startIndex + 7L);

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
                                .setPrice(VERY_LOW_PRICE * 0.0001)
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

        economy.getSettings().setEstimatesEnabled(false);

        economy.populateMarketsWithSellers();

        economy.setTopology(topology);
        try {
            Field traderOidField = Topology.class.getDeclaredField("traderOids_");
            traderOidField.setAccessible(true);
            traderOidField.set(topology, traderOids);
            Field unmodifiableTraderOidField = Topology.class
                    .getDeclaredField("unmodifiableTraderOids_");
            unmodifiableTraderOidField.setAccessible(true);
            unmodifiableTraderOidField.set(topology, traderOids);
        } catch (Exception e) {
            logger.error("Error setting up topology.");
        }

        return traders;
    }

    /**
     * Return a list of test Traders.  All buyers will be added to a single scaling group
     * @param economy economy to add Traders to
     * @param numBuyers number of buyers to create
     * @return list of Traders.  There are numBuyers Traders.
     */
    private Trader[] setupConsumersInCSG(Economy economy, int numBuyers, String scalingGroupId,
                                        int startIndex, double cpuQnty) {
        Trader[] traders = new Trader[numBuyers];
        int traderIndex = 0;
        for (int i = 1; i <= numBuyers; i++) {
            // Create two Traders in a single scaling group.
            Trader trader = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
            Context context = new Context(REGION, ZONE, new Context.BalanceAccount(0, 10000, BA, 0));
            trader.setDebugInfoNeverUseInCode("VirtualMachine|" + (startIndex + i));
            trader.setScalingGroupId(scalingGroupId);
            trader.getSettings().setQuoteFactor(1).setMoveCostFactor(0);
            // populate CPU bought for the VM that is being created
            ShoppingList shoppingList = economy.addBasketBought(trader, BOUGHTbyVM)
                    .setQuantity(0, cpuQnty).setMovable(true);
            economy.getCommodityBought(shoppingList, CPU).setQuantity(cpuQnty);
            // First buyer is the group leader
            shoppingList.setGroupFactor(i == 1 ? numBuyers : 0);
            economy.registerShoppingListWithScalingGroup(scalingGroupId, shoppingList);
            trader.getSettings().setContext(context);
            traders[traderIndex++] = trader;
            traderOids.put(trader, (long)(i + startIndex));
        }

        return traders;
    }

    private ShoppingList getSl(Economy economy, Trader trader) {
        // Return the first (and only) ShoppingList for buyer
        return economy.getMarketsAsBuyer(trader).keySet().iterator().next();
    }

    /**
     *
     * This test verifies that the VM uses the right number of coupons for the template size it picks
     */
    @Test
    public void testCouponUpdationOnMoves_1() {
        // CBTP has a capacity of 40 coupons
        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumers(e);
        Trader[] sellers = setupProviders(e, t, 0);
        sellers[2].getCommoditySold(COUPON).setCapacity(40);
        ShoppingList slVM1 = getSl(e, vms[0]);
        ShoppingList slVM2 = getSl(e, vms[1]);
        slVM1.move(sellers[0]);
        slVM2.move(sellers[0]);
        Placement.generateShopAlonePlacementDecisions(e, slVM1);
        Placement.generateShopAlonePlacementDecisions(e, slVM2);
        // moving each VM to CBTP consuming 8 coupons each
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 16, 0);
        assertEquals(slVM1.getQuantity(1), 8, 0);
        assertEquals(slVM2.getQuantity(1), 8, 0);
    }

    @Test
    public void testCouponUpdationOnMoves_2() {
        // CBTP has a capacity of 30 coupons
        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumers(e);
        Trader[] sellers = setupProviders(e, t, 0);
        sellers[2].getCommoditySold(COUPON).setCapacity(30).setQuantity(10);
        ShoppingList slVM1 = getSl(e, vms[0]);
        ShoppingList slVM2 = getSl(e, vms[1]);
        slVM1.move(sellers[0]);
        slVM2.move(sellers[0]);
        Placement.generateShopAlonePlacementDecisions(e, slVM1);
        Placement.generateShopAlonePlacementDecisions(e, slVM2);
        // moving each VM to CBTP consuming 8 coupons each
        // there is an overhead of 10
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 26, 0);
    }

    // when there is a partially covered VM on a CBTP, if there is some other VM that shops before it,
    // it will try to move but not get any coupons as the partially covered VM will take up all coupons
    @Test
    public void testCouponUpdationOnMoves_3() {
        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumers(e);
        Trader[] sellers = setupProviders(e, t, 0);
        // CBTP has a capacity of 30 coupons
        sellers[2].getCommoditySold(COUPON).setCapacity(16).setQuantity(8);
        sellers[3].getCommoditySold(COUPON).setCapacity(16);
        ShoppingList slVM1 = getSl(e, vms[0]);
        ShoppingList slVM2 = getSl(e, vms[1]);
        slVM1.move(sellers[2]);
        // Partially covered VM is on the CBTP even though there are coupons
        e.getCommodityBought(slVM1, COUPON).setQuantity(8);
        e.getCommodityBought(slVM1, CPU).setQuantity(60);
        e.getCommodityBought(slVM2, CPU).setQuantity(60);
        Context context = makeContext(t.getTraderOid(sellers[2]), 16, 8);
        vms[0].getSettings().setContext(context);
        Move move2 = new Move(e, slVM2, sellers[2]).take();
        // new VM requests 16 coupons but gets partial 8 coupons from CBTP1
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 16, 0);
        // This following is a case that was failing before.
        // This has been fixed now that we have changed the way couponUpdatingFunction works.
        // coupons will be used by the VM that is moving in
        assertEquals(slVM2.getQuantity(1), 8, 0);

        // VM2's context got updated after move
        assertEquals(vms[1].getSettings().getContext().getTotalRequestedCoupons(
                t.getTraderOid(sellers[2])).get(), 16, 0);
        assertEquals(vms[1].getSettings().getContext().getTotalAllocatedCoupons(
            t.getTraderOid(sellers[2])).get(), 8, 0);
        // now say we trigger an actual placement instead of forcing the move
        Placement.generateShopAlonePlacementDecisions(e, slVM2);
        // new VM requests 16 coupons and gets all from CBTP2 and returns 8 back to CBTP1
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 8, 0);
        assertEquals(sellers[3].getCommoditySold(COUPON).getQuantity(), 16, 0);
        assertEquals(slVM1.getQuantity(1), 8, 0);
        assertEquals(slVM2.getQuantity(1), 16, 0);
        Placement.generateShopAlonePlacementDecisions(e, slVM1);
        // VM1 uses up the relinquished coupons
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 16, 0);
        assertEquals(slVM1.getQuantity(1), 16, 0);
    }

    private Context makeContext(final long providerId,
                                final double totalAllocatedCoupons,
                                final double totalRequestedCoupons) {
        return new Context(providerId, REGION, ZONE,
            new Context.BalanceAccount(0d, 10000d, BA, 0L),
            totalAllocatedCoupons, totalRequestedCoupons);
    }

    // scale up on CBTP
    @Test
    public void testCouponUpdationOnMoves_4() {
        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumers(e);
        Trader[] sellers = setupProviders(e, t, 0);
        // CBTP has a capacity of 30 coupons
        sellers[2].getCommoditySold(COUPON).setCapacity(16).setQuantity(8);
        ShoppingList slVM1 = getSl(e, vms[0]);
        slVM1.move(sellers[2]);
        // VM scales on the CBTP alone without any co-customers
        e.getCommodityBought(slVM1, COUPON).setQuantity(8);
        e.getCommodityBought(slVM1, CPU).setQuantity(60);
        Context context = makeContext(t.getTraderOid(sellers[2]), 8, 8);
        vms[0].getSettings().setContext(context);
        EconomyDTOs.Context quoteContext = EconomyDTOs.Context.newBuilder()
            .addFamilyBasedCoverage(CoverageEntry.newBuilder()
                .setTotalRequestedCoupons(16d)
                .setTotalAllocatedCoupons(8d))
            .build();
        // the following will effectively do
        // new Move(e, slVM1, sellers[2], sellers[2], Optional.of(quoteContext)).take();
        Placement.generateShopAlonePlacementDecisions(e, slVM1);
        // VM now requests 16 coupons
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 16, 0);
        // All 16 coupons will be allocated
        assertEquals(slVM1.getQuantity(1), 16, 0);
    }

    // scale down on CBTP and different VM using the coupons
    @Test
    public void testCouponUpdationOnMoves_5() {
        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumers(e);
        Trader[] sellers = setupProviders(e, t, 0);
        // CBTP has a capacity of 30 coupons
        sellers[2].getCommoditySold(COUPON).setCapacity(16).setQuantity(16);
        ShoppingList slVM1 = getSl(e, vms[0]);
        ShoppingList slVM2 = getSl(e, vms[1]);
        slVM1.move(sellers[2]);
        // VM scales on the CBTP alone without any co-customers
        e.getCommodityBought(slVM1, COUPON).setQuantity(16);
        e.getCommodityBought(slVM1, CPU).setQuantity(10);
        Context context = makeContext(t.getTraderOid(sellers[2]), 16L, 16L);
        vms[0].getSettings().setContext(context);
        EconomyDTOs.Context quoteContext = EconomyDTOs.Context.newBuilder()
            .addFamilyBasedCoverage(CoverageEntry.newBuilder()
                .setTotalRequestedCoupons(8d)
                .setTotalAllocatedCoupons(8d))
            .build();
        Placement.generateShopAlonePlacementDecisions(e, slVM1);
        // VM now requests 8 coupons since it is scaling down
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 8, 0);
        // 8 coupons will be allocated
        assertEquals(slVM1.getQuantity(1), 8, 0);
        slVM2.move(sellers[0]);
        Placement.generateShopAlonePlacementDecisions(e, slVM2);
        // VM's now requests 8 coupons each
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 16, 0);
        // 8 coupons will be allocated
        assertEquals(slVM1.getQuantity(1), 8, 0);
        assertEquals(slVM2.getQuantity(1), 8, 0);
    }

    // moving from 1 cbtp to another
    @Test
    public void testCouponUpdationOnMoves_6() {
        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumers(e);
        Trader[] sellers = setupProviders(e, t, 0);
        // CBTP has a capacity of 16 coupons off which 8 is overhead by VM1
        sellers[2].getCommoditySold(COUPON).setCapacity(16).setQuantity(16);
        sellers[3].getCommoditySold(COUPON).setCapacity(16);
        ShoppingList slVM1 = getSl(e, vms[0]);
        ShoppingList slVM2 = getSl(e, vms[1]);
        slVM1.move(sellers[2]);
        slVM2.move(sellers[2]);
        // VM scales on the CBTP alone without any co-customers
        e.getCommodityBought(slVM1, COUPON).setQuantity(8);
        e.getCommodityBought(slVM1, CPU).setQuantity(10);
        e.getCommodityBought(slVM2, COUPON).setQuantity(8);
        e.getCommodityBought(slVM2, CPU).setQuantity(60);
        Context context = makeContext(t.getTraderOid(sellers[2]), 8, 8);
        vms[0].getSettings().setContext(context);
        Placement.generateShopAlonePlacementDecisions(e, slVM2);
        // VM1 now requests 8 coupons from CBTP1
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 8, 0);
        // VM2 now requests 16 coupons from CBTP2
        assertEquals(sellers[3].getCommoditySold(COUPON).getQuantity(), 16, 0);
        // All 16 coupons will be allocated
        assertEquals(slVM1.getQuantity(1), 8, 0);
        assertEquals(slVM2.getQuantity(1), 16, 0);
    }

    // CSG's
    // Group leader part of a CSG containing 2 VMs moving to a CBTP
    @Test
    public void testCouponUpdationOnMoves_7() {
        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumersInCSG(e, 2, "id1", 0, 8);//new double[] {8, 16});
        Trader[] sellers = setupProviders(e, t, 0);
        sellers[2].getCommoditySold(COUPON).setCapacity(30);
        ShoppingList slVM1 = getSl(e, vms[0]);
        ShoppingList slVM2 = getSl(e, vms[1]);
        slVM1.move(sellers[0]);
        // move group leader to CBTP1
        PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, slVM1);
        // Move move1 = new Move(e, slVM1, sellers[2]).take();
        assertEquals(results.getActions().get(0).getSubsequentActions().size(), 1);
        // moving each VM to CBTP consuming 8 coupons each
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 16, 0);
        // assert that both VMs pick the same CBTP getting full coverage
        assertEquals(slVM1.getQuantity(1), 8, 0);
        assertEquals(slVM2.getQuantity(1), 8, 0);
    }

    @Test
    public void testCouponUpdationOnMoves_8() {
        Economy e = new Economy();
        Topology t = new Topology();

    }

    @Test
    public void testCouponUpdationOnMoves_9() {
        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumersInCSG(e, 3, "id1", 0, 8);//new double[] {8, 16});
        Trader[] sellers = setupProviders(e, t, 0);
        sellers[2].getCommoditySold(COUPON).setCapacity(12);
        sellers[3].getCommoditySold(COUPON).setCapacity(4).setQuantity(4);
        ShoppingList slVM1 = getSl(e, vms[0]);
        ShoppingList slVM2 = getSl(e, vms[1]);
        ShoppingList slVM3 = getSl(e, vms[2]);
        slVM1.move(sellers[0]);
        slVM2.move(sellers[1]);
        // VM3 on CBTP2 with full coverage
        slVM3.setQuantity(1, 4);
        slVM3.move(sellers[3]);
        // move group leader to CBTP1 and hence move all peers to CBTP1 or TP1
        PlacementResults results = Placement.generateShopAlonePlacementDecisions(e, slVM1);
        assertEquals(results.getActions().get(0).getSubsequentActions().size(), 2);
        // moving each VM to CBTP consuming 8 coupons each
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), 12, 0);
        assertEquals(slVM1.getQuantity(1), 8, 0);
        // assert that first follower VM picks the CBTP and uses up the remaining 4 coupons
        assertEquals(((Move)results.getActions().get(0).getSubsequentActions().get(1)).getDestination(), sellers[2]);
        assertEquals(slVM2.getQuantity(1), 4, 0);
        // assert that second follower VM picks TP once the CBTP is out of coupons
        assertEquals(((Move)results.getActions().get(0).getSubsequentActions().get(0)).getDestination(), sellers[0]);
        // VM3 moves to TP1 with 0 coverage. VM3 relinquishes coupons back to CBTP2.
        assertEquals(slVM3.getQuantity(1), 0, 0);
        // assert coupons have been relinquished
        assertEquals(sellers[3].getCommoditySold(COUPON).getQuantity(), 0, 0);
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: CouponUpdationWithOverhead({0}, {1}, {2}, {3})")
    public final void testCouponUpdationOnMoveFromTpToCBTPwithOverhead(double cpuUsed, double couponSoldCap,
                                                                       double couponSoldUsed, double couponAllocated) {
        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumers(e);
        Trader[] sellers = setupProviders(e, t, 0);
        sellers[2].getCommoditySold(COUPON).setCapacity(couponSoldCap).setQuantity(couponSoldUsed);
        // invalidating CBTP2 by making it fully used
        sellers[3].getCommoditySold(COUPON).setCapacity(couponSoldCap).setQuantity(couponSoldCap);
        ShoppingList slVM1 = getSl(e, vms[0]);
        slVM1.move(sellers[0]);
        e.getCommodityBought(slVM1, CPU).setQuantity(cpuUsed);
        Placement.generateShopAlonePlacementDecisions(e, slVM1);
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), couponSoldUsed + couponAllocated, 0);
        assertEquals(slVM1.getQuantity(1), couponAllocated, 0);
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

        Economy e = new Economy();
        Topology t = new Topology();
        Trader[] vms = setupConsumers(e);
        Trader[] sellers = setupProviders(e, t, 0);
        sellers[2].getCommoditySold(COUPON).setCapacity(couponSoldCap).setQuantity(couponSoldUsed);
        // invalidating CBTP2 by making it fully used
        sellers[3].getCommoditySold(COUPON).setCapacity(couponSoldCap).setQuantity(couponSoldCap);
        ShoppingList slVM1 = getSl(e, vms[0]);
        ShoppingList slVM2 = getSl(e, vms[1]);
        if (cpuUsed1 != 0) {
            e.getCommodityBought(slVM1, CPU).setQuantity(cpuUsed1);
            e.getCommodityBought(slVM1, COUPON).setQuantity(couponAlloc1);
            Context context = makeContext(t.getTraderOid(sellers[2]), couponAlloc1, couponReq1);
            vms[0].getSettings().setContext(context);
            slVM1.move(sellers[0]);
        }
        e.getCommodityBought(slVM2, CPU).setQuantity(cpuUsed2);
        Placement.generateShopAlonePlacementDecisions(e, slVM2);

        // couponSoldUsed already captures usage of VM1 and overhead
        // we just add the usage of VM2
        assertEquals(sellers[2].getCommoditySold(COUPON).getQuantity(), couponSoldUsed + couponAlloc2, 0);
        assertEquals(slVM1.getQuantity(1), couponAlloc1, 0);
        assertEquals(slVM2.getQuantity(1), couponAlloc2, 0);

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